use std::{borrow::Cow, hash::BuildHasher};

use crate::names::{InlineId, NameMap};
use anyhow::Result;
use hashbrown::{HashTable, hash_table::Entry};
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;

use crate::input::{DAY_MS, Dimensions, Id, Input, Timestamps, digits, visit_rows};

// A normal reporting window, including the click lookback, fits in 32 bits.
// Wider manifests use the same pipeline with absolute 64-bit timestamps.
pub trait Stamp: Copy + Default + Ord + Send + Sync {
    fn encode(time: i64, base: i64) -> Self;
    fn millis(self, base: i64) -> i64;
}

impl Stamp for u32 {
    fn encode(time: i64, base: i64) -> Self {
        u32::try_from(time - base).expect("timestamp inside compact reporting window")
    }
    fn millis(self, base: i64) -> i64 {
        base + i64::from(self)
    }
}

impl Stamp for i64 {
    fn encode(time: i64, _: i64) -> Self {
        time
    }
    fn millis(self, _: i64) -> i64 {
        self
    }
}

// A valid price fits in 24 bits. Choose the category width once per catalog.
pub trait Sale: Copy + Default + Eq + Send + Sync + Into<u64> {
    fn pack(category: u32, cents: u32) -> Self;
    fn cents(self) -> u32 {
        self.into() as u32 & 0xff_ffff
    }
    fn category(self) -> usize {
        (self.into() >> 24) as usize
    }
}
impl Sale for u32 {
    fn pack(category: u32, cents: u32) -> Self {
        debug_assert!(category <= u8::MAX as u32);
        (category << 24) | cents
    }
}
impl Sale for u64 {
    fn pack(category: u32, cents: u32) -> Self {
        (u64::from(category) << 24) | u64::from(cents)
    }
}

const INVALID: u32 = u32::MAX;

#[derive(Clone, Copy, Default)]
pub struct Event<T, S> {
    pub person: u32,
    pub activity: Activity<T, S>,
}

#[derive(Clone, Copy, Default)]
pub struct Activity<T, S> {
    pub time: T,
    pub sale: S,
}

struct Claim<T, S> {
    id: EventId,
    ingested: Ingest,
    event: Event<T, S>,
}

#[derive(Clone, Copy)]
pub struct Click<'a, T> {
    pub id: Id<'a>,
    pub person: u32,
    pub time: T,
    pub campaign: usize,
}

pub struct ShopperPartition<'a, T, S> {
    pub events: Vec<Vec<Event<T, S>>>,
    pub clicks: Vec<Vec<Click<'a, T>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct EventId(u64);

impl EventId {
    const SPILL: u64 = 1 << 63;

    fn hash(self, spill: &Spill) -> u64 {
        if self.0 < Self::SPILL {
            FxBuildHasher.hash_one(self.0)
        } else {
            FxBuildHasher.hash_one(spill.get(self))
        }
    }

    fn matches(self, other: Self, left: &Spill, right: &Spill) -> bool {
        if self.0 < Self::SPILL || other.0 < Self::SPILL {
            return self == other;
        }
        left.get(self) == right.get(other)
    }
}

// Dedup only compares ingest times on duplicate IDs. Typical ingests need
// one word; arbitrary earlier/later values remain exact in batch-local storage.
#[derive(Clone, Copy)]
struct Ingest(u32);

impl Ingest {
    const EXTENDED: u32 = 1 << 31;

    fn encode(time: i64, base: i64, extended: &mut Vec<i64>) -> Self {
        if let Some(delta) = time.checked_sub(base)
            && (0..i64::from(Self::EXTENDED)).contains(&delta)
        {
            return Self(delta as u32);
        }
        let index = u32::try_from(extended.len()).expect("extended ingest index overflow");
        assert!(index < Self::EXTENDED, "extended ingest index overflow");
        extended.push(time);
        Self(Self::EXTENDED | index)
    }

    fn millis(self, base: i64, extended: &[i64]) -> i64 {
        if self.0 < Self::EXTENDED {
            base + i64::from(self.0)
        } else {
            extended[(self.0 ^ Self::EXTENDED) as usize]
        }
    }

    fn earlier(self, other: Self, base: i64, left: &[i64], right: &[i64]) -> bool {
        if (self.0 | other.0) < Self::EXTENDED {
            self.0 < other.0
        } else {
            self.millis(base, left) < other.millis(base, right)
        }
    }
}

// Keep uncommon event IDs in contiguous batch-owned storage so their input
// mapping can be released. The ID stores the index of its starting offset.
#[derive(Default)]
struct Spill {
    bytes: Vec<u8>,
    starts: Vec<usize>,
}

impl Spill {
    fn push(&mut self, bytes: &[u8]) -> EventId {
        let id = EventId(EventId::SPILL | self.starts.len() as u64);
        self.starts.push(self.bytes.len());
        self.bytes.extend_from_slice(bytes);
        id
    }

    fn get(&self, id: EventId) -> &[u8] {
        let index = (id.0 ^ EventId::SPILL) as usize;
        &self.bytes[self.starts[index]
            ..self
                .starts
                .get(index + 1)
                .copied()
                .unwrap_or(self.bytes.len())]
    }
}

type Names = NameMap<Box<[u8]>>;

// Linked shoppers have stable indices shared by every parser. Batch-local
// unlinked indices follow that prefix until dedup routes the winning claim.
fn unlinked_index(linked: u32, local: u32) -> u32 {
    let index = linked.checked_add(local).expect("shopper index overflow");
    assert_ne!(index, INVALID, "shopper index overflow");
    index
}

struct Batch<T, S> {
    claims: Vec<Vec<Claim<T, S>>>,
    names: Names,
    spill: Spill,
    extended: Vec<i64>,
}

struct Source {
    // Both tables are read-only throughout deduplication.
    people: Vec<u32>,
    spill: Spill,
    extended: Vec<i64>,
}

struct ClaimPart<T, S> {
    source: usize,
    rows: Vec<Claim<T, S>>,
}

pub fn partition<'a, T: Stamp, S: Sale>(
    input: &'a Input,
    dimensions: &Dimensions<'a>,
    workers: usize,
    base: i64,
) -> Result<(Vec<ShopperPartition<'a, T, S>>, Vec<Cow<'a, [u8]>>)> {
    let linked = u32::try_from(dimensions.people.names.len()).expect("shopper index overflow");
    let chunks = input.event_chunks()?;
    let bytes: usize = chunks.iter().map(|c| c.range.len()).sum();
    // Roughly 8K claims per event shard on the generator's CSV. The small
    // index and its referenced records are reused by a single worker.
    let event_shards = bytes
        .div_ceil(768 * 1024)
        .clamp(8, 4096)
        .next_power_of_two();
    let shopper_shards = (workers * 4).next_power_of_two().max(8);
    let batches: Vec<_> = chunks
        .par_chunks(batch_size(chunks.len(), workers))
        .map(|group| {
            let mut claims = buckets(event_shards);
            let mut names = Names::default();
            let mut spill = Spill::default();
            let mut extended = Vec::new();
            let mut dates = Timestamps::default();
            for chunk in group {
                chunk.visit(|row| {
                    let Some((raw_id, ingested, event)) =
                        parse_event(input, dimensions, &mut names, &mut dates, row, base)
                    else {
                        return;
                    };
                    let id = InlineId::new(raw_id)
                        .map(|key| EventId(key.0))
                        .unwrap_or_else(|| spill.push(raw_id));
                    // Use middle hash bits here. The table inside the shard uses
                    // the low bits for probing and the highest bits for tags.
                    let shard = (id.hash(&spill) >> 32) as usize & (event_shards - 1);
                    claims[shard].push(Claim {
                        id,
                        ingested: Ingest::encode(ingested, base, &mut extended),
                        event,
                    });
                })?;
            }
            Ok(Batch {
                claims,
                names,
                spill,
                extended,
            })
        })
        .collect::<Result<_>>()?;
    drop(chunks);

    let mut people = Names::default();
    let mut sources = Vec::with_capacity(batches.len());
    let mut shards = buckets(event_shards);
    for (source, batch) in batches.into_iter().enumerate() {
        let mut translations = vec![0; batch.names.len()];
        for (key, local) in batch.names.short {
            translations[local as usize] = unlinked_index(linked, people.intern_short(key));
        }
        for (alias, local) in batch.names.other {
            translations[local as usize] = unlinked_index(linked, people.intern_other(&alias));
        }
        sources.push(Source {
            people: translations,
            spill: batch.spill,
            extended: batch.extended,
        });
        for (shard, rows) in shards.iter_mut().zip(batch.claims) {
            if !rows.is_empty() {
                shard.push(ClaimPart { source, rows });
            }
        }
    }

    // The alias table is complete. Click parsing and event deduplication can
    // use the same pool independently, including when a shard finishes early.
    let (routed, clicks) = rayon::join(
        move || {
            let routed: Vec<_> = shards
                .into_par_iter()
                .map(|parts| deduplicate(parts, &sources, shopper_shards, base, linked))
                .collect();
            drop(sources);
            routed
        },
        || {
            let click_chunks = input.click_chunks();
            click_chunks
                .par_chunks(batch_size(click_chunks.len(), workers))
                .map(|group| {
                    let mut buckets = buckets(shopper_shards);
                    let mut dates = Timestamps::default();
                    for chunk in group {
                        visit_rows(chunk, |row| {
                            if let Some(click) =
                                parse_click(input, dimensions, &people, &mut dates, row, base)
                            {
                                buckets[click.person as usize & (shopper_shards - 1)].push(click);
                            }
                        });
                    }
                    buckets
                })
                .collect::<Vec<_>>()
        },
    );

    let partitions = transpose(routed, shopper_shards)
        .into_iter()
        .zip(transpose(clicks, shopper_shards))
        .map(|(events, clicks)| ShopperPartition { events, clicks })
        .collect();
    let mut names: Vec<_> = dimensions
        .people
        .names
        .iter()
        .copied()
        .map(Cow::Borrowed)
        .collect();
    names.resize(names.len() + people.len(), Cow::Borrowed(&[]));
    for (key, index) in people.short {
        names[linked as usize + index as usize] = Cow::Owned(key.bytes());
    }
    for (name, index) in people.other {
        names[linked as usize + index as usize] = Cow::Owned(name.into_vec());
    }
    Ok((partitions, names))
}

#[derive(Clone, Copy)]
struct Position {
    part: u32,
    row: u32,
}

fn deduplicate<T: Stamp, S: Sale>(
    mut parts: Vec<ClaimPart<T, S>>,
    sources: &[Source],
    shopper_shards: usize,
    base: i64,
    linked: u32,
) -> Vec<Vec<Event<T, S>>> {
    let count = parts.iter().map(|p| p.rows.len()).sum();
    let mut winners = HashTable::<Position>::with_capacity(count);
    for p in 0..parts.len() {
        let spill = &sources[parts[p].source].spill;
        let part = u32::try_from(p).expect("too many claim parts");
        assert!(
            u32::try_from(parts[p].rows.len()).is_ok(),
            "claim part too large"
        );
        for r in 0..parts[p].rows.len() {
            let claim = &parts[p].rows[r];
            let pos = Position {
                part,
                row: r as u32,
            };
            let entry = winners.entry(
                claim.id.hash(spill),
                |pos| {
                    let other = &parts[pos.part as usize];
                    claim.id.matches(
                        other.rows[pos.row as usize].id,
                        spill,
                        &sources[other.source].spill,
                    )
                },
                |pos| {
                    let other = &parts[pos.part as usize];
                    other.rows[pos.row as usize]
                        .id
                        .hash(&sources[other.source].spill)
                },
            );
            match entry {
                Entry::Vacant(slot) => {
                    slot.insert(pos);
                }
                Entry::Occupied(mut slot) => {
                    let old = *slot.get();
                    let previous_part = &parts[old.part as usize];
                    let loser = if claim.ingested.earlier(
                        previous_part.rows[old.row as usize].ingested,
                        base,
                        &sources[parts[p].source].extended,
                        &sources[previous_part.source].extended,
                    ) {
                        *slot.get_mut() = pos;
                        old
                    } else {
                        pos
                    };
                    parts[loser.part as usize].rows[loser.row as usize]
                        .event
                        .person = INVALID;
                }
            }
        }
    }
    drop(winners);
    let mut routed = buckets(shopper_shards);
    for part in parts {
        let people = &sources[part.source].people;
        for claim in part.rows {
            let mut event = claim.event;
            if event.person != INVALID {
                if event.person >= linked {
                    event.person = people[(event.person - linked) as usize];
                }
                routed[event.person as usize & (shopper_shards - 1)].push(event);
            }
        }
    }
    routed
}

fn batch_size(chunks: usize, workers: usize) -> usize {
    chunks.div_ceil(workers * 2).max(1)
}

fn buckets<T>(count: usize) -> Vec<Vec<T>> {
    (0..count).map(|_| Vec::new()).collect()
}

fn transpose<T>(batches: Vec<Vec<Vec<T>>>, count: usize) -> Vec<Vec<Vec<T>>> {
    let mut shards = buckets(count);
    for batch in batches {
        for (shard, rows) in shards.iter_mut().zip(batch) {
            if !rows.is_empty() {
                shard.push(rows);
            }
        }
    }
    shards
}

fn parse_event<'a, T: Stamp, S: Sale>(
    input: &Input,
    dimensions: &Dimensions<'a>,
    names: &mut Names,
    dates: &mut Timestamps,
    row: [Id<'a>; 7],
    base: i64,
) -> Option<(Id<'a>, i64, Event<T, S>)> {
    let [id, user, ts, ingest, kind, product, price] = row;
    if id.is_empty() {
        return None;
    }
    let ingested = dates.parse(ingest)?;
    let event = (|| {
        if user.is_empty() || product.is_empty() {
            return None;
        }
        let time = dates.parse(ts)?;
        if time < input.start || time >= input.end || ingested.saturating_sub(time) > DAY_MS {
            return None;
        }
        let cents = match kind {
            b"view" | b"cart" => 0,
            b"purchase" => {
                let cents = digits(price)?;
                if !(1..=10_000_000).contains(&cents) {
                    return None;
                }
                cents as u32
            }
            _ => return None,
        };
        let category = if cents > 0 {
            dimensions.products.get(product).copied().unwrap_or(0)
        } else {
            0
        };
        let short = InlineId::new(user);
        let person = dimensions
            .people
            .aliases
            .get(user, short)
            .unwrap_or_else(|| {
                unlinked_index(
                    dimensions.people.names.len() as u32,
                    names.intern(user, short),
                )
            });
        Some(Event {
            person,
            activity: Activity {
                time: T::encode(time, base),
                sale: S::pack(category, cents),
            },
        })
    })()
    .unwrap_or(Event {
        person: INVALID,
        ..Event::default()
    });
    Some((id, ingested, event))
}

fn parse_click<'a, T: Stamp>(
    input: &Input,
    dimensions: &Dimensions<'a>,
    people: &Names,
    dates: &mut Timestamps,
    row: [Id<'a>; 4],
    base: i64,
) -> Option<Click<'a, T>> {
    let [id, user, campaign, ts] = row;
    if id.is_empty() || user.is_empty() {
        return None;
    }
    let time = dates.parse(ts)?;
    if time < base || time >= input.end {
        return None;
    }
    let short = InlineId::new(user);
    let person = match dimensions.people.aliases.get(user, short) {
        Some(linked) => linked,
        None => unlinked_index(
            dimensions.people.names.len() as u32,
            people.get(user, short)?,
        ),
    };
    let &campaign = dimensions.campaigns.get(campaign)?;
    Some(Click {
        id,
        person,
        time: T::encode(time, base),
        campaign,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_layout() {
        assert_eq!(size_of::<Claim<u32, u32>>(), 24);
        assert_eq!(size_of::<Event<u32, u32>>(), 12);
        assert_eq!(size_of::<Activity<u32, u32>>(), 8);
        assert_eq!(size_of::<Position>(), 8);
    }

    #[test]
    fn ingests_compare_across_batches_and_encoding_boundaries() {
        let base = 1_700_000_000_000;
        let times = [
            i64::MIN,
            base - 1,
            base,
            base + i64::from(i32::MAX),
            base + i64::from(i32::MAX) + 1,
            i64::MAX,
        ];
        let (mut left, mut right) = (Vec::new(), Vec::new());
        let a = times.map(|t| Ingest::encode(t, base, &mut left));
        right.push(17); // The same timestamp can have a different index in a second batch.
        let b = times.map(|t| Ingest::encode(t, base, &mut right));
        for (i, x) in a.into_iter().enumerate() {
            for (j, y) in b.into_iter().enumerate() {
                assert_eq!(x.earlier(y, base, &left, &right), times[i] < times[j]);
            }
        }
    }
}
