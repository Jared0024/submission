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
pub struct Event<T, S, P = u32> {
    pub person: P,
    pub activity: Activity<T, S>,
}

#[derive(Clone, Copy, Default)]
pub struct Activity<T, S> {
    pub time: T,
    pub sale: S,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
struct ShopperKey(u64);

impl ShopperKey {
    const INVALID: Self = Self(u64::MAX);

    #[inline]
    fn new(raw: &[u8], spill: &mut Spill, long_shoppers: &mut HashTable<EventId>) -> Self {
        if let Some(key) = InlineId::new(raw) {
            return Self(key.0);
        }
        // Repeated long aliases share one owned copy until the batch's claims
        // finish deduplication. The table never borrows from a growing buffer.
        let hash = FxBuildHasher.hash_one(raw);
        let id = match long_shoppers.entry(
            hash,
            |id| spill.get(*id) == raw,
            |id| FxBuildHasher.hash_one(spill.get(*id)),
        ) {
            Entry::Occupied(slot) => *slot.get(),
            Entry::Vacant(slot) => {
                let id = spill.push(raw);
                slot.insert(id);
                id
            }
        };
        Self(id.0)
    }

    fn resolve(self, spill: &Spill, dimensions: &Dimensions<'_>, names: &mut Names) -> Option<u32> {
        if self == Self::INVALID {
            return None;
        }
        let (short, raw) = if self.0 < EventId::SPILL {
            (Some(InlineId(self.0)), &[][..])
        } else {
            (None, spill.get(EventId(self.0)))
        };
        Some(
            dimensions
                .people
                .aliases
                .get(raw, short)
                .unwrap_or_else(|| {
                    unlinked_index(
                        dimensions.people.names.len() as u32,
                        names.intern(raw, short),
                    )
                }),
        )
    }
}

struct Claim<T, S> {
    id: EventId,
    ingested: Ingest,
    event: Event<T, S, ShopperKey>,
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

    #[inline]
    fn partition(raw: &[u8], spill: &mut Spill, shards: usize) -> (Self, usize) {
        // A nine-byte inline ID consists of a one-byte prefix and eight suffix
        // bytes. For equal suffixes, XOR routes each distinct prefix to a
        // different shard when the mask keeps at least seven bits. Shorter IDs
        // use prefix zero. Comparisons inside the shard need only the suffix.
        let compact = if shards >= 128 {
            Self::suffix(raw)
        } else {
            InlineId::new(raw).map(|key| (Self(key.0), 0))
        };
        let (id, prefix) = compact.unwrap_or_else(|| (spill.push(raw), 0));
        let shard = ((id.hash(spill) >> 32) as usize ^ prefix) & (shards - 1);
        (id, shard)
    }

    #[inline]
    fn suffix(raw: &[u8]) -> Option<(Self, usize)> {
        const HIGH: u64 = 0x8080_8080_8080_8080;
        let (prefix, bytes) = match raw {
            [first, tail @ ..] if tail.len() == 8 && (1..128).contains(first) => (*first, tail),
            short if short.len() <= 8 => (0, short),
            _ => return None,
        };
        if let Ok(full) = <[u8; 8]>::try_from(bytes) {
            let word = u64::from_le_bytes(full);
            let zeros = word.wrapping_sub(0x0101_0101_0101_0101) & !word;
            return ((word | zeros) & HIGH == 0).then_some((Self(word), usize::from(prefix)));
        }
        let mut padded = [0u8; 8];
        for (dst, &ch) in padded.iter_mut().zip(bytes) {
            if !(1..128).contains(&ch) {
                return None;
            }
            *dst = ch;
        }
        Some((Self(u64::from_le_bytes(padded)), 0))
    }

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

// Keep uncommon IDs in contiguous batch-owned storage so their input
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

// Linked shoppers occupy a fixed prefix. Each deduplication group numbers
// its unlinked winners locally until routing translates them to global indices.
fn unlinked_index(linked: u32, local: u32) -> u32 {
    let index = linked.checked_add(local).expect("shopper index overflow");
    assert_ne!(index, INVALID, "shopper index overflow");
    index
}

struct Batch<T, S> {
    claims: Vec<Vec<Claim<T, S>>>,
    spill: Spill,
    extended: Vec<i64>,
}

struct Source {
    // Event IDs, long shopper aliases and extended ingests live until all
    // winners referencing this parse batch have been resolved.
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
    let per_batch = batch_size(chunks.len(), workers);
    let batches: Vec<_> = chunks
        .into_par_iter()
        .chunks(per_batch)
        .map(|group| {
            let mut claims = buckets(event_shards);
            let mut spill = Spill::default();
            let mut long_shoppers = HashTable::new();
            let mut extended = Vec::new();
            let mut dates = Timestamps::default();
            for chunk in group {
                chunk.visit(|row| {
                    let Some((raw_id, ingested, event)) = parse_event(
                        input,
                        dimensions,
                        &mut spill,
                        &mut long_shoppers,
                        &mut dates,
                        row,
                        base,
                    ) else {
                        return;
                    };
                    let (id, shard) = EventId::partition(raw_id, &mut spill, event_shards);
                    claims[shard].push(Claim {
                        id,
                        ingested: Ingest::encode(ingested, base, &mut extended),
                        event,
                    });
                })?;
            }
            Ok(Batch {
                claims,
                spill,
                extended,
            })
        })
        .collect::<Result<_>>()?;

    let mut sources = Vec::with_capacity(batches.len());
    let mut shards = buckets(event_shards);
    for (source, batch) in batches.into_iter().enumerate() {
        sources.push(Source {
            spill: batch.spill,
            extended: batch.extended,
        });
        for (shard, rows) in shards.iter_mut().zip(batch.claims) {
            if !rows.is_empty() {
                shard.push(ClaimPart { source, rows });
            }
        }
    }
    // Share a local shopper dictionary across several disjoint event-ID shards.
    // This bounds the number of dictionaries to merge after resolving winners.
    let winners: Vec<_> = shards
        .into_par_iter()
        .chunks(batch_size(event_shards, workers))
        .map(|group| {
            let mut names = Names::default();
            let mut events = Vec::new();
            for parts in group {
                deduplicate(parts, &sources, dimensions, base, &mut names, &mut events);
            }
            (names, events)
        })
        .collect();
    drop(sources);
    let mut people = Names::default();
    let mut translated = Vec::with_capacity(winners.len());
    for (names, events) in winners {
        let mut translations = vec![0; names.len()];
        for (key, local) in names.short {
            translations[local as usize] = unlinked_index(linked, people.intern_short(key));
        }
        for (id, local) in names.other {
            translations[local as usize] = unlinked_index(linked, people.intern_other(&id));
        }
        translated.push((translations, events));
    }
    // Only winning, valid events contribute unlinked shopper numbers. The
    // completed dictionary is now available to routing and click parsing.
    let (routed, clicks) = rayon::join(
        move || {
            translated
                .into_par_iter()
                .map(|(translations, events)| {
                    let mut routed = buckets(shopper_shards);
                    for mut event in events {
                        if event.person >= linked {
                            event.person = translations[(event.person - linked) as usize];
                        }
                        routed[event.person as usize & (shopper_shards - 1)].push(event);
                    }
                    routed
                })
                .collect::<Vec<_>>()
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
    dimensions: &Dimensions<'_>,
    base: i64,
    names: &mut Names,
    events: &mut Vec<Event<T, S>>,
) {
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
                        .person = ShopperKey::INVALID;
                }
            }
        }
    }
    drop(winners);
    for part in parts {
        let spill = &sources[part.source].spill;
        for claim in part.rows {
            if let Some(person) = claim.event.person.resolve(spill, dimensions, names) {
                events.push(Event {
                    person,
                    activity: claim.event.activity,
                });
            }
        }
    }
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
    spill: &mut Spill,
    long_shoppers: &mut HashTable<EventId>,
    dates: &mut Timestamps,
    row: [Id<'a>; 7],
    base: i64,
) -> Option<(Id<'a>, i64, Event<T, S, ShopperKey>)> {
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
        let person = ShopperKey::new(user, spill, long_shoppers);
        Some(Event {
            person,
            activity: Activity {
                time: T::encode(time, base),
                sale: S::pack(category, cents),
            },
        })
    })()
    .unwrap_or(Event {
        person: ShopperKey::INVALID,
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
    fn repeated_long_shoppers_share_their_spill_across_interleaved_events() {
        let mut spill = Spill::default();
        let mut seen = HashTable::new();
        let raw = b"long-shopper-\x00-name";
        let first = ShopperKey::new(raw, &mut spill, &mut seen);
        spill.push(b"an unrelated long event id");
        for i in 0..128 {
            ShopperKey::new(
                format!("another-long-user-{i}").as_bytes(),
                &mut spill,
                &mut seen,
            );
        }
        assert!(first == ShopperKey::new(raw, &mut spill, &mut seen));
        assert_eq!(seen.len(), 129);
        assert_eq!(spill.starts.len(), 130);
        assert_eq!(spill.get(EventId(first.0)), raw);
    }

    #[test]
    fn event_suffix_and_shard_together_preserve_identity() {
        use std::collections::HashSet;
        let mut ids = vec![b"12345678".to_vec()];
        for prefix in 1..128 {
            let mut id = vec![prefix];
            id.extend_from_slice(b"12345678");
            ids.push(id);
        }
        ids.extend([b"1".to_vec(), b"12".to_vec(), b"1234567".to_vec()]);
        for shards in [128, 256, 4096] {
            let mut seen = HashSet::new();
            for raw in &ids {
                let (id, shard) = EventId::partition(raw, &mut Spill::default(), shards);
                assert!(id.0 < EventId::SPILL);
                assert!(seen.insert((id.0, shard)));
            }
        }
        for raw in [
            b"\x0012345678".as_slice(),
            b"x1234\x000678",
            b"12345678\xff",
            b"0123456789",
        ] {
            let mut spill = Spill::default();
            let (id, _) = EventId::partition(raw, &mut spill, 128);
            assert_eq!(spill.get(id), raw);
        }
    }

    #[test]
    fn compact_layout() {
        assert_eq!(size_of::<Claim<u32, u32>>(), 32);
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
