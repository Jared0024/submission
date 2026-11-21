use std::{
    fs::{self, File},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::names::LinkedNames;
use anyhow::{Context, Result, ensure};
use memchr::{memchr, memchr_iter};
use memmap2::{Mmap, MmapOptions};
use rustc_hash::FxHashMap;

pub const DAY_MS: i64 = 86_400_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;
pub const GAP_MS: i64 = 30 * 60 * 1000;
pub type Id<'a> = &'a [u8];

pub struct Input {
    events: Vec<PathBuf>,
    mapping_budget: u64,
    clicks: Vec<Mmap>,
    links: Vec<Mmap>,
    products: Option<Mmap>,
    campaigns: Option<Mmap>,
    pub start: i64,
    pub end: i64,
}

pub struct Dimensions<'a> {
    pub people: LinkedPeople<'a>,
    pub products: FxHashMap<Id<'a>, u32>,
    pub categories: Vec<Id<'a>>,
    pub campaigns: FxHashMap<Id<'a>, usize>,
    pub campaign_names: Vec<(Id<'a>, Id<'a>)>,
}

impl Input {
    pub fn open(path: &Path, mapping_budget: u64) -> Result<Self> {
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(path.join("manifest.json"))?)?;
        let mut dates = Timestamps::default();
        let mut time = |key: &str| {
            manifest[key]
                .as_str()
                .and_then(|s| dates.parse(s.as_bytes()))
                .with_context(|| format!("invalid manifest field {key}"))
        };
        let (start, end) = (time("window_start")?, time("window_end")?);
        ensure!(start <= end, "the reporting window is reversed");
        Ok(Self {
            mapping_budget,
            events: csv_paths(&path.join("events"))?,
            clicks: csv_files(&path.join("clicks"))?,
            links: csv_files(&path.join("links"))?,
            products: map_file(&path.join("products.csv"))?,
            campaigns: map_file(&path.join("campaigns.csv"))?,
            start,
            end,
        })
    }

    pub fn event_chunks(&self) -> Result<Vec<EventChunk<'_>>> {
        let mut chunks = Vec::new();
        let mut budget = self.mapping_budget;
        for path in &self.events {
            if let Some(mapping) = map_file(path)? {
                let mapping = Arc::new(mapping);
                let shared = budget.checked_sub(mapping.len() as u64).map(|remaining| {
                    budget = remaining;
                    Arc::clone(&mapping)
                });
                let mut offset = mapping.len() - body(&mapping).len();
                for bytes in split_chunks(&mapping[offset..]) {
                    let end = offset + bytes.len();
                    chunks.push(EventChunk {
                        path,
                        shared: shared.clone(),
                        range: offset..end,
                    });
                    offset = end;
                }
            }
        }
        Ok(chunks)
    }

    pub fn click_chunks(&self) -> Vec<&[u8]> {
        chunks(&self.clicks)
    }

    pub fn dimensions(&self) -> Dimensions<'_> {
        let people = linked_people(&self.links);
        // Category zero is reserved for products absent from the catalog.
        let mut categories: Vec<Id<'_>> = vec![b"unknown"];
        let mut category_ids = FxHashMap::default();
        let mut products = FxHashMap::default();
        if let Some(file) = &self.products {
            for [id, category] in lines(body(file)).filter_map(fields::<2>) {
                let number = *category_ids.entry(category).or_insert_with(|| {
                    let number = u32::try_from(categories.len()).expect("category index overflow");
                    categories.push(category);
                    number
                });
                products.insert(id, number);
            }
        }

        let mut campaigns = FxHashMap::default();
        let mut campaign_names = Vec::new();
        if let Some(file) = &self.campaigns {
            for [id, channel] in lines(body(file)).filter_map(fields::<2>) {
                campaigns.insert(id, campaign_names.len());
                campaign_names.push((id, channel));
            }
        }
        campaign_names.push((b"organic", b"organic"));
        Dimensions {
            people,
            products,
            categories,
            campaigns,
            campaign_names,
        }
    }
}

fn map_file(path: &Path) -> Result<Option<Mmap>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    map_open(&file).with_context(|| format!("mapping {}", path.display()))
}

fn map_open(file: &File) -> Result<Option<Mmap>> {
    if file.metadata()?.len() == 0 {
        return Ok(None);
    }
    // Inputs are mounted read-only for the entire run.
    Ok(Some(unsafe { Mmap::map(file) }?))
}

// Files within the mapping budget share their planning map across parse tasks.
// Other files are mapped one newline-aligned range at a time.
pub struct EventChunk<'a> {
    path: &'a Path,
    shared: Option<Arc<Mmap>>,
    pub range: Range<usize>,
}

impl EventChunk<'_> {
    pub fn visit<const N: usize>(&self, visit: impl FnMut([&[u8]; N])) -> Result<()> {
        if let Some(mapping) = &self.shared {
            visit_rows(&mapping[self.range.clone()], visit);
            return Ok(());
        }
        let file =
            File::open(self.path).with_context(|| format!("opening {}", self.path.display()))?;
        // The same read-only file supplied the boundaries above.
        let mapping = unsafe {
            MmapOptions::new()
                .offset(self.range.start as u64)
                .len(self.range.len())
                .map(&file)?
        };
        visit_rows(&mapping, visit);
        Ok(())
    }
}

fn csv_paths(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in
        fs::read_dir(directory).with_context(|| format!("reading {}", directory.display()))?
    {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "csv") {
            files.push(path);
        }
    }
    Ok(files)
}

fn csv_files(directory: &Path) -> Result<Vec<Mmap>> {
    csv_paths(directory)?
        .iter()
        .filter_map(|path| map_file(path).transpose())
        .collect()
}

fn body(file: &[u8]) -> &[u8] {
    memchr(b'\n', file).map_or(&[], |end| &file[end + 1..])
}

fn chunks(files: &[Mmap]) -> Vec<&[u8]> {
    files.iter().flat_map(|f| split_chunks(body(f))).collect()
}

fn split_chunks(mut remaining: &[u8]) -> impl Iterator<Item = &[u8]> {
    const SIZE: usize = 4 * 1024 * 1024;
    std::iter::from_fn(move || {
        if remaining.is_empty() {
            return None;
        }
        let end = remaining
            .get(SIZE..)
            .and_then(|tail| memchr(b'\n', tail))
            .map_or(remaining.len(), |offset| SIZE + offset + 1);
        let (chunk, rest) = remaining.split_at(end);
        remaining = rest;
        Some(chunk)
    })
}

pub fn lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    memchr_iter(b'\n', bytes)
        .chain(std::iter::once(bytes.len()))
        .map(move |end| {
            let line = &bytes[start..end];
            start = end + 1;
            line
        })
}

pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "x86_64")]
    if (16..=128).contains(&line.len()) {
        return fields_masked(line);
    }
    fields_generic(line)
}

fn fields_generic<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut start = 0;
    let mut ends = memchr_iter(b',', line).chain(std::iter::once(line.len()));
    let mut values: [&[u8]; N] = [&[]; N];
    for value in &mut values {
        let end = ends.next()?;
        let raw = &line[start..end];
        let contents = if let Some(quoted) = raw.strip_prefix(b"\"") {
            quoted.strip_suffix(b"\"")?
        } else {
            raw
        };
        if memchr(b'"', contents).is_some() {
            return None;
        }
        *value = contents;
        start = end + 1;
    }
    ends.next().is_none().then_some(values)
}

// SSE2 is part of the x86-64 baseline. Both masks are generated by the
// same vector loads. Longer lines and other architectures use fields_generic.
#[cfg(target_arch = "x86_64")]
fn fields_masked<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::*;

    #[inline]
    fn masks(block: &[u8; 16]) -> (u16, u16) {
        // SAFETY: the array contains 16 readable bytes; loadu needs no alignment.
        unsafe {
            let v = _mm_loadu_si128(block.as_ptr().cast());
            let commas = _mm_cmpeq_epi8(v, _mm_set1_epi8(b',' as i8));
            let quotes = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'"' as i8));
            (
                _mm_movemask_epi8(commas) as u16,
                _mm_movemask_epi8(quotes) as u16,
            )
        }
    }

    let mut commas = 0u128;
    let mut quotes = 0u128;
    let mut add = |offset, block| {
        let (c, q) = masks(block);
        commas |= u128::from(c) << offset;
        quotes |= u128::from(q) << offset;
    };
    let mut blocks = line.chunks_exact(16);
    for (i, block) in blocks.by_ref().enumerate() {
        add(i * 16, block.try_into().unwrap());
    }
    if !blocks.remainder().is_empty() {
        // Overlap the last complete vector so the final load stays in bounds.
        let offset = line.len() - 16;
        add(offset, line[offset..].try_into().unwrap());
    }
    decode_fields(line, commas, quotes.count_ones())
}

// Every quote belongs to exactly one complete outer pair on a valid row.
// Counting them also catches a stray or interior quote in any field.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn decode_fields<const N: usize>(line: &[u8], commas: u128, quotes: u32) -> Option<[&[u8]; N]> {
    if commas.count_ones() as usize + 1 != N {
        return None;
    }
    if quotes == 0 {
        split_fields::<N, false>(line, commas, quotes)
    } else {
        split_fields::<N, true>(line, commas, quotes)
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn split_fields<const N: usize, const QUOTED: bool>(
    line: &[u8],
    mut commas: u128,
    quotes: u32,
) -> Option<[&[u8]; N]> {
    let mut values: [&[u8]; N] = [&[]; N];
    let mut start = 0;
    let mut paired = 0;
    for (i, value) in values.iter_mut().enumerate() {
        let end = if i + 1 == N {
            line.len()
        } else {
            let end = commas.trailing_zeros() as usize;
            commas &= commas.wrapping_sub(1);
            end
        };
        let raw = &line[start..end];
        if QUOTED && raw.first() == Some(&b'"') {
            *value = raw[1..].strip_suffix(b"\"")?;
            paired += 2;
        } else {
            *value = raw;
        }
        start = end + 1;
    }
    (paired == quotes).then_some(values)
}

pub fn visit_rows<'a, const N: usize>(bytes: &'a [u8], mut visit: impl FnMut([&'a [u8]; N])) {
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("popcnt") {
        // SAFETY: both features were detected before entering the specialized loop.
        unsafe { visit_rows_avx2(bytes, visit) };
        return;
    }
    for row in lines(bytes).filter_map(fields::<N>) {
        visit(row);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt")]
unsafe fn visit_rows_avx2<'a, const N: usize>(
    mut bytes: &'a [u8],
    mut visit: impl FnMut([&'a [u8]; N]),
) {
    use std::arch::x86_64::*;
    // All four possible 32-byte loads are inside the slice. Finish a short
    // tail with the general parser, including an unterminated final row.
    while bytes.len() >= 128 {
        let mut commas = 0u128;
        let mut quotes = 0;
        let mut end = None;
        for block in 0..4 {
            let offset = block * 32;
            // SAFETY: offset is at most 96 and loadu has no alignment requirement.
            let data = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(offset).cast()) };
            let mask =
                |ch| _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, _mm256_set1_epi8(ch))) as u32;
            let newline = mask(b'\n' as i8);
            let preceding = newline.wrapping_sub(1) & !newline;
            commas |= u128::from(mask(b',' as i8) & preceding) << offset;
            quotes += (mask(b'"' as i8) & preceding).count_ones();
            if newline != 0 {
                end = Some(offset + newline.trailing_zeros() as usize);
                break;
            }
        }
        if let Some(end) = end {
            if let Some(row) = decode_fields(&bytes[..end], commas, quotes) {
                visit(row);
            }
            bytes = &bytes[end + 1..];
        } else {
            let end = memchr(b'\n', &bytes[128..]).map_or(bytes.len(), |pos| 128 + pos);
            if let Some(row) = fields_generic(&bytes[..end]) {
                visit(row);
            }
            bytes = bytes.get(end + 1..).unwrap_or_default();
        }
    }
    for row in lines(bytes).filter_map(fields::<N>) {
        visit(row);
    }
}

// Parse eight checked ASCII digits in parallel: adjacent pairs, groups
// of four, then the complete eight-digit number.
#[inline]
fn eight_digits(bytes: [u8; 8]) -> Option<u32> {
    let word = u64::from_le_bytes(bytes);
    // Adding 0x46 sets the high bit starting at ':'. Subtracting '0'
    // sets it below '0'. An original high bit always rejects the byte.
    let high = (word
        | word.wrapping_add(0x4646_4646_4646_4646)
        | word.wrapping_sub(0x3030_3030_3030_3030))
        & 0x8080_8080_8080_8080;
    if high != 0 {
        return None;
    }
    let digits = word & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = (digits.wrapping_mul(10) + (digits >> 8)) & 0x00ff_00ff_00ff_00ff;
    let quads = (pairs.wrapping_mul(100) + (pairs >> 16)) & 0x0000_ffff_0000_ffff;
    Some((quads as u32) * 10_000 + (quads >> 32) as u32)
}

pub fn digits(s: &[u8]) -> Option<i64> {
    if let &[a, b, c, d, e, f, g, h, i, j, k, l, m] = s {
        let first = eight_digits([a, b, c, d, e, f, g, h])?;
        let last = eight_digits([b'0', b'0', b'0', i, j, k, l, m])?;
        return Some(i64::from(first) * 100_000 + i64::from(last));
    }
    if s.is_empty() {
        return None;
    }
    s.iter().try_fold(0_i64, |n, &b| {
        b.is_ascii_digit().then_some(())?;
        n.checked_mul(10)?.checked_add(i64::from(b - b'0'))
    })
}

// The proleptic Gregorian calendar repeats every 400 years. Each entry is
// the start of the next year measured from January 1, year 1 of the cycle.
const YEAR_STARTS: [i32; 401] = {
    let mut starts = [0; 401];
    let mut year = 1;
    while year <= 400 {
        let leap = year % 4 == 0 && (year % 100 != 0 || year == 400);
        starts[year] = starts[year - 1] + 365 + leap as i32;
        year += 1;
    }
    starts
};

const MONTH_STARTS: [[i32; 13]; 2] = [
    [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
    [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
];

const fn days_before_year(year: u32) -> i32 {
    let years = year - 1;
    (years / 400) as i32 * YEAR_STARTS[400] + YEAR_STARTS[(years % 400) as usize]
}

fn utc_midnight(year: u32, month: u32, day: u32) -> Option<i64> {
    if year == 0 || !(1..=12).contains(&month) {
        return None;
    }
    let y = ((year - 1) % 400) as usize;
    let months = &MONTH_STARTS[(YEAR_STARTS[y + 1] - YEAR_STARTS[y] == 366) as usize];
    let m = month as usize;
    if day == 0 || day > (months[m] - months[m - 1]) as u32 {
        return None;
    }
    const EPOCH: i32 = days_before_year(1970);
    let days = days_before_year(year) - EPOCH + months[m - 1] + day as i32 - 1;
    Some(i64::from(days) * DAY_MS)
}

// Each parse batch caches validated UTC midnights. A full YYYYMMDD key
// distinguishes months, years and invalid input even when the slot collides.
pub struct Timestamps {
    days: [(u64, i64); 32],
}

impl Default for Timestamps {
    fn default() -> Self {
        Self {
            days: [(u64::from_le_bytes(*b"19700101"), 0); 32],
        }
    }
}

fn pair(a: u8, b: u8) -> Option<u32> {
    let tens = a.wrapping_sub(b'0');
    let ones = b.wrapping_sub(b'0');
    if tens > 9 || ones > 9 {
        return None;
    }
    Some(u32::from(tens) * 10 + u32::from(ones))
}

impl Timestamps {
    pub fn parse(&mut self, s: &[u8]) -> Option<i64> {
        if !matches!(s.len(), 20 | 25) || s[10] != b'T' {
            return digits(s);
        }
        if s[4] != b'-' || s[7] != b'-' || s[13] != b':' || s[16] != b':' {
            return None;
        }

        let hour = pair(s[11], s[12])?;
        let minute = pair(s[14], s[15])?;
        let second = pair(s[17], s[18])?;
        if hour >= 24 || minute >= 60 || second >= 60 {
            return None;
        }
        let offset_minutes = match &s[19..] {
            b"Z" => 0,
            [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2] => {
                let hours = pair(*h1, *h2)?;
                let minutes = pair(*m1, *m2)?;
                if hours >= 24 || minutes >= 60 {
                    return None;
                }
                let offset = i64::from(hours * 60 + minutes);
                if *sign == b'+' { offset } else { -offset }
            }
            _ => return None,
        };
        let key = u64::from_le_bytes([s[0], s[1], s[2], s[3], s[5], s[6], s[8], s[9]]);
        let slot = &mut self.days[(usize::from(s[8]) * 10 + usize::from(s[9])) & 31];
        if slot.0 != key {
            let year = pair(s[0], s[1])? * 100 + pair(s[2], s[3])?;
            *slot = (
                key,
                utc_midnight(year, pair(s[5], s[6])?, pair(s[8], s[9])?)?,
            );
        }
        let seconds = i64::from(hour * 3600 + minute * 60 + second) - offset_minutes * 60;
        Some(slot.1 + seconds * 1000)
    }
}

struct Node<'a> {
    parent: u32,
    size: u32,
    minimum: Id<'a>,
}

#[derive(Default)]
struct Components<'a>(Vec<Node<'a>>);

impl<'a> Components<'a> {
    fn add(&mut self, name: Id<'a>) -> u32 {
        let id = u32::try_from(self.0.len()).expect("alias index overflow");
        self.0.push(Node {
            parent: id,
            size: 1,
            minimum: name,
        });
        id
    }

    fn root(&mut self, mut id: u32) -> u32 {
        while self.0[id as usize].parent != id {
            let parent = self.0[id as usize].parent;
            let grandparent = self.0[parent as usize].parent;
            self.0[id as usize].parent = grandparent;
            id = grandparent;
        }
        id
    }

    fn unite(&mut self, a: u32, b: u32) {
        let (mut left, mut right) = (self.root(a), self.root(b));
        if left == right {
            return;
        }
        if self.0[left as usize].size < self.0[right as usize].size {
            std::mem::swap(&mut left, &mut right);
        }
        self.0[right as usize].parent = left;
        let right = &self.0[right as usize];
        let (size, minimum) = (right.size, right.minimum);
        let left = &mut self.0[left as usize];
        left.size += size;
        left.minimum = left.minimum.min(minimum);
    }
}

pub struct LinkedPeople<'a> {
    pub aliases: LinkedNames<'a>,
    pub names: Vec<Id<'a>>,
}

fn linked_people(files: &[Mmap]) -> LinkedPeople<'_> {
    let mut aliases = LinkedNames::default();
    let mut components = Components::default();
    for file in files {
        for [a, b] in lines(body(file)).filter_map(fields::<2>) {
            if a.is_empty() || b.is_empty() {
                continue;
            }
            let left = aliases.intern(a, || components.add(a));
            let right = aliases.intern(b, || components.add(b));
            components.unite(left, right);
        }
    }

    let mut names = Vec::new();
    let root_numbers: Vec<u32> = components
        .0
        .iter()
        .enumerate()
        .map(|(id, node)| {
            if node.parent as usize != id {
                return 0;
            }
            let number = u32::try_from(names.len()).expect("shopper index overflow");
            names.push(node.minimum);
            number
        })
        .collect();
    // The key tables already have the layout used by the event and click readers.
    // Replace each union-find index in place once all links are known.
    aliases.remap(|id| root_numbers[components.root(id) as usize]);
    LinkedPeople { aliases, names }
}

#[cfg(all(test, target_arch = "x86_64"))]
mod tests {
    use super::{fields, fields_generic};

    #[test]
    fn delimiter_masks_match_the_scalar_parser_at_every_vector_boundary() {
        for len in 15..=130 {
            for delimiter in 0..len {
                let mut line = vec![b'x'; len];
                line[delimiter] = b',';
                for quote in 0..len {
                    let previous = std::mem::replace(&mut line[quote], b'"');
                    assert_eq!(fields::<2>(&line), fields_generic::<2>(&line));
                    line[quote] = previous;
                }
                if delimiter >= 2 {
                    line[0] = b'"';
                    line[delimiter - 1] = b'"';
                }
                if delimiter + 3 <= len {
                    line[delimiter + 1] = b'"';
                    line[len - 1] = b'"';
                }
                assert_eq!(fields::<2>(&line), fields_generic::<2>(&line));
            }
        }
    }
}

#[cfg(test)]
mod ingestion_tests {
    use super::{Timestamps, fields_generic, lines, visit_rows};

    #[test]
    fn thirteen_digit_fast_path_matches_checked_decimal() {
        let raw = *b"1791071890123";
        for pos in 0..raw.len() {
            for ch in 0..=u8::MAX {
                let mut value = raw;
                value[pos] = ch;
                let expected = value.iter().try_fold(0i64, |n, &c| {
                    c.is_ascii_digit().then(|| n * 10 + i64::from(c - b'0'))
                });
                assert_eq!(super::digits(&value), expected);
            }
        }
    }

    #[test]
    fn batch_parser_matches_field_rules_across_vectors_and_tail() {
        let mut input = Vec::new();
        for n in 0..=256 {
            let value = "a".repeat(n);
            for row in [
                format!("{value},x,\"é\","),
                format!("a,\"{value}\",c,\r"),
                format!("a,{value}\",c,d"),
                format!("a,\"{value}\"\",c,d"),
                format!("{value},wrong,count"),
                format!("{value},1,2,3,4"),
                format!("a,{value},c,\0"),
            ] {
                input.extend_from_slice(row.as_bytes());
                input.push(b'\n');
            }
        }
        input.pop();
        for start in 0..64 {
            let bytes = &input[start..];
            let expected: Vec<_> = lines(bytes).filter_map(fields_generic::<4>).collect();
            let mut actual = Vec::new();
            visit_rows(bytes, |row| actual.push(row));
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn calendar_tables_match_all_dates_in_a_gregorian_cycle() {
        use chrono::NaiveDate;
        for year in (1600..=2000).chain([1, 9999]) {
            for month in 0..=13 {
                for day in 0..=32 {
                    let expected = NaiveDate::from_ymd_opt(year as i32, month, day).map(|date| {
                        date.and_hms_opt(0, 0, 0)
                            .unwrap()
                            .and_utc()
                            .timestamp_millis()
                    });
                    assert_eq!(super::utc_midnight(year, month, day), expected);
                }
            }
        }
    }

    #[test]
    fn cached_dates_validate_the_full_date_and_time_zone() {
        let mut ts = Timestamps::default();
        for (s, expected) in [
            ("1970-01-01T00:00:00Z", Some(0)),
            ("1970-01-01T00:00:00+00:01", Some(-60_000)),
            ("1970-01-01T00:00:00-00:01", Some(60_000)),
            ("1969-12-31T23:59:59Z", Some(-1000)),
            ("2000-02-29T00:00:00Z", Some(951782400000)),
            ("1900-02-29T00:00:00Z", None),
            ("2000-02-29T00:00:00+24:00", None),
            ("2000-02-29T23:59:60Z", None),
            ("2000-02-29T23:59:59+00:60", None),
            ("0000-01-01T00:00:00Z", None),
            ("2000/02-29T00:00:00Z", None),
            ("2000-02-29T00:00:0/Z", None),
            ("00000000000000000000000042", Some(42)),
        ] {
            assert_eq!(ts.parse(s.as_bytes()), expected, "{s}");
        }
    }
}
