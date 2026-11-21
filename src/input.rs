use std::{
    fs::{self, File},
    ops::Range,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use memchr::{memchr, memchr_iter};
use memmap2::{Mmap, MmapOptions};
use rustc_hash::FxHashMap;

pub const DAY_MS: i64 = 86_400_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;
pub const GAP_MS: i64 = 30 * 60 * 1000;
pub type Id<'a> = &'a [u8];

pub struct Input {
    events: Vec<PathBuf>,
    clicks: Vec<Mmap>,
    links: Vec<Mmap>,
    products: Option<Mmap>,
    campaigns: Option<Mmap>,
    pub start: i64,
    pub end: i64,
}

pub struct Dimensions<'a> {
    pub people: FxHashMap<Id<'a>, Id<'a>>,
    pub products: FxHashMap<Id<'a>, u32>,
    pub categories: Vec<Id<'a>>,
    pub campaigns: FxHashMap<Id<'a>, usize>,
    pub campaign_names: Vec<(Id<'a>, Id<'a>)>,
}

impl Input {
    pub fn open(path: &Path) -> Result<Self> {
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(path.join("manifest.json"))?)?;
        let time = |key: &str| {
            manifest[key]
                .as_str()
                .and_then(|s| timestamp(s.as_bytes()))
                .with_context(|| format!("invalid manifest field {key}"))
        };
        let (start, end) = (time("window_start")?, time("window_end")?);
        ensure!(start <= end, "the reporting window is reversed");
        Ok(Self {
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
        for path in &self.events {
            if let Some(mapping) = map_file(path)? {
                let mut offset = mapping.len() - body(&mapping).len();
                for bytes in split_chunks(&mapping[offset..]) {
                    let end = offset + bytes.len();
                    chunks.push(EventChunk {
                        path,
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

// The planning pass touches only headers and chunk boundaries. Each parser
// maps its own nonempty range and releases the mapping before its next chunk.
pub struct EventChunk<'a> {
    path: &'a Path,
    pub range: Range<usize>,
}

impl EventChunk<'_> {
    pub fn map(&self) -> Result<Mmap> {
        let file =
            File::open(self.path).with_context(|| format!("opening {}", self.path.display()))?;
        // The same read-only file supplied the boundaries above.
        Ok(unsafe {
            MmapOptions::new()
                .offset(self.range.start as u64)
                .len(self.range.len())
                .map(&file)
        }?)
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
    if commas.count_ones() as usize + 1 != N {
        return None;
    }
    let mut values: [&[u8]; N] = [&[]; N];
    let mut start = 0;
    for value in &mut values {
        let end = if commas == 0 {
            line.len()
        } else {
            commas.trailing_zeros() as usize
        };
        commas &= commas.wrapping_sub(1);
        if start < end && line[start] == b'"' {
            if end - start < 2 || line[end - 1] != b'"' {
                return None;
            }
            quotes &= !((1u128 << start) | (1u128 << (end - 1)));
            *value = &line[start + 1..end - 1];
        } else {
            *value = &line[start..end];
        }
        start = end + 1;
    }
    (quotes == 0).then_some(values)
}

pub fn digits(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    s.iter().try_fold(0_i64, |n, &b| {
        b.is_ascii_digit().then_some(())?;
        n.checked_mul(10)?.checked_add(i64::from(b - b'0'))
    })
}

pub fn timestamp(s: &[u8]) -> Option<i64> {
    if !matches!(s.len(), 20 | 25) || s.get(10) != Some(&b'T') {
        return digits(s);
    }
    if s[4] != b'-' || s[7] != b'-' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = digits(&s[..4])? as i32;
    let month = digits(&s[5..7])? as u32;
    let day = digits(&s[8..10])? as u32;
    let first = digits(&s[11..13])? as u32;
    let minute = digits(&s[14..16])? as u32;
    let second = digits(&s[17..19])? as u32;
    if year == 0 {
        return None;
    }
    let wall_time = NaiveDate::from_ymd_opt(year, month, day)?
        .and_hms_opt(first, minute, second)?
        .and_utc()
        .timestamp_millis();
    match &s[19..] {
        b"Z" => Some(wall_time),
        [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2] => {
            let hours = digits(&[*h1, *h2])?;
            let minutes = digits(&[*m1, *m2])?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let offset = (hours * 60 + minutes) * 60_000;
            Some(if *sign == b'+' {
                wall_time - offset
            } else {
                wall_time + offset
            })
        }
        _ => None,
    }
}

#[derive(Default)]
struct Components {
    parents: Vec<usize>,
    sizes: Vec<usize>,
}

impl Components {
    fn add(&mut self) -> usize {
        let id = self.parents.len();
        self.parents.push(id);
        self.sizes.push(1);
        id
    }

    fn root(&mut self, mut id: usize) -> usize {
        while self.parents[id] != id {
            self.parents[id] = self.parents[self.parents[id]];
            id = self.parents[id];
        }
        id
    }

    fn unite(&mut self, a: usize, b: usize) {
        let (mut left, mut right) = (self.root(a), self.root(b));
        if left == right {
            return;
        }
        if self.sizes[left] < self.sizes[right] {
            std::mem::swap(&mut left, &mut right);
        }
        self.parents[right] = left;
        self.sizes[left] += self.sizes[right];
    }
}

fn linked_people(files: &[Mmap]) -> FxHashMap<Id<'_>, Id<'_>> {
    let mut ids = FxHashMap::default();
    let mut components = Components::default();
    for file in files {
        for [a, b] in lines(body(file)).filter_map(fields::<2>) {
            if !a.is_empty() && !b.is_empty() {
                let a = *ids.entry(a).or_insert_with(|| components.add());
                let b = *ids.entry(b).or_insert_with(|| components.add());
                components.unite(a, b);
            }
        }
    }

    let mut names: Vec<Id<'_>> = vec![&[]; ids.len()];
    for (&alias, &id) in &ids {
        let root = components.root(id);
        if names[root].is_empty() || alias < names[root] {
            names[root] = alias;
        }
    }
    ids.into_iter()
        .map(|(alias, id)| (alias, names[components.root(id)]))
        .collect()
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
