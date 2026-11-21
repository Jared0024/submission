use rustc_hash::FxHashMap;
use std::{borrow::Borrow, hash::Hash};

// Nine nonzero ASCII bytes fit in 63 bits, padded on the right. All other
// names are compared byte-for-byte in the separate variable-length table.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct InlineId(pub u64);

impl InlineId {
    #[inline]
    pub fn new(bytes: &[u8]) -> Option<Self> {
        if let [first @ .., last] = bytes
            && let Ok(word) = <[u8; 8]>::try_from(first)
        {
            let x = u64::from_be_bytes(word);
            const HIGH: u64 = 0x8080_8080_8080_8080;
            let zeros = x.wrapping_sub(0x0101_0101_0101_0101) & !x;
            if (x | zeros) & HIGH != 0 || !(1..128).contains(last) {
                return None;
            }
            #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
            // This binary only runs on BMI2-capable CPUs. Extract the seven low bits of each byte.
            let eight = unsafe { std::arch::x86_64::_pext_u64(x, !HIGH) };
            #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
            let eight = {
                // Remove the empty high bit from adjacent pairs, then from
                // each pair of pairs. The low 56 bits now hold all eight bytes.
                let pairs = (x & 0x007f_007f_007f_007f) | ((x & 0x7f00_7f00_7f00_7f00) >> 1);
                let quads =
                    (pairs & 0x0000_3fff_0000_3fff) | ((pairs & 0x3fff_0000_3fff_0000) >> 2);
                (quads & 0x0fff_ffff) | ((quads >> 32) << 28)
            };
            return Some(Self((eight << 7) | u64::from(*last)));
        }
        if bytes.len() > 8 {
            return None;
        }
        let mut value = 0;
        for &b in bytes {
            if !(1..128).contains(&b) {
                return None;
            }
            value = (value << 7) | u64::from(b);
        }
        Some(Self(value << (7 * (9 - bytes.len()))))
    }

    pub fn bytes(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        for i in (0..9).rev() {
            let byte = ((self.0 >> (7 * i)) & 127) as u8;
            if byte == 0 {
                break;
            }
            out.push(byte);
        }
        out
    }
}

#[derive(Default)]
pub struct NameMap<K> {
    pub short: FxHashMap<InlineId, u32>,
    pub other: FxHashMap<K, u32>,
}

impl<K: Borrow<[u8]> + Eq + Hash> NameMap<K> {
    pub fn len(&self) -> usize {
        self.short.len() + self.other.len()
    }

    #[inline]
    pub fn get(&self, id: &[u8], short: Option<InlineId>) -> Option<u32> {
        match short {
            Some(key) => self.short.get(&key),
            None => self.other.get(id),
        }
        .copied()
    }
}

// Five short aliases and their canonical shopper numbers fit in one cache line.
// InlineId reserves the high bit, so every valid key differs from EMPTY_KEY.
const EMPTY_KEY: u64 = u64::MAX;
const BUCKET_WIDTH: usize = 5;

#[repr(C, align(64))]
#[derive(Clone)]
struct AliasBucket {
    keys: [u64; BUCKET_WIDTH],
    people: [u32; BUCKET_WIDTH],
}

impl Default for AliasBucket {
    fn default() -> Self {
        Self {
            keys: [EMPTY_KEY; BUCKET_WIDTH],
            people: [0; BUCKET_WIDTH],
        }
    }
}

impl AliasBucket {
    fn is_full(&self) -> bool {
        self.keys[BUCKET_WIDTH - 1] != EMPTY_KEY
    }

    fn insert(&mut self, key: InlineId, person: u32) {
        let slot = self
            .keys
            .iter()
            .position(|&stored| stored == EMPTY_KEY)
            .unwrap();
        self.keys[slot] = key.0;
        self.people[slot] = person;
    }
}

pub struct LinkedNames<'a> {
    buckets: Vec<AliasBucket>,
    short_count: usize,
    other: FxHashMap<&'a [u8], u32>,
}

impl Default for LinkedNames<'_> {
    fn default() -> Self {
        Self {
            buckets: vec![AliasBucket::default()],
            short_count: 0,
            other: FxHashMap::default(),
        }
    }
}

impl<'a> LinkedNames<'a> {
    #[inline]
    fn bucket(&self, key: InlineId) -> usize {
        use std::hash::BuildHasher;
        rustc_hash::FxBuildHasher.hash_one(key) as usize & (self.buckets.len() - 1)
    }

    // Return the stored number, or the first bucket with room for this key.
    #[inline]
    fn probe(&self, key: InlineId) -> Result<u32, usize> {
        let mut index = self.bucket(key);
        loop {
            let bucket = &self.buckets[index];
            for (&stored, &person) in bucket.keys.iter().zip(&bucket.people) {
                if stored == key.0 {
                    return Ok(person);
                }
            }
            if !bucket.is_full() {
                return Err(index);
            }
            index = (index + 1) & (self.buckets.len() - 1);
        }
    }

    fn grow(&mut self) {
        let count = self
            .buckets
            .len()
            .checked_mul(2)
            .expect("alias table overflow");
        let old = std::mem::replace(&mut self.buckets, vec![AliasBucket::default(); count]);
        for bucket in old {
            for (key, person) in bucket.keys.into_iter().zip(bucket.people) {
                if key == EMPTY_KEY {
                    break;
                }
                let key = InlineId(key);
                let mut index = self.bucket(key);
                while self.buckets[index].is_full() {
                    index = (index + 1) & (count - 1);
                }
                self.buckets[index].insert(key, person);
            }
        }
    }

    pub fn intern(&mut self, name: &'a [u8], add: impl FnOnce() -> u32) -> u32 {
        let Some(key) = InlineId::new(name) else {
            return *self.other.entry(name).or_insert_with(add);
        };
        let mut index = match self.probe(key) {
            Ok(person) => return person,
            Err(index) => index,
        };
        // At most four entries per five-slot bucket on average. A probe for a
        // missing name always reaches an empty slot, including during growth.
        if self.short_count == self.buckets.len() * (BUCKET_WIDTH - 1) {
            self.grow();
            index = self.probe(key).unwrap_err();
        }
        let person = add();
        self.buckets[index].insert(key, person);
        self.short_count += 1;
        person
    }

    pub fn remap(&mut self, mut resolve: impl FnMut(u32) -> u32) {
        for bucket in &mut self.buckets {
            for (&key, person) in bucket.keys.iter().zip(&mut bucket.people) {
                if key == EMPTY_KEY {
                    break;
                }
                *person = resolve(*person);
            }
        }
        for person in self.other.values_mut() {
            *person = resolve(*person);
        }
    }

    #[inline]
    pub fn get(&self, name: &[u8], short: Option<InlineId>) -> Option<u32> {
        match short {
            Some(key) => self.probe(key).ok(),
            None => self.other.get(name).copied(),
        }
    }
}

impl NameMap<Box<[u8]>> {
    pub fn intern_short(&mut self, key: InlineId) -> u32 {
        let next = self.len();
        *self.short.entry(key).or_insert_with(|| next_index(next))
    }

    pub fn intern_other(&mut self, id: &[u8]) -> u32 {
        if let Some(&known) = self.other.get(id) {
            return known;
        }
        let n = next_index(self.len());
        self.other.insert(id.into(), n);
        n
    }

    pub fn intern(&mut self, id: &[u8], short: Option<InlineId>) -> u32 {
        match short {
            Some(key) => self.intern_short(key),
            None => self.intern_other(id),
        }
    }
}

fn next_index(len: usize) -> u32 {
    let index = u32::try_from(len).expect("shopper index overflow");
    assert_ne!(index, u32::MAX, "shopper index overflow");
    index
}

#[cfg(test)]
mod tests {
    use super::InlineId;

    #[test]
    fn nine_byte_keys_round_trip_and_reject_spill_bytes() {
        let mut raw = *b"abcdefghi";
        for i in 0..9 {
            for byte in 0..=255 {
                raw[i] = byte;
                if (1..128).contains(&byte) {
                    assert_eq!(InlineId::new(&raw).unwrap().bytes(), raw);
                } else {
                    assert!(InlineId::new(&raw).is_none());
                }
            }
            raw[i] = b'a';
        }
        for n in 0..9 {
            assert_eq!(InlineId::new(&raw[..n]).unwrap().bytes(), &raw[..n]);
        }
        assert!(InlineId::new(b"0123456789").is_none());
    }

    #[test]
    fn linked_alias_lookup_survives_collisions_and_wraparound() {
        use super::{InlineId, LinkedNames};
        assert_eq!(size_of::<super::AliasBucket>(), 64);
        assert_eq!(align_of::<super::AliasBucket>(), 64);
        let words: Vec<_> = (0..4096).map(|i| format!("key{i:04}")).collect();
        // Find nine names with the same final bucket in a four-bucket table.
        let probe = LinkedNames {
            buckets: vec![super::AliasBucket::default(); 4],
            short_count: 0,
            other: Default::default(),
        };
        let colliding: Vec<_> = words
            .iter()
            .filter(|name| probe.bucket(InlineId::new(name.as_bytes()).unwrap()) == 3)
            .take(10)
            .collect();
        assert_eq!(colliding.len(), 10);
        let mut table = LinkedNames::default();
        for (i, name) in colliding[..9].iter().enumerate() {
            assert_eq!(table.intern(name.as_bytes(), || i as u32), i as u32);
            assert_eq!(
                table.intern(name.as_bytes(), || panic!("duplicate insertion")),
                i as u32
            );
        }
        table.intern("é".as_bytes(), || 15);
        table.remap(|id| id + 1);
        for (i, name) in colliding[..9].iter().enumerate() {
            assert_eq!(
                table.get(name.as_bytes(), InlineId::new(name.as_bytes())),
                Some(i as u32 + 1)
            );
        }
        let absent = colliding[9].as_bytes();
        assert_eq!(table.get(absent, InlineId::new(absent)), None);
        assert_eq!(table.get("é".as_bytes(), None), Some(16));
        assert_eq!(table.get(b"", InlineId::new(b"")), None);
    }
}
