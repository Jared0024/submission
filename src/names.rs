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
            // Remove the empty high bit from adjacent pairs, then from
            // each pair of pairs. The low 56 bits now hold all eight bytes.
            let pairs = (x & 0x007f_007f_007f_007f) | ((x & 0x7f00_7f00_7f00_7f00) >> 1);
            let quads = (pairs & 0x0000_3fff_0000_3fff) | ((pairs & 0x3fff_0000_3fff_0000) >> 2);
            let eight = (quads & 0x0fff_ffff) | ((quads >> 32) << 28);
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
}
