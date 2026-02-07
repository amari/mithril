//! See https://github.com/apple/foundationdb/blob/main/design/tuple.md

use ::smallvec::SmallVec;
use std::collections::VecDeque;
use std::io;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("IO error")]
    Io(#[from] io::Error),
}

pub struct Bytes<'a> {
    inner: &'a [u8],
}

impl<'a> Bytes<'a> {
    pub const fn new(inner: &'a [u8]) -> Bytes<'a> {
        Bytes { inner }
    }
}

fn write_escaped_bytes(buf: &[u8], w: &mut impl io::Write) -> Result<(), TupleError> {
    let mut parts = buf.split(|v| *v == 0);

    if let Some(part) = parts.next() {
        let _ = w.write_all(part)?;
    }

    for part in parts {
        let _ = w.write_all(b"\x00\xff")?;
        let _ = w.write_all(part)?;
    }

    Ok(())
}

pub trait Pack {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError>;

    fn pack_nested(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        self.pack(w)
    }

    fn pack_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();

        self.pack(&mut v).unwrap();

        v
    }

    fn pack_to_vec_deque(&self) -> VecDeque<u8> {
        let mut v = VecDeque::new();

        self.pack(&mut v).unwrap();

        v
    }

    fn pack_to_small_vec<const N: usize>(&self) -> SmallVec<[u8; N]> {
        let mut v = SmallVec::new();

        self.pack(&mut v).unwrap();

        v
    }

    fn pack_prefix_range(
        &self,
        start: &mut impl io::Write,
        end: &mut impl io::Write,
    ) -> Result<(), TupleError> {
        let _ = self.pack(start)?;
        let _ = self.pack(end)?;

        start.write_all(b"\x00")?;
        end.write_all(b"\xFF")?;

        Ok(())
    }

    fn pack_prefix_range_to_vec_range(&self) -> Range<Vec<u8>> {
        let mut start = Vec::new();
        let mut end = Vec::new();

        self.pack_prefix_range(&mut start, &mut end).unwrap();

        start..end
    }

    fn pack_prefix_range_to_small_vec_range<const N: usize>(&self) -> Range<SmallVec<[u8; N]>> {
        let mut start = SmallVec::new();
        let mut end = SmallVec::new();

        self.pack_prefix_range(&mut start, &mut end).unwrap();

        start..end
    }
}

impl<'a, T: Pack> Pack for &'a T {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        (**self).pack(w)
    }

    fn pack_nested(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        (**self).pack_nested(w)
    }
}

impl<T: Pack> Pack for Option<T> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        match self {
            None => ().pack(w),
            Some(v) => v.pack(w),
        }
    }

    fn pack_nested(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        match self {
            None => ().pack_nested(w),
            Some(v) => v.pack_nested(w),
        }
    }
}

impl Pack for () {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        let _ = w.write_all(b"\x00")?;

        Ok(())
    }

    fn pack_nested(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        let _ = w.write_all(b"\x00\xff")?;

        Ok(())
    }
}

impl<'a> Pack for Bytes<'a> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        let _ = w.write_all(b"\x01")?;
        let _ = write_escaped_bytes(self.inner, w)?;
        let _ = w.write_all(b"\x00")?;

        Ok(())
    }
}

impl Pack for [u8] {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self).pack(w)
    }
}

impl<const N: usize> Pack for [u8; N] {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

impl Pack for Box<[u8]> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

impl Pack for Arc<[u8]> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

impl Pack for Rc<[u8]> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

impl Pack for Vec<u8> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

impl Pack for VecDeque<u8> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        let (a, b) = self.as_slices();

        let _ = w.write_all(b"\x01")?;
        let _ = write_escaped_bytes(a, w)?;
        if !b.is_empty() {
            let _ = write_escaped_bytes(b, w)?;
        }
        let _ = w.write_all(b"\x00")?;

        Ok(())
    }
}

impl Pack for str {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        let _ = w.write_all(b"\x02")?;
        let _ = write_escaped_bytes(self.as_bytes(), w)?;
        let _ = w.write_all(b"\x00")?;

        Ok(())
    }
}

impl Pack for String {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Pack::pack(self.as_str(), w)
    }
}

// Generate impls for multiple tuple arities
macro_rules! impl_pack_for_tuples {
    ($( ($($T:ident),+) ),+ $(,)?) => {
        $(
            impl<$($T: Pack),+> Pack for ($($T,)+) {
                fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
                    // Fixed type tag
                    w.write_all(b"\x05")?;

                    // Destructure only to bind names for repetition
                    #[allow(non_snake_case)]
                    let ($($T,)+) = self;

                    // Encode each element using pack_nested
                    $(
                        $T.pack_nested(w)?;
                    )+

                    // Fixed terminator — NOT a value, not dependent on tuple contents
                    w.write_all(b"\x00")?;

                    Ok(())
                }
            }
        )+
    };
}

impl_pack_for_tuples! {
    (T0),
    (T0, T1),
    (T0, T1, T2),
    (T0, T1, T2, T3),
    (T0, T1, T2, T3, T4),
    (T0, T1, T2, T3, T4, T5),
    (T0, T1, T2, T3, T4, T5, T6),
    (T0, T1, T2, T3, T4, T5, T6, T7),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9),
}

#[inline(always)]
fn pack_signed_i64(x: i64, w: &mut impl io::Write) -> Result<(), TupleError> {
    // Zero is a special case
    if x == 0 {
        w.write_all(&[0x14])?;
        return Ok(());
    }

    let negative = x < 0;

    // Step 1: magnitude as u64
    let mag = x.unsigned_abs();

    // Step 2: convert to big-endian bytes
    let mut buf = mag.to_be_bytes();

    // Step 3: strip leading zeros (minimal representation)
    let first = buf.iter().position(|&b| b != 0).unwrap();
    let bytes = &mut buf[first..];

    // Step 4: one's complement for negative values
    if negative {
        for b in bytes.iter_mut() {
            *b = 0xFF - *b;
        }
    }

    // Step 5: compute type code
    let len = bytes.len();

    let type_code = if negative {
        0x14u8 - (len as u8)
    } else {
        0x14u8 + (len as u8)
    };

    // Step 6: write type code + bytes
    w.write_all(&[type_code])?;
    w.write_all(bytes)?;

    Ok(())
}

impl Pack for i8 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_signed_i64(*self as i64, w)
    }
}

impl Pack for i16 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_signed_i64(*self as i64, w)
    }
}

impl Pack for i32 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_signed_i64(*self as i64, w)
    }
}

impl Pack for i64 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_signed_i64(*self, w)
    }
}

#[inline(always)]
fn pack_unsigned_u64(x: u64, w: &mut impl io::Write) -> Result<(), TupleError> {
    if x == 0 {
        w.write_all(&[0x14])?;
        return Ok(());
    }

    let buf = x.to_be_bytes();
    let first = buf.iter().position(|&b| b != 0).unwrap();
    let bytes = &buf[first..];

    let len = bytes.len();
    let type_code = 0x14u8 + (len as u8);

    w.write_all(&[type_code])?;
    w.write_all(bytes)?;

    Ok(())
}

impl Pack for u8 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_unsigned_u64(*self as u64, w)
    }
}

impl Pack for u16 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_unsigned_u64(*self as u64, w)
    }
}

impl Pack for u32 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_unsigned_u64(*self as u64, w)
    }
}

impl Pack for u64 {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        pack_unsigned_u64(*self, w)
    }
}

impl Pack for bool {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        match *self {
            false => w.write_all(b"\x26")?,
            true => w.write_all(b"\x27")?,
        }

        Ok(())
    }
}

impl Pack for ::uuid::Uuid {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        let _ = w.write_all(b"\x30")?;
        let _ = w.write_all(self.as_bytes())?;

        Ok(())
    }
}

impl Pack for ::bytes::Bytes {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

impl Pack for ::bytes::BytesMut {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

impl<const N: usize> Pack for SmallVec<[u8; N]> {
    fn pack(&self, w: &mut impl io::Write) -> Result<(), TupleError> {
        Bytes::new(self.as_ref()).pack(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn vecdeque_and_vec_encode_the_same(bytes: Vec<u8>) {
            use std::collections::VecDeque;

            let vd: VecDeque<u8> = bytes.iter().copied().collect();

            prop_assert_eq!(
                bytes.pack_to_vec(),
                vd.pack_to_vec(),
            );
        }

        #[test]
        fn packs_vec(v: Vec<u8>) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }

        #[test]
        fn packs_vecdeque(v: VecDeque<u8>) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }

        #[test]
        fn packs_string(v: String) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }

        #[test]
        fn packs_bool(v: bool) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }

        #[test]
        fn packs_u8(v: u8) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }

        #[test]
        fn packs_u16(v: u16) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }

        #[test]
        fn packs_u32(v: u32) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }

        #[test]
        fn packs_u64(v: u64) {
            let mut buf = Vec::new();

            let _ = v.pack(&mut buf).unwrap();

            prop_assert_eq!(buf, v.pack_to_vec());
        }
    }

    fn assert_order_preserved<T: Pack + Ord + Clone + std::fmt::Debug>(values: Vec<T>) {
        // Sort by Rust's natural ordering
        let mut rust_sorted = values.clone();
        rust_sorted.sort();

        // Pack each value
        let mut packed_sorted: Vec<(Vec<u8>, T)> =
            values.into_iter().map(|v| (v.pack_to_vec(), v)).collect();

        // Sort lexicographically by packed bytes
        packed_sorted.sort_by(|a, b| a.0.cmp(&b.0));

        // Extract the values in packed order
        let packed_order: Vec<T> = packed_sorted.into_iter().map(|(_, v)| v).collect();

        // Compare
        assert_eq!(
            rust_sorted, packed_order,
            "Ordering mismatch between Rust and tuple encoding"
        );
    }

    proptest! {
        #[test]
        fn test_order_preserved_i64(values in proptest::collection::vec(any::<i64>(), 1..50)) {
            assert_order_preserved(values);
        }

        #[test]
        fn test_order_preserved_tuples(
            values in proptest::collection::vec(
                (any::<i64>(), any::<u64>(), ".*"), // (i64, u64, String)
                1..50
            )
        ) {
            assert_order_preserved(values);
        }

        #[test]
        fn test_order_preserved_bytes(
            values in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..20),
                1..50
            )
        ) {
            let values: Vec<Vec<u8>> = values;
            assert_order_preserved(values);
        }
    }
}
