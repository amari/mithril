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

impl<'a, T: Pack> Pack for &'a T
where
    T: ?Sized,
{
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
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::Arc;

    // Helper to pack and return bytes
    fn pack_to_vec<T: Pack + ?Sized>(value: &T) -> Vec<u8> {
        value.pack_to_vec()
    }

    // Tests for TupleError
    #[test]
    fn test_tuple_error_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::Other, "test error");
        let tuple_err: TupleError = io_err.into();
        assert!(matches!(tuple_err, TupleError::Io(_)));
        assert!(tuple_err.to_string().contains("IO error"));
    }

    // Tests for Bytes
    #[test]
    fn test_bytes_new() {
        let data = b"hello";
        let bytes = Bytes::new(data);
        assert_eq!(bytes.inner, data);
    }

    #[test]
    fn test_bytes_pack_simple() {
        let bytes = Bytes::new(b"hello");
        let packed = pack_to_vec(&bytes);
        assert_eq!(packed, b"\x01hello\x00");
    }

    #[test]
    fn test_bytes_pack_empty() {
        let bytes = Bytes::new(b"");
        let packed = pack_to_vec(&bytes);
        assert_eq!(packed, b"\x01\x00");
    }

    #[test]
    fn test_bytes_pack_with_null_byte() {
        let bytes = Bytes::new(b"hel\x00lo");
        let packed = pack_to_vec(&bytes);
        assert_eq!(packed, b"\x01hel\x00\xfflo\x00");
    }

    #[test]
    fn test_bytes_pack_with_multiple_null_bytes() {
        let bytes = Bytes::new(b"\x00\x00\x00");
        let packed = pack_to_vec(&bytes);
        assert_eq!(packed, b"\x01\x00\xff\x00\xff\x00\xff\x00");
    }

    #[test]
    fn test_bytes_pack_starting_with_null() {
        let bytes = Bytes::new(b"\x00abc");
        let packed = pack_to_vec(&bytes);
        assert_eq!(packed, b"\x01\x00\xffabc\x00");
    }

    // Tests for write_escaped_bytes
    #[test]
    fn test_write_escaped_bytes_no_nulls() {
        let mut buf = Vec::new();
        write_escaped_bytes(b"hello", &mut buf).unwrap();
        assert_eq!(buf, b"hello");
    }

    #[test]
    fn test_write_escaped_bytes_with_nulls() {
        let mut buf = Vec::new();
        write_escaped_bytes(b"a\x00b", &mut buf).unwrap();
        assert_eq!(buf, b"a\x00\xffb");
    }

    // Tests for Pack trait reference impl
    #[test]
    fn test_pack_reference() {
        let value = 42i32;
        let ref_value = &value;
        assert_eq!(pack_to_vec(&value), pack_to_vec(&ref_value));
    }

    #[test]
    fn test_pack_nested_reference() {
        let value = ();
        let ref_value = &value;
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        value.pack_nested(&mut buf1).unwrap();
        ref_value.pack_nested(&mut buf2).unwrap();
        assert_eq!(buf1, buf2);
    }

    // Tests for Option<T>
    #[test]
    fn test_option_none() {
        let value: Option<i32> = None;
        let packed = pack_to_vec(&value);
        assert_eq!(packed, b"\x00");
    }

    #[test]
    fn test_option_some() {
        let value: Option<i32> = Some(42);
        let packed = pack_to_vec(&value);
        assert_eq!(packed, pack_to_vec(&42i32));
    }

    #[test]
    fn test_option_nested_none() {
        let value: Option<()> = None;
        let mut buf = Vec::new();
        value.pack_nested(&mut buf).unwrap();
        assert_eq!(buf, b"\x00\xff");
    }

    #[test]
    fn test_option_nested_some() {
        let value: Option<i32> = Some(42);
        let mut buf = Vec::new();
        value.pack_nested(&mut buf).unwrap();
        assert_eq!(buf, pack_to_vec(&42i32));
    }

    // Tests for ()
    #[test]
    fn test_unit_pack() {
        let packed = pack_to_vec(&());
        assert_eq!(packed, b"\x00");
    }

    #[test]
    fn test_unit_pack_nested() {
        let mut buf = Vec::new();
        ().pack_nested(&mut buf).unwrap();
        assert_eq!(buf, b"\x00\xff");
    }

    // Tests for byte slices and arrays
    #[test]
    fn test_u8_slice_pack() {
        let data: &[u8] = b"test";
        let packed = pack_to_vec(data);
        assert_eq!(packed, b"\x01test\x00");
    }

    #[test]
    fn test_u8_array_pack() {
        let data: [u8; 4] = *b"test";
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    #[test]
    fn test_boxed_u8_slice_pack() {
        let data: Box<[u8]> = Box::new(*b"test");
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    #[test]
    fn test_arc_u8_slice_pack() {
        let data: Arc<[u8]> = Arc::from(b"test" as &[u8]);
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    #[test]
    fn test_rc_u8_slice_pack() {
        let data: Rc<[u8]> = Rc::from(b"test" as &[u8]);
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    #[test]
    fn test_vec_u8_pack() {
        let data: Vec<u8> = b"test".to_vec();
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    // Tests for VecDeque<u8>
    #[test]
    fn test_vec_deque_u8_pack_simple() {
        let mut data: VecDeque<u8> = VecDeque::new();
        data.extend(b"test".iter());
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    #[test]
    fn test_vec_deque_u8_pack_with_wrap() {
        // Create a VecDeque that wraps around
        let mut data: VecDeque<u8> = VecDeque::with_capacity(4);
        data.push_back(b'a');
        data.push_back(b'b');
        data.push_back(b'c');
        data.push_back(b'd');
        data.pop_front();
        data.pop_front();
        data.push_back(b'e');
        data.push_back(b'f');
        // Now data contains "cdef" but may be wrapped
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01cdef\x00");
    }

    #[test]
    fn test_vec_deque_u8_pack_empty() {
        let data: VecDeque<u8> = VecDeque::new();
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01\x00");
    }

    #[test]
    fn test_vec_deque_u8_pack_with_nulls() {
        let mut data: VecDeque<u8> = VecDeque::new();
        data.extend(b"a\x00b".iter());
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01a\x00\xffb\x00");
    }

    // Tests for str and String
    #[test]
    fn test_str_pack() {
        let data: &str = "hello";
        let packed = pack_to_vec(data);
        assert_eq!(packed, b"\x02hello\x00");
    }

    #[test]
    fn test_str_pack_empty() {
        let data: &str = "";
        let packed = pack_to_vec(data);
        assert_eq!(packed, b"\x02\x00");
    }

    #[test]
    fn test_str_pack_with_null() {
        let data: &str = "hel\x00lo";
        let packed = pack_to_vec(data);
        assert_eq!(packed, b"\x02hel\x00\xfflo\x00");
    }

    #[test]
    fn test_str_pack_unicode() {
        let data: &str = "日本語";
        let packed = pack_to_vec(data);
        let mut expected = vec![0x02];
        expected.extend_from_slice("日本語".as_bytes());
        expected.push(0x00);
        assert_eq!(packed, expected);
    }

    #[test]
    fn test_string_pack() {
        let data: String = "hello".to_string();
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x02hello\x00");
    }

    // Tests for tuples
    #[test]
    fn test_tuple_1() {
        let data = (42i32,);
        let packed = pack_to_vec(&data);
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
    }

    #[test]
    fn test_tuple_2() {
        let data = (1i32, 2i32);
        let packed = pack_to_vec(&data);
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
    }

    #[test]
    fn test_tuple_3() {
        let data = (1i32, 2i32, 3i32);
        let packed = pack_to_vec(&data);
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
    }

    #[test]
    fn test_tuple_mixed_types() {
        let data = ("hello", 42i32, true);
        let packed = pack_to_vec(&data);
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
    }

    #[test]
    fn test_tuple_with_nested_none() {
        let data = ((),);
        let packed = pack_to_vec(&data);
        // Should contain \x05 ... \x00\xff ... \x00
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
        // The nested () should be \x00\xff
        assert!(packed.windows(2).any(|w| w == b"\x00\xff"));
    }

    #[test]
    fn test_tuple_10_elements() {
        let data = (0i32, 1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32, 9i32);
        let packed = pack_to_vec(&data);
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
    }

    // Tests for signed integers
    #[test]
    fn test_i8_zero() {
        let packed = pack_to_vec(&0i8);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_i8_positive() {
        let packed = pack_to_vec(&1i8);
        assert_eq!(packed, b"\x15\x01");
    }

    #[test]
    fn test_i8_negative() {
        let packed = pack_to_vec(&-1i8);
        assert_eq!(packed, b"\x13\xfe");
    }

    #[test]
    fn test_i8_max() {
        let packed = pack_to_vec(&i8::MAX);
        assert_eq!(packed, b"\x15\x7f");
    }

    #[test]
    fn test_i8_min() {
        let packed = pack_to_vec(&i8::MIN);
        assert_eq!(packed, b"\x13\x7f");
    }

    #[test]
    fn test_i16_zero() {
        let packed = pack_to_vec(&0i16);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_i16_positive_small() {
        let packed = pack_to_vec(&255i16);
        assert_eq!(packed, b"\x15\xff");
    }

    #[test]
    fn test_i16_positive_large() {
        let packed = pack_to_vec(&256i16);
        assert_eq!(packed, b"\x16\x01\x00");
    }

    #[test]
    fn test_i16_negative() {
        let packed = pack_to_vec(&-256i16);
        assert_eq!(packed, b"\x12\xfe\xff");
    }

    #[test]
    fn test_i32_zero() {
        let packed = pack_to_vec(&0i32);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_i32_positive() {
        let packed = pack_to_vec(&0x01020304i32);
        assert_eq!(packed, b"\x18\x01\x02\x03\x04");
    }

    #[test]
    fn test_i32_negative() {
        let packed = pack_to_vec(&-0x01020304i32);
        assert_eq!(packed, b"\x10\xfe\xfd\xfc\xfb");
    }

    #[test]
    fn test_i64_zero() {
        let packed = pack_to_vec(&0i64);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_i64_positive_max() {
        let packed = pack_to_vec(&i64::MAX);
        assert_eq!(packed, b"\x1c\x7f\xff\xff\xff\xff\xff\xff\xff");
    }

    #[test]
    fn test_i64_negative_min() {
        let packed = pack_to_vec(&i64::MIN);
        // i64::MIN is -9223372036854775808, magnitude is 0x8000000000000000
        // One's complement: 0x7fffffffffffffff
        assert_eq!(packed, b"\x0c\x7f\xff\xff\xff\xff\xff\xff\xff");
    }

    // Tests for unsigned integers
    #[test]
    fn test_u8_zero() {
        let packed = pack_to_vec(&0u8);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_u8_positive() {
        let packed = pack_to_vec(&1u8);
        assert_eq!(packed, b"\x15\x01");
    }

    #[test]
    fn test_u8_max() {
        let packed = pack_to_vec(&u8::MAX);
        assert_eq!(packed, b"\x15\xff");
    }

    #[test]
    fn test_u16_zero() {
        let packed = pack_to_vec(&0u16);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_u16_positive() {
        let packed = pack_to_vec(&256u16);
        assert_eq!(packed, b"\x16\x01\x00");
    }

    #[test]
    fn test_u16_max() {
        let packed = pack_to_vec(&u16::MAX);
        assert_eq!(packed, b"\x16\xff\xff");
    }

    #[test]
    fn test_u32_zero() {
        let packed = pack_to_vec(&0u32);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_u32_positive() {
        let packed = pack_to_vec(&0x01020304u32);
        assert_eq!(packed, b"\x18\x01\x02\x03\x04");
    }

    #[test]
    fn test_u32_max() {
        let packed = pack_to_vec(&u32::MAX);
        assert_eq!(packed, b"\x18\xff\xff\xff\xff");
    }

    #[test]
    fn test_u64_zero() {
        let packed = pack_to_vec(&0u64);
        assert_eq!(packed, b"\x14");
    }

    #[test]
    fn test_u64_positive() {
        let packed = pack_to_vec(&0x0102030405060708u64);
        assert_eq!(packed, b"\x1c\x01\x02\x03\x04\x05\x06\x07\x08");
    }

    #[test]
    fn test_u64_max() {
        let packed = pack_to_vec(&u64::MAX);
        assert_eq!(packed, b"\x1c\xff\xff\xff\xff\xff\xff\xff\xff");
    }

    // Tests for bool
    #[test]
    fn test_bool_false() {
        let packed = pack_to_vec(&false);
        assert_eq!(packed, b"\x26");
    }

    #[test]
    fn test_bool_true() {
        let packed = pack_to_vec(&true);
        assert_eq!(packed, b"\x27");
    }

    // Tests for UUID
    #[test]
    fn test_uuid_pack() {
        let uuid = ::uuid::Uuid::from_bytes([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]);
        let packed = pack_to_vec(&uuid);
        assert_eq!(
            packed,
            b"\x30\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
        );
    }

    #[test]
    fn test_uuid_nil() {
        let uuid = ::uuid::Uuid::nil();
        let packed = pack_to_vec(&uuid);
        assert_eq!(
            packed,
            b"\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
    }

    // Tests for bytes::Bytes and bytes::BytesMut
    #[test]
    fn test_bytes_bytes_pack() {
        let data = ::bytes::Bytes::from_static(b"test");
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    #[test]
    fn test_bytes_bytes_mut_pack() {
        let data = ::bytes::BytesMut::from(b"test" as &[u8]);
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    // Tests for SmallVec
    #[test]
    fn test_smallvec_pack() {
        let mut data: SmallVec<[u8; 8]> = SmallVec::new();
        data.extend_from_slice(b"test");
        let packed = pack_to_vec(&data);
        assert_eq!(packed, b"\x01test\x00");
    }

    // Tests for pack_to_vec_deque
    #[test]
    fn test_pack_to_vec_deque() {
        let value = 42i32;
        let packed = value.pack_to_vec_deque();
        assert_eq!(packed, VecDeque::from(vec![0x15, 0x2a]));
    }

    // Tests for pack_to_small_vec
    #[test]
    fn test_pack_to_small_vec() {
        let value = 42i32;
        let packed: SmallVec<[u8; 8]> = value.pack_to_small_vec();
        assert_eq!(packed.as_slice(), &[0x15, 0x2a]);
    }

    // Tests for pack_prefix_range
    #[test]
    fn test_pack_prefix_range() {
        let value = "test";
        let mut start = Vec::new();
        let mut end = Vec::new();
        value.pack_prefix_range(&mut start, &mut end).unwrap();
        assert!(start.ends_with(b"\x00"));
        assert!(end.ends_with(b"\xff"));
    }

    #[test]
    fn test_pack_prefix_range_to_vec_range() {
        let value = "test";
        let range = value.pack_prefix_range_to_vec_range();
        assert!(range.start.ends_with(b"\x00"));
        assert!(range.end.ends_with(b"\xff"));
    }

    #[test]
    fn test_pack_prefix_range_to_small_vec_range() {
        let value = "test";
        let range: Range<SmallVec<[u8; 16]>> = value.pack_prefix_range_to_small_vec_range();
        assert!(range.start.ends_with(b"\x00"));
        assert!(range.end.ends_with(b"\xff"));
    }

    // Integer ordering tests (ensure packed values maintain sort order)
    #[test]
    fn test_integer_ordering_positive() {
        let a = pack_to_vec(&1i64);
        let b = pack_to_vec(&2i64);
        let c = pack_to_vec(&255i64);
        let d = pack_to_vec(&256i64);
        assert!(a < b);
        assert!(b < c);
        assert!(c < d);
    }

    #[test]
    fn test_integer_ordering_negative() {
        let a = pack_to_vec(&-256i64);
        let b = pack_to_vec(&-255i64);
        let c = pack_to_vec(&-2i64);
        let d = pack_to_vec(&-1i64);
        assert!(a < b);
        assert!(b < c);
        assert!(c < d);
    }

    #[test]
    fn test_integer_ordering_across_zero() {
        let a = pack_to_vec(&-1i64);
        let b = pack_to_vec(&0i64);
        let c = pack_to_vec(&1i64);
        assert!(a < b);
        assert!(b < c);
    }

    // String ordering tests
    #[test]
    fn test_string_ordering() {
        let a = pack_to_vec("aaa");
        let b = pack_to_vec("aab");
        let c = pack_to_vec("ab");
        let d = pack_to_vec("b");
        assert!(a < b);
        assert!(b < c);
        assert!(c < d);
    }

    // Additional edge cases
    #[test]
    fn test_deeply_nested_tuple() {
        let data = ((1i32,),);
        let packed = pack_to_vec(&data);
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
    }

    #[test]
    fn test_tuple_with_strings_and_bytes() {
        let data = ("hello", b"world".to_vec());
        let packed = pack_to_vec(&data);
        assert!(packed.starts_with(b"\x05"));
        assert!(packed.ends_with(b"\x00"));
    }

    // Tests for writer failures
    struct FailingWriter {
        fail_after: usize,
        bytes_written: usize,
    }

    impl FailingWriter {
        fn new(fail_after: usize) -> Self {
            FailingWriter {
                fail_after,
                bytes_written: 0,
            }
        }
    }

    impl io::Write for FailingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.bytes_written >= self.fail_after {
                return Err(io::Error::new(io::ErrorKind::Other, "intentional failure"));
            }
            let can_write = (self.fail_after - self.bytes_written).min(buf.len());
            self.bytes_written += can_write;
            if can_write < buf.len() {
                Ok(can_write)
            } else {
                Ok(buf.len())
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct ImmediateFailWriter;

    impl io::Write for ImmediateFailWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Other, "immediate failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_pack_bytes_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let bytes = Bytes::new(b"hello");
        let result = bytes.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_str_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let result = "hello".pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_i64_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let result = 42i64.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_u64_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let result = 42u64.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_bool_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let result = true.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_unit_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let result = ().pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_uuid_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let uuid = ::uuid::Uuid::nil();
        let result = uuid.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_tuple_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let data = (1i32, 2i32);
        let result = data.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_vec_deque_writer_fails_immediately() {
        let mut writer = ImmediateFailWriter;
        let mut data: VecDeque<u8> = VecDeque::new();
        data.extend(b"test".iter());
        let result = data.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_bytes_writer_fails_midway() {
        let mut writer = FailingWriter::new(2);
        let bytes = Bytes::new(b"hello world");
        let result = bytes.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_str_writer_fails_midway() {
        let mut writer = FailingWriter::new(3);
        let result = "hello world".pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_i64_large_writer_fails_on_payload() {
        let mut writer = FailingWriter::new(1);
        let result = i64::MAX.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_tuple_writer_fails_on_second_element() {
        let mut writer = FailingWriter::new(3);
        let data = (1i32, "hello");
        let result = data.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_prefix_range_writer_fails() {
        let mut start = ImmediateFailWriter;
        let mut end = Vec::new();
        let result = "test".pack_prefix_range(&mut start, &mut end);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_prefix_range_end_writer_fails() {
        let mut start = Vec::new();
        let mut end = ImmediateFailWriter;
        let result = "test".pack_prefix_range(&mut start, &mut end);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_nested_unit_writer_fails() {
        let mut writer = ImmediateFailWriter;
        let result = ().pack_nested(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_option_some_writer_fails() {
        let mut writer = ImmediateFailWriter;
        let value: Option<i32> = Some(42);
        let result = value.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_option_none_writer_fails() {
        let mut writer = ImmediateFailWriter;
        let value: Option<i32> = None;
        let result = value.pack(&mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_bytes_with_null_writer_fails_on_escape() {
        // Fail after writing the type byte and "hel"
        let mut writer = FailingWriter::new(4);
        let bytes = Bytes::new(b"hel\x00lo");
        let result = bytes.pack(&mut writer);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::VecDeque;

    proptest! {
        // Property tests for signed integers
        #[test]
        fn prop_i8_pack_succeeds(x: i8) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        #[test]
        fn prop_i16_pack_succeeds(x: i16) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        #[test]
        fn prop_i32_pack_succeeds(x: i32) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        #[test]
        fn prop_i64_pack_succeeds(x: i64) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        // Property tests for unsigned integers
        #[test]
        fn prop_u8_pack_succeeds(x: u8) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        #[test]
        fn prop_u16_pack_succeeds(x: u16) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        #[test]
        fn prop_u32_pack_succeeds(x: u32) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        #[test]
        fn prop_u64_pack_succeeds(x: u64) {
            let packed = x.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        // Property test: i64 sort order preservation
        #[test]
        fn prop_i64_ordering(a: i64, b: i64) {
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: u64 sort order preservation
        #[test]
        fn prop_u64_ordering(a: u64, b: u64) {
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: i32 sort order preservation
        #[test]
        fn prop_i32_ordering(a: i32, b: i32) {
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: u32 sort order preservation
        #[test]
        fn prop_u32_ordering(a: u32, b: u32) {
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: strings
        #[test]
        fn prop_string_pack_succeeds(s: String) {
            let packed = s.pack_to_vec();
            prop_assert!(packed.starts_with(b"\x02"));
            prop_assert!(packed.ends_with(b"\x00"));
        }

        // Property test: string sort order preservation
        #[test]
        fn prop_string_ordering(a: String, b: String) {
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: bytes
        #[test]
        fn prop_bytes_pack_succeeds(data: Vec<u8>) {
            let packed = data.pack_to_vec();
            prop_assert!(packed.starts_with(b"\x01"));
            prop_assert!(packed.ends_with(b"\x00"));
        }

        // Property test: bytes sort order preservation
        #[test]
        fn prop_bytes_ordering(a: Vec<u8>, b: Vec<u8>) {
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: bool
        #[test]
        fn prop_bool_pack_succeeds(b: bool) {
            let packed = b.pack_to_vec();
            prop_assert_eq!(packed.len(), 1);
            match b {
                false => prop_assert_eq!(packed[0], 0x26),
                true => prop_assert_eq!(packed[0], 0x27),
            }
        }

        // Property test: bool ordering (false < true)
        #[test]
        fn prop_bool_ordering(a: bool, b: bool) {
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: Option<T>
        #[test]
        fn prop_option_i32_pack_succeeds(opt: Option<i32>) {
            let packed = opt.pack_to_vec();
            prop_assert!(!packed.is_empty());
        }

        // Property test: tuples
        #[test]
        fn prop_tuple_2_pack_succeeds(a: i32, b: i32) {
            let data = (a, b);
            let packed = data.pack_to_vec();
            prop_assert!(packed.starts_with(b"\x05"));
            prop_assert!(packed.ends_with(b"\x00"));
        }

        #[test]
        fn prop_tuple_3_pack_succeeds(a: i32, b: String, c: bool) {
            let data = (a, b, c);
            let packed = data.pack_to_vec();
            prop_assert!(packed.starts_with(b"\x05"));
            prop_assert!(packed.ends_with(b"\x00"));
        }

        // Property test: UUID
        #[test]
        fn prop_uuid_pack_succeeds(bytes: [u8; 16]) {
            let uuid = ::uuid::Uuid::from_bytes(bytes);
            let packed = uuid.pack_to_vec();
            prop_assert_eq!(packed.len(), 17);
            prop_assert_eq!(packed[0], 0x30);
            prop_assert_eq!(&packed[1..], &bytes[..]);
        }

        // Property test: UUID ordering
        #[test]
        fn prop_uuid_ordering(a_bytes: [u8; 16], b_bytes: [u8; 16]) {
            let a = ::uuid::Uuid::from_bytes(a_bytes);
            let b = ::uuid::Uuid::from_bytes(b_bytes);
            let packed_a = a.pack_to_vec();
            let packed_b = b.pack_to_vec();
            prop_assert_eq!(a.cmp(&b), packed_a.cmp(&packed_b));
        }

        // Property test: VecDeque<u8>
        #[test]
        fn prop_vec_deque_pack_succeeds(data: Vec<u8>) {
            let deque: VecDeque<u8> = data.iter().copied().collect();
            let packed = deque.pack_to_vec();
            prop_assert!(packed.starts_with(b"\x01"));
            prop_assert!(packed.ends_with(b"\x00"));
        }

        // Property test: VecDeque equals Vec packing
        #[test]
        fn prop_vec_deque_equals_vec(data: Vec<u8>) {
            let deque: VecDeque<u8> = data.iter().copied().collect();
            let packed_vec = data.pack_to_vec();
            let packed_deque = deque.pack_to_vec();
            prop_assert_eq!(packed_vec, packed_deque);
        }

        // Property test: SmallVec
        #[test]
        fn prop_smallvec_pack_succeeds(data: Vec<u8>) {
            let sv: SmallVec<[u8; 32]> = SmallVec::from_vec(data.clone());
            let packed = sv.pack_to_vec();
            prop_assert_eq!(packed, data.pack_to_vec());
        }

        // Property test: pack_prefix_range produces valid range
        #[test]
        fn prop_prefix_range_valid(s: String) {
            let range = s.pack_prefix_range_to_vec_range();
            prop_assert!(range.start < range.end);
            // Start should be packed string + \x00
            // End should be packed string + \xff
            let packed = s.pack_to_vec();
            let mut expected_start = packed.clone();
            expected_start.push(0x00);
            let mut expected_end = packed;
            expected_end.push(0xff);
            prop_assert_eq!(range.start, expected_start);
            prop_assert_eq!(range.end, expected_end);
        }

        // Property test: reference packing equals value packing
        #[test]
        fn prop_reference_pack_equals_value(x: i64) {
            let packed_value = x.pack_to_vec();
            let packed_ref = (&x).pack_to_vec();
            prop_assert_eq!(packed_value, packed_ref);
        }

        // Property test: pack_to_vec_deque equals pack_to_vec
        #[test]
        fn prop_pack_to_vec_deque_equals_vec(x: i64) {
            let packed_vec = x.pack_to_vec();
            let packed_deque = x.pack_to_vec_deque();
            let deque_vec: Vec<u8> = packed_deque.into_iter().collect();
            prop_assert_eq!(packed_vec, deque_vec);
        }

        // Property test: pack_to_small_vec equals pack_to_vec
        #[test]
        fn prop_pack_to_small_vec_equals_vec(x: i64) {
            let packed_vec = x.pack_to_vec();
            let packed_small: SmallVec<[u8; 16]> = x.pack_to_small_vec();
            prop_assert_eq!(packed_vec.as_slice(), packed_small.as_slice());
        }

        // Property test: bytes::Bytes equals Vec<u8> packing
        #[test]
        fn prop_bytes_bytes_equals_vec(data: Vec<u8>) {
            let bytes = ::bytes::Bytes::from(data.clone());
            let packed_bytes = bytes.pack_to_vec();
            let packed_vec = data.pack_to_vec();
            prop_assert_eq!(packed_bytes, packed_vec);
        }

        // Property test: bytes::BytesMut equals Vec<u8> packing
        #[test]
        fn prop_bytes_bytes_mut_equals_vec(data: Vec<u8>) {
            let bytes_mut = ::bytes::BytesMut::from(data.as_slice());
            let packed_bytes_mut = bytes_mut.pack_to_vec();
            let packed_vec = data.pack_to_vec();
            prop_assert_eq!(packed_bytes_mut, packed_vec);
        }

        // Property test: Box<[u8]> equals Vec<u8> packing
        #[test]
        fn prop_boxed_slice_equals_vec(data: Vec<u8>) {
            let boxed: Box<[u8]> = data.clone().into_boxed_slice();
            let packed_boxed = boxed.pack_to_vec();
            let packed_vec = data.pack_to_vec();
            prop_assert_eq!(packed_boxed, packed_vec);
        }

        // Property test: Arc<[u8]> equals Vec<u8> packing
        #[test]
        fn prop_arc_slice_equals_vec(data: Vec<u8>) {
            let arc: Arc<[u8]> = Arc::from(data.as_slice());
            let packed_arc = arc.pack_to_vec();
            let packed_vec = data.pack_to_vec();
            prop_assert_eq!(packed_arc, packed_vec);
        }

        // Property test: Rc<[u8]> equals Vec<u8> packing
        #[test]
        fn prop_rc_slice_equals_vec(data: Vec<u8>) {
            let rc: Rc<[u8]> = Rc::from(data.as_slice());
            let packed_rc = rc.pack_to_vec();
            let packed_vec = data.pack_to_vec();
            prop_assert_eq!(packed_rc, packed_vec);
        }

        // Property test: array equals slice packing
        #[test]
        fn prop_array_equals_slice(data: [u8; 8]) {
            let slice: &[u8] = &data;
            let packed_array = data.pack_to_vec();
            let packed_slice = slice.pack_to_vec();
            prop_assert_eq!(packed_array, packed_slice);
        }

        // Property test: String equals str packing
        #[test]
        fn prop_string_equals_str(s: String) {
            let str_ref: &str = &s;
            let packed_string = s.pack_to_vec();
            let packed_str = str_ref.pack_to_vec();
            prop_assert_eq!(packed_string, packed_str);
        }

        // Property test: zero always produces \x14
        #[test]
        fn prop_zero_always_0x14(x in 0i64..=0i64) {
            let packed = x.pack_to_vec();
            prop_assert_eq!(packed, vec![0x14]);
        }

        // Property test: positive integers have type code > 0x14
        #[test]
        fn prop_positive_type_code(x in 1i64..=i64::MAX) {
            let packed = x.pack_to_vec();
            prop_assert!(packed[0] > 0x14);
            prop_assert!(packed[0] <= 0x1c);
        }

        // Property test: negative integers have type code < 0x14
        #[test]
        fn prop_negative_type_code(x in i64::MIN..=-1i64) {
            let packed = x.pack_to_vec();
            prop_assert!(packed[0] < 0x14);
            prop_assert!(packed[0] >= 0x0c);
        }

        // Property test: tuple ordering for 2-tuples (lexicographic)
        #[test]
        fn prop_tuple_2_ordering((a1, a2) in (any::<i32>(), any::<i32>()), (b1, b2) in (any::<i32>(), any::<i32>())) {
            let tuple_a = (a1, a2);
            let tuple_b = (b1, b2);
            let packed_a = tuple_a.pack_to_vec();
            let packed_b = tuple_b.pack_to_vec();
            prop_assert_eq!(tuple_a.cmp(&tuple_b), packed_a.cmp(&packed_b));
        }

        // Property test: escaping null bytes in strings
        #[test]
        fn prop_string_with_nulls_escaping(parts: Vec<String>) {
            let s = parts.join("\x00");
            let packed = s.pack_to_vec();
            // Check that the packed form starts with \x02 and ends with \x00
            prop_assert!(packed.starts_with(b"\x02"));
            prop_assert!(packed.ends_with(b"\x00"));
            // Count null bytes in the middle - they should all be escaped
            let middle = &packed[1..packed.len()-1];
            // Every \x00 in middle should be followed by \xff
            for i in 0..middle.len() {
                if middle[i] == 0x00 {
                    prop_assert!(i + 1 < middle.len() && middle[i + 1] == 0xff,
                        "Null byte at position {} not properly escaped", i);
                }
            }
        }

        // Property test: escaping null bytes in byte arrays
        #[test]
        fn prop_bytes_with_nulls_escaping(data: Vec<u8>) {
            let packed = data.pack_to_vec();
            // Check that the packed form starts with \x01 and ends with \x00
            prop_assert!(packed.starts_with(b"\x01"));
            prop_assert!(packed.ends_with(b"\x00"));
            // Count null bytes in the middle - they should all be escaped
            let middle = &packed[1..packed.len()-1];
            // Every \x00 in middle should be followed by \xff
            for i in 0..middle.len() {
                if middle[i] == 0x00 {
                    prop_assert!(i + 1 < middle.len() && middle[i + 1] == 0xff,
                        "Null byte at position {} not properly escaped", i);
                }
            }
        }
    }
}
