use smallvec::SmallVec;
use std::{io, num::NonZeroU32};
use thiserror::Error;

#[derive(Clone, Copy)]
pub struct Xor {
    num_data_shards: NonZeroU32,
}

impl Xor {
    pub const fn new(n: NonZeroU32) -> Option<Self> {
        if n.get() > 1 {
            Some(Xor { num_data_shards: n })
        } else {
            None
        }
    }

    pub fn encode<'a>(&self, data: &'a [u8]) -> Result<Encoded<'a>, EncodeError> {
        Encoded::new(data, self.num_data_shards)
    }

    pub fn decoder<'a>(&self) -> Decoder<'a> {
        Decoder::new(self.num_data_shards)
    }
}

#[derive(Clone)]
pub struct Encoded<'a> {
    data: &'a [u8],
    num_data_shards: NonZeroU32,
    shard_len: usize,
    last_shard_len: usize,
}

impl<'a> Encoded<'a> {
    pub fn new(data: &'a [u8], n: NonZeroU32) -> Result<Self, EncodeError> {
        if n.get() < 2 {
            return Err(EncodeError::InvalidShardConfiguration {
                data_shards: n.get(),
            });
        }

        if data.len() < (n.get() as usize) {
            return Err(EncodeError::NotEnoughData {
                min_expected: n.get() as usize,
                actual: data.len(),
            });
        }

        let shard_len = data
            .len()
            .checked_add(n.get() as usize - 1)
            .and_then(|v| v.checked_div(n.get() as usize))
            .unwrap();

        let last_shard_len = data.len() - (shard_len * (n.get() as usize - 1));

        Ok(Encoded {
            data,
            num_data_shards: n,
            shard_len,
            last_shard_len,
        })
    }

    pub fn get_data_shard_bytes(&self, idx: u32) -> Option<&'a [u8]> {
        if idx >= self.num_data_shards.get() {
            return None;
        }

        let idx = idx as usize;
        let start = idx * self.shard_len;

        let end = if idx == (self.num_data_shards.get() as usize - 1) {
            start + self.last_shard_len
        } else {
            start + self.shard_len
        };

        Some(&self.data[start..end])
    }

    pub fn get_data_shard_size(&self, idx: u32) -> Option<usize> {
        if idx >= self.num_data_shards.get() {
            return None;
        }

        if idx == self.num_data_shards.get() - 1 {
            Some(self.last_shard_len)
        } else {
            Some(self.shard_len)
        }
    }

    pub fn data_shard_size_hint(&self) -> (usize, usize) {
        (self.last_shard_len, self.shard_len)
    }

    pub fn data_shard_iter(&self) -> impl Iterator<Item = (u32, &'a [u8])> {
        (0..self.num_data_shards.get()).map(|idx| (idx, self.get_data_shard_bytes(idx).unwrap()))
    }

    pub fn write_data_shard_to<W: io::Write>(
        &self,
        idx: u32,
        w: &mut W,
    ) -> Result<(), EncodeError> {
        if let Some(data) = self.get_data_shard_bytes(idx) {
            w.write_all(data)?;

            Ok(())
        } else {
            Err(EncodeError::InvalidDataShardIndex {
                idx,
                data_shards: self.num_data_shards.get(),
            })
        }
    }

    pub fn copy_data_shard_to_slice(&self, idx: u32, dst: &mut [u8]) {
        self.try_copy_data_shard_to_slice(idx, dst).unwrap()
    }

    pub fn try_copy_data_shard_to_slice(
        &self,
        idx: u32,
        dst: &mut [u8],
    ) -> Result<(), EncodeError> {
        if let Some(data) = self.get_data_shard_bytes(idx) {
            // Validate buffer size
            if dst.len() != data.len() {
                return Err(EncodeError::InvalidBufferSize {
                    expected: data.len(),
                    actual: dst.len(),
                });
            }

            // Zero the buffer before XORing
            dst.fill(0);

            dst[0..data.len()].copy_from_slice(data);

            Ok(())
        } else {
            Err(EncodeError::InvalidDataShardIndex {
                idx,
                data_shards: self.num_data_shards.get(),
            })
        }
    }

    pub fn parity_shard_size(&self) -> usize {
        self.shard_len
    }

    pub fn write_parity_shard_to<W: io::Write>(&self, w: &mut W) -> Result<(), EncodeError> {
        let mut buf = vec![0u8; self.shard_len];

        do_xor(
            buf.as_mut_slice(),
            self.data_shard_iter().map(|(_, bytes)| bytes),
        );

        w.write_all(buf.as_ref())?;

        Ok(())
    }

    pub fn copy_parity_shard_to_slice(&self, dst: &mut [u8]) {
        self.try_copy_parity_shard_to_slice(dst).unwrap()
    }

    pub fn try_copy_parity_shard_to_slice(&self, dst: &mut [u8]) -> Result<(), EncodeError> {
        // Validate buffer size
        if dst.len() != self.shard_len {
            return Err(EncodeError::InvalidBufferSize {
                expected: self.shard_len,
                actual: dst.len(),
            });
        }

        // Zero the buffer before XORing
        dst.fill(0);

        // XOR all data shards into the caller-provided buffer
        do_xor(dst, self.data_shard_iter().map(|(_, bytes)| bytes));

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("XOR parity is undefined for {data_shards} data shard(s); at least 2 are required")]
    InvalidShardConfiguration { data_shards: u32 },

    #[error("not enough data: expected at least {min_expected} bytes, got {actual}")]
    NotEnoughData { min_expected: usize, actual: usize },

    #[error("data shard index {idx} is invalid; encoder exposes {data_shards} data shards")]
    InvalidDataShardIndex { idx: u32, data_shards: u32 },

    #[error("buffer has incorrect size: expected {expected}, got {actual}")]
    InvalidBufferSize { expected: usize, actual: usize },

    #[error("i/o")]
    IoError(#[from] io::Error),
}

#[derive(Clone)]
pub struct Decoder<'a> {
    data_shards: SmallVec<[Option<&'a [u8]>; 3]>,
    parity_shard: Option<&'a [u8]>,
    max_num_data_shards: NonZeroU32,
    max_shard_len: Option<usize>,
    last_shard_len: Option<usize>,
}

impl<'a> Decoder<'a> {
    pub fn new(n: NonZeroU32) -> Decoder<'a> {
        let mut data_shards = SmallVec::new_const();
        data_shards.resize(n.get() as usize, None);

        Decoder {
            data_shards: data_shards,
            parity_shard: None,
            max_num_data_shards: n,
            max_shard_len: None,
            last_shard_len: None,
        }
    }

    pub fn add_data_shard(
        &mut self,
        idx: u32,
        shard_bytes: &'a [u8],
    ) -> Result<&mut Self, DecodeError> {
        let max = self.max_num_data_shards.get();

        if idx >= max {
            return Err(DecodeError::DataShardIndexOutOfRange { idx, max });
        }

        if self.data_shards[idx as usize].is_some() {
            return Err(DecodeError::DuplicateDataShard { idx });
        }

        // Track last shard length
        if idx == max - 1 {
            self.last_shard_len = Some(shard_bytes.len());
        }

        // Track max shard length (for all but last)
        self.max_shard_len = Some(self.max_shard_len.unwrap_or(0).max(shard_bytes.len()));

        self.data_shards[idx as usize] = Some(shard_bytes);

        Ok(self)
    }

    pub fn add_parity_shard(&mut self, shard_bytes: &'a [u8]) -> Result<&mut Self, DecodeError> {
        if self.parity_shard.is_some() {
            return Err(DecodeError::DuplicateParityShard);
        }

        self.parity_shard = Some(shard_bytes);
        self.max_shard_len = Some(self.max_shard_len.unwrap_or(0).max(shard_bytes.len()));

        Ok(self)
    }

    pub fn decode(self) -> Result<Decoded<'a>, DecodeError> {
        let num_required_data_shards = if self.parity_shard.is_none() {
            self.max_num_data_shards.get() as usize
        } else {
            self.max_num_data_shards.get() as usize - 1
        };

        let actual = self.data_shards.iter().filter(|v| v.is_some()).count();
        if actual != num_required_data_shards {
            return Err(DecodeError::NotEnoughShards {
                expected: num_required_data_shards,
                actual,
            });
        }

        let max_shard_len = self
            .max_shard_len
            .ok_or_else(|| DecodeError::InvalidShardSize {
                idx: 0,
                expected: 0,
                actual: 0,
            })?;

        let last_shard_len =
            self.last_shard_len
                .ok_or_else(|| DecodeError::InvalidLastShardSize {
                    expected: 0,
                    actual: 0,
                })?;

        let max = self.max_num_data_shards.get();

        for (i, shard) in self.data_shards.iter().enumerate().take(max as usize - 1) {
            if let Some(shard) = shard {
                if shard.len() != max_shard_len {
                    return Err(DecodeError::InvalidShardSize {
                        idx: i as u32,
                        expected: max_shard_len,
                        actual: shard.len(),
                    });
                }
            }
        }

        if let Some(last) = self.data_shards.last().unwrap() {
            if last.len() != last_shard_len {
                return Err(DecodeError::InvalidLastShardSize {
                    expected: last_shard_len,
                    actual: last.len(),
                });
            }
        }

        if let Some(parity) = self.parity_shard {
            if parity.len() != max_shard_len {
                return Err(DecodeError::InvalidParityShardSize {
                    expected: max_shard_len,
                    actual: parity.len(),
                });
            }

            Ok(Decoded {
                inner: DecodedInner::Degraded {
                    data_shards: self.data_shards,
                    parity_shard: parity,
                },
                max_num_data_shards: self.max_num_data_shards,
                shard_len: max_shard_len,
                last_shard_len,
            })
        } else {
            Ok(Decoded {
                inner: DecodedInner::Healthy {
                    data_shards: self.data_shards.iter().map(|v| v.unwrap()).collect(),
                },
                max_num_data_shards: self.max_num_data_shards,
                shard_len: max_shard_len,
                last_shard_len,
            })
        }
    }
}

#[derive(Clone)]
pub struct Decoded<'a> {
    inner: DecodedInner<'a>,
    max_num_data_shards: NonZeroU32,
    shard_len: usize,
    last_shard_len: usize,
}

#[derive(Clone)]
enum DecodedInner<'a> {
    Healthy {
        data_shards: SmallVec<[&'a [u8]; 3]>,
    },
    Degraded {
        data_shards: SmallVec<[Option<&'a [u8]>; 3]>,
        parity_shard: &'a [u8],
    },
}

impl<'a> Decoded<'a> {
    pub fn stripe_size(&self) -> usize {
        (self.max_num_data_shards.get() as usize - 1) * self.shard_len + self.last_shard_len
    }

    pub fn write_stripe_to<W: io::Write>(&self, w: &mut W) -> Result<(), DecodeError> {
        match &self.inner {
            DecodedInner::Degraded {
                data_shards,
                parity_shard,
            } => {
                for (i, data_shard) in data_shards.iter().enumerate() {
                    if let Some(data_shard) = data_shard {
                        w.write_all(data_shard)?;
                    } else {
                        let mut buf = vec![0u8; self.shard_len];

                        do_xor(
                            buf.as_mut_slice(),
                            data_shards
                                .iter()
                                .flat_map(|v| v)
                                .map(|v| *v)
                                .chain(Some(*parity_shard)),
                        );

                        let shard_len = if i == (self.max_num_data_shards.get() as usize - 1) {
                            self.last_shard_len
                        } else {
                            self.shard_len
                        };

                        w.write_all(&buf[..shard_len])?;
                    }
                }
            }
            DecodedInner::Healthy { data_shards } => {
                for data_shard in data_shards
                    .iter()
                    .take(self.max_num_data_shards.get() as usize - 1)
                {
                    w.write_all(data_shard)?;
                }

                w.write_all(&data_shards.last().unwrap()[..self.last_shard_len])?;
            }
        }

        Ok(())
    }

    pub fn copy_stripe_to_slice(&self, dst: &mut [u8]) {
        self.try_copy_stripe_to_slice(dst).unwrap()
    }

    pub fn try_copy_stripe_to_slice(&self, dst: &mut [u8]) -> Result<(), EncodeError> {
        // Validate buffer size
        let expected = self.stripe_size();

        if dst.len() != expected {
            return Err(EncodeError::InvalidBufferSize {
                expected: expected,
                actual: dst.len(),
            });
        }

        // Zero the buffer before XORing
        dst.fill(0);

        match &self.inner {
            DecodedInner::Degraded {
                data_shards,
                parity_shard,
            } => {
                for (i, data_shard) in data_shards.iter().enumerate() {
                    let start = i * self.shard_len;
                    let end = if i == (self.max_num_data_shards.get() as usize - 1) {
                        start + self.last_shard_len
                    } else {
                        start + self.shard_len
                    };

                    if let Some(data_shard) = data_shard {
                        dst[start..end].copy_from_slice(data_shard);
                    } else {
                        do_xor(
                            &mut dst[start..end],
                            data_shards
                                .iter()
                                .flat_map(|v| v)
                                .map(|v| *v)
                                .chain(Some(*parity_shard)),
                        );
                    }
                }
            }
            DecodedInner::Healthy { data_shards } => {
                for (i, shard) in data_shards.iter().enumerate() {
                    let start = i * self.shard_len;
                    let end = if i == (self.max_num_data_shards.get() as usize - 1) {
                        start + self.last_shard_len
                    } else {
                        start + self.shard_len
                    };
                    dst[start..end].copy_from_slice(&shard[..end - start]);
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("data shard index {idx} is out of range; decoder expects {max} shards")]
    DataShardIndexOutOfRange { idx: u32, max: u32 },

    #[error("data shard {idx} was provided more than once")]
    DuplicateDataShard { idx: u32 },

    #[error("parity shard was provided more than once")]
    DuplicateParityShard,

    #[error("not enough shards to reconstruct: expected {expected}, got {actual}")]
    NotEnoughShards { expected: usize, actual: usize },

    #[error("shard {idx} has incorrect size: expected {expected}, got {actual}")]
    InvalidShardSize {
        idx: u32,
        expected: usize,
        actual: usize,
    },

    #[error("last shard has incorrect size: expected {expected}, got {actual}")]
    InvalidLastShardSize { expected: usize, actual: usize },

    #[error("parity shard has incorrect size: expected {expected}, got {actual}")]
    InvalidParityShardSize { expected: usize, actual: usize },

    #[error("i/o")]
    IoError(#[from] io::Error),
}

pub fn do_xor<'a, 'b, 'c: 'a>(dst: &'b mut [u8], iter: impl Iterator<Item = &'c [u8]> + 'a) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe {
                return do_xor_avx2(dst, iter);
            }
        }
        if std::arch::is_x86_feature_detected!("sse2") {
            unsafe {
                return do_xor_sse2(dst, iter);
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                return do_xor_neon(dst, iter);
            }
        }
    }

    // scalar fallback
    do_xor_scalar(dst, iter)
}

fn do_xor_scalar<'a, 'b, 'c: 'a>(dst: &'b mut [u8], iter: impl Iterator<Item = &'c [u8]> + 'a) {
    for src in iter {
        let len = dst.len().min(src.len());
        for i in 0..len {
            dst[i] ^= src[i];
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn do_xor_avx2<'a, 'b, 'c: 'a>(
    dst: &'b mut [u8],
    iter: impl Iterator<Item = &'c [u8]> + 'a,
) {
    use core::arch::x86_64::*;

    for src in iter {
        let len = dst.len().min(src.len());
        let mut j = 0;

        let ps = src.as_ptr();
        let pd = dst.as_mut_ptr();

        let align = (32 - (ps as usize & 31)) & 31;
        while j < align && j < len {
            *pd.add(j) ^= *ps.add(j);
            j += 1;
        }

        while j + 32 <= len {
            let v = _mm256_load_si256(ps.add(j) as *const __m256i);
            let r = _mm256_load_si256(pd.add(j) as *const __m256i);
            let out = _mm256_xor_si256(r, v);
            _mm256_store_si256(pd.add(j) as *mut __m256i, out);
            j += 32;
        }

        while j < len {
            *pd.add(j) ^= *ps.add(j);
            j += 1;
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn do_xor_sse2<'a, 'b, 'c: 'a>(
    dst: &'b mut [u8],
    iter: impl Iterator<Item = &'c [u8]> + 'a,
) {
    use core::arch::x86_64::*;

    for src in iter {
        let len = dst.len().min(src.len());
        let mut j = 0;

        let ps = src.as_ptr();
        let pd = dst.as_mut_ptr();

        let align = (16 - (ps as usize & 15)) & 15;
        while j < align && j < len {
            *pd.add(j) ^= *ps.add(j);
            j += 1;
        }

        while j + 16 <= len {
            let v = _mm_load_si128(ps.add(j) as *const __m128i);
            let r = _mm_load_si128(pd.add(j) as *const __m128i);
            let out = _mm_xor_si128(r, v);
            _mm_store_si128(pd.add(j) as *mut __m128i, out);
            j += 16;
        }

        while j < len {
            *pd.add(j) ^= *ps.add(j);
            j += 1;
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn do_xor_neon<'a, 'b, 'c: 'a>(
    dst: &'b mut [u8],
    iter: impl Iterator<Item = &'c [u8]> + 'a,
) {
    use core::arch::aarch64::*;

    for src in iter {
        let len = dst.len().min(src.len());
        let mut j = 0;

        let ps = src.as_ptr();
        let pd = dst.as_mut_ptr();

        let align = (16 - (ps as usize & 15)) & 15;
        while j < align && j < len {
            *pd.add(j) ^= *ps.add(j);
            j += 1;
        }

        while j + 16 <= len {
            let v = vld1q_u8(ps.add(j));
            let r = vld1q_u8(pd.add(j));
            let out = veorq_u8(r, v);
            vst1q_u8(pd.add(j), out);
            j += 16;
        }

        while j < len {
            *pd.add(j) ^= *ps.add(j);
            j += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Role;
    use proptest::prelude::*;

    #[test]
    fn encode_produces_n_data_chunks_and_parity() {
        let xor = Xor::new(NonZeroU32::new(4).unwrap()).unwrap();
        let input = b"abcdefghijklmnopqrstuvwxyz";

        let out = xor.encode(input).unwrap();
        let vec: Vec<_> = out
            .data_shard_iter()
            .map(|(idx, _)| Role::Data(idx))
            .chain(Some(Role::Parity(0)))
            .collect();

        assert_eq!(vec.len(), 5); // 4 data + 1 parity
        assert!(vec.iter().any(|rd| matches!(rd, Role::Parity(_))));
    }

    #[test]
    fn round_trip_no_missing() {
        let xor = Xor::new(NonZeroU32::new(3).unwrap()).unwrap();
        let input = b"hello world, this is a test";

        let out = xor.encode(input).unwrap();

        let mut decoder = xor.decoder();
        out.data_shard_iter().for_each(|(idx, shard_bytes)| {
            decoder.add_data_shard(idx, shard_bytes).unwrap();
        });

        let decoded = decoder.decode().unwrap();

        let mut decoded_vec = Vec::new();
        decoded.write_stripe_to(&mut decoded_vec).unwrap();

        assert_eq!(decoded_vec, input);
    }

    #[test]
    fn round_trip_with_missing_data() {
        let xor = Xor::new(NonZeroU32::new(3).unwrap()).unwrap();
        let input = b"some moderately sized buffer";

        let out = xor.encode(input).unwrap();
        let mut data_vec: Vec<_> = out.data_shard_iter().collect();
        let mut parity_vec = Vec::new();
        out.write_parity_shard_to(&mut parity_vec).unwrap();

        // remove one data block
        data_vec.retain(|rd| !matches!(rd.0, 1));

        let mut decoder = xor.decoder();
        data_vec.iter().for_each(|(idx, shard_bytes)| {
            decoder.add_data_shard(*idx, shard_bytes).unwrap();
        });
        decoder.add_parity_shard(parity_vec.as_ref()).unwrap();
        let decoded = decoder.decode().unwrap();

        let mut decoded_vec = Vec::new();
        decoded.write_stripe_to(&mut decoded_vec).unwrap();

        assert_eq!(decoded_vec, input);
    }

    proptest! {
        #[test]
        fn prop_round_trip_random_input(input in proptest::collection::vec(any::<u8>(), 1..10_000)) {
            let n = 3u32;
            let xor = Xor::new(NonZeroU32::new(n).unwrap()).unwrap();

            // skip too-small inputs (encode rejects them)
            if input.len() < n as usize {
                return Ok(());
            }

            let out = xor.encode(&input).unwrap();
            let mut data_vec: Vec<_> = out.data_shard_iter().collect();
            let mut parity_vec = Vec::new();
            out.write_parity_shard_to(&mut parity_vec).unwrap();

            // randomly remove one data block (simulate erasure)
            let missing = 0usize;
            data_vec.retain(|rd| !matches!(rd.0, x if x == missing as u32));

            let mut decoder = xor.decoder();
            data_vec.iter().for_each(|(idx, shard_bytes)| {
                decoder.add_data_shard(*idx, shard_bytes).unwrap();
            });
            decoder.add_parity_shard(parity_vec.as_ref()).unwrap();
            let decoded = decoder.decode().unwrap();

            let mut decoded_vec = Vec::new();
            decoded.write_stripe_to(&mut decoded_vec).unwrap();

            prop_assert_eq!(&decoded_vec, &input);
        }
    }

    #[test]
    fn simd_matches_scalar() {
        for size in [1, 7, 32, 64, 1000, 4096] {
            let input: Vec<u8> = (0..size).map(|i| (i * 31 % 251) as u8).collect();

            // scalar
            let mut dst_scalar = vec![0u8; (size + 3) / 4];
            do_xor_scalar(&mut dst_scalar, std::iter::once(input.as_slice()));

            // simd-dispatched
            let mut dst_simd = vec![0u8; (size + 3) / 4];
            do_xor(&mut dst_simd, std::iter::once(input.as_slice()));

            assert_eq!(dst_scalar, dst_simd);
        }
    }
}
