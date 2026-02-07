use super::bitset::{BitSet, BitSetMut};
use super::word::Word;
use std::mem::MaybeUninit;

#[repr(transparent)]
pub struct BitArray<T: Word, const N: usize> {
    inner: [T; N],
}

impl<T: Word, const N: usize> BitArray<T, N> {
    pub const fn zeroed() -> Self {
        Self {
            inner: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }

    pub const fn uninit() -> MaybeUninit<BitArray<T, N>> {
        MaybeUninit::uninit()
    }

    pub const fn into_inner(self) -> [T; N] {
        self.inner
    }
}

impl<T: Word, const N: usize> From<[T; N]> for BitArray<T, N> {
    fn from(value: [T; N]) -> Self {
        Self { inner: value }
    }
}

impl<T: Word, const N: usize> BitSet for BitArray<T, N> {
    fn bit_width(&self) -> usize {
        T::BITS as usize * N
    }

    fn count_ones(&self) -> usize {
        self.inner.iter().map(|w| w.count_ones() as usize).sum()
    }

    fn count_zeros(&self) -> usize {
        self.inner.iter().map(|w| w.count_zeros() as usize).sum()
    }

    fn leading_ones(&self) -> usize {
        let mut stop = false;

        self.inner
            .iter()
            .rev()
            .map(|w| (w.leading_ones(), w.bit_width()))
            .map_while(|(leading_ones, bit_width)| {
                if stop {
                    return None;
                }

                if leading_ones != bit_width {
                    stop = true;
                }

                Some(leading_ones as usize)
            })
            .sum()
    }

    fn leading_zeros(&self) -> usize {
        let mut stop = false;

        self.inner
            .iter()
            .rev()
            .map(|w| (w.leading_zeros(), w.bit_width()))
            .map_while(|(leading_zeros, bit_width)| {
                if stop {
                    return None;
                }

                if leading_zeros != bit_width {
                    stop = true;
                }

                Some(leading_zeros as usize)
            })
            .sum()
    }

    fn trailing_ones(&self) -> usize {
        let mut stop = false;

        self.inner
            .iter()
            .map(|w| (w.trailing_ones(), w.bit_width()))
            .map_while(|(trailing_ones, bit_width)| {
                if stop {
                    return None;
                }

                if trailing_ones != bit_width {
                    stop = true;
                }

                Some(trailing_ones as usize)
            })
            .sum()
    }

    fn trailing_zeros(&self) -> usize {
        let mut stop = false;

        self.inner
            .iter()
            .map(|w| (w.trailing_zeros(), w.bit_width()))
            .map_while(|(trailing_zeros, bit_width)| {
                if stop {
                    return None;
                }

                if trailing_zeros != bit_width {
                    stop = true;
                }

                Some(trailing_zeros as usize)
            })
            .sum()
    }

    fn highest_one(&self) -> Option<usize> {
        let (i, highest_one) = self
            .inner
            .iter()
            .rev()
            .enumerate()
            .flat_map(|(i, w)| w.highest_one().map(|w| (i, w)))
            .next()?;

        Some(highest_one as usize + self.bit_width() - T::BITS as usize - (i * T::BITS as usize))
    }

    fn lowest_one(&self) -> Option<usize> {
        let (i, lowest_one) = self
            .inner
            .iter()
            .enumerate()
            .flat_map(|(i, w)| w.lowest_one().map(|w| (i, w)))
            .next()?;

        Some(lowest_one as usize + i * T::BITS as usize)
    }

    fn test(&self, index: usize) -> Option<bool> {
        let i = index / T::BITS as usize;
        let j = index % T::BITS as usize;

        self.inner[i].test(j as u32)
    }
}

impl<T: Word, const N: usize> BitSetMut for BitArray<T, N> {
    fn set(&mut self, index: usize, val: bool) {
        let i = index / T::BITS as usize;
        let j = index % T::BITS as usize;

        self.inner[i].set(j as u32, val)
    }
}
