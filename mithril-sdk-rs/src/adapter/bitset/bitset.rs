use super::word::Word;

pub trait BitSet {
    fn bit_width(&self) -> usize;

    fn count_ones(&self) -> usize;

    fn count_zeros(&self) -> usize;

    fn leading_ones(&self) -> usize;

    fn leading_zeros(&self) -> usize;

    fn trailing_ones(&self) -> usize;

    fn trailing_zeros(&self) -> usize;

    fn highest_one(&self) -> Option<usize>;

    fn lowest_one(&self) -> Option<usize>;

    fn test(&self, index: usize) -> Option<bool>;
}

pub trait BitSetMut: BitSet {
    fn set(&mut self, index: usize, val: bool);
}

/*
impl<T: Word> BitSet for T {
    fn bit_width(&self) -> usize {
        Word::bit_width(*self) as usize
    }

    fn count_ones(&self) -> usize {
        Word::count_ones(*self) as usize
    }

    fn count_zeros(&self) -> usize {
        Word::count_zeros(*self) as usize
    }

    fn leading_ones(&self) -> usize {
        Word::leading_ones(*self) as usize
    }

    fn leading_zeros(&self) -> usize {
        Word::leading_zeros(*self) as usize
    }

    fn trailing_ones(&self) -> usize {
        Word::trailing_ones(*self) as usize
    }

    fn trailing_zeros(&self) -> usize {
        Word::trailing_zeros(*self) as usize
    }

    fn highest_one(&self) -> Option<usize> {
        Word::highest_one(*self).map(|v| v as usize)
    }

    fn lowest_one(&self) -> Option<usize> {
        Word::lowest_one(*self).map(|v| v as usize)
    }

    fn test(&self, index: usize) -> Option<bool> {
        Word::test(*self, index.min(u32::MAX as usize) as u32)
    }
}

impl<T: Word> BitSetMut for T {
    fn set(&mut self, index: usize, val: bool) {
        Word::set(self, index.min(u32::MAX as usize) as u32, val)
    }
}
*/

impl<'a, T: BitSet> BitSet for &'a T {
    fn bit_width(&self) -> usize {
        (**self).bit_width()
    }

    fn count_ones(&self) -> usize {
        (**self).count_ones()
    }

    fn count_zeros(&self) -> usize {
        (**self).count_zeros()
    }

    fn leading_ones(&self) -> usize {
        (**self).leading_ones()
    }

    fn leading_zeros(&self) -> usize {
        (**self).leading_zeros()
    }

    fn trailing_ones(&self) -> usize {
        (**self).trailing_ones()
    }

    fn trailing_zeros(&self) -> usize {
        (**self).trailing_zeros()
    }

    fn highest_one(&self) -> Option<usize> {
        (**self).highest_one()
    }

    fn lowest_one(&self) -> Option<usize> {
        (**self).lowest_one()
    }

    fn test(&self, index: usize) -> Option<bool> {
        (**self).test(index)
    }
}

impl<'a, T: BitSetMut> BitSet for &'a mut T {
    fn bit_width(&self) -> usize {
        (**self).bit_width()
    }

    fn count_ones(&self) -> usize {
        (**self).count_ones()
    }

    fn count_zeros(&self) -> usize {
        (**self).count_zeros()
    }

    fn leading_ones(&self) -> usize {
        (**self).leading_ones()
    }

    fn leading_zeros(&self) -> usize {
        (**self).leading_zeros()
    }

    fn trailing_ones(&self) -> usize {
        (**self).trailing_ones()
    }

    fn trailing_zeros(&self) -> usize {
        (**self).trailing_zeros()
    }

    fn highest_one(&self) -> Option<usize> {
        (**self).highest_one()
    }

    fn lowest_one(&self) -> Option<usize> {
        (**self).lowest_one()
    }

    fn test(&self, index: usize) -> Option<bool> {
        (**self).test(index)
    }
}

impl<'a, T: BitSetMut> BitSetMut for &'a mut T {
    fn set(&mut self, index: usize, val: bool) {
        (**self).set(index, val)
    }
}

impl<T: Word> BitSet for [T] {
    fn bit_width(&self) -> usize {
        T::BITS as usize * self.len()
    }

    fn count_ones(&self) -> usize {
        self.iter().map(|w| w.count_ones() as usize).sum()
    }

    fn count_zeros(&self) -> usize {
        self.iter().map(|w| w.count_zeros() as usize).sum()
    }

    fn leading_ones(&self) -> usize {
        let mut stop = false;

        self.iter()
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

        self.iter()
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

        self.iter()
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

        self.iter()
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
            .iter()
            .rev()
            .enumerate()
            .flat_map(|(i, w)| w.highest_one().map(|w| (i, w)))
            .next()?;

        Some(highest_one as usize + self.bit_width() - T::BITS as usize - (i * T::BITS as usize))
    }

    fn lowest_one(&self) -> Option<usize> {
        let (i, lowest_one) = self
            .iter()
            .enumerate()
            .flat_map(|(i, w)| w.lowest_one().map(|w| (i, w)))
            .next()?;

        Some(lowest_one as usize + i * T::BITS as usize)
    }

    fn test(&self, index: usize) -> Option<bool> {
        let i = index / T::BITS as usize;
        let j = index % T::BITS as usize;

        self[i].test(j as u32)
    }
}

impl<T: Word> BitSetMut for [T] {
    fn set(&mut self, index: usize, val: bool) {
        let i = index / T::BITS as usize;
        let j = index % T::BITS as usize;

        self[i].set(j as u32, val)
    }
}
