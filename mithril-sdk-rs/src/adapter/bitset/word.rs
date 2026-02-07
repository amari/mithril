use std::num::NonZero;

pub trait Word: Copy {
    const BITS: u32;

    fn bit_width(self) -> u32;

    fn count_ones(self) -> u32;

    fn count_zeros(self) -> u32;

    fn leading_ones(self) -> u32;

    fn leading_zeros(self) -> u32;

    fn trailing_ones(self) -> u32;

    fn trailing_zeros(self) -> u32;

    fn isolate_highest_one(self) -> Self;

    fn isolate_lowest_one(self) -> Self;

    fn highest_one(self) -> Option<u32>;

    fn lowest_one(self) -> Option<u32>;

    fn test(self, index: u32) -> Option<bool>;

    fn set(&mut self, index: u32, val: bool);

    fn split_at_highest_one(self) -> Option<(Self, Self)>;

    fn split_at_lowest_one(self) -> Option<(Self, Self)>;
}

macro_rules! uint_impl {
    (
        Self = $SelfT:ty,
    ) => {
        const BITS: u32 = (!0 as $SelfT).count_ones();

        fn bit_width(self) -> u32 {
            <$SelfT>::BITS
        }

        fn count_ones(self) -> u32 {
            <$SelfT>::count_ones(self)
        }

        fn count_zeros(self) -> u32 {
            <$SelfT>::count_zeros(self)
        }

        fn leading_ones(self) -> u32 {
            <$SelfT>::leading_ones(self)
        }

        fn leading_zeros(self) -> u32 {
            <$SelfT>::leading_zeros(self)
        }

        fn trailing_ones(self) -> u32 {
            <$SelfT>::trailing_ones(self)
        }

        fn trailing_zeros(self) -> u32 {
            <$SelfT>::trailing_zeros(self)
        }

        fn isolate_highest_one(self) -> Self {
            self & (((1 as $SelfT) << (<$SelfT>::BITS - 1)).wrapping_shr(self.leading_zeros()))
        }

        fn isolate_lowest_one(self) -> Self {
            self & self.wrapping_neg()
        }

        fn highest_one(self) -> Option<u32> {
            match NonZero::new(self) {
                Some(v) => Some(<$SelfT>::BITS - 1 - v.leading_zeros()),
                None => None,
            }
        }

        fn lowest_one(self) -> Option<u32> {
            match NonZero::new(self) {
                Some(v) => Some(v.trailing_zeros()),
                None => None,
            }
        }

        fn split_at_highest_one(self) -> Option<(Self, Self)> {
            let bit = Word::isolate_highest_one(self);

            if bit == 0 {
                None
            } else {
                Some((bit, self & !bit))
            }
        }

        fn split_at_lowest_one(self) -> Option<(Self, Self)> {
            let bit = Word::isolate_lowest_one(self);

            if bit == 0 {
                None
            } else {
                Some((bit, self & !bit))
            }
        }

        fn test(self, index: u32) -> Option<bool> {
            if index >= Self::BITS {
                return None;
            }
            Some((self & (1 << index as u32)) != 0)
        }

        fn set(&mut self, index: u32, val: bool) {
            if val {
                *self |= 1 << index;
            } else {
                *self &= !(1 << index);
            }
        }
    };
}

impl Word for u8 {
    uint_impl! {
        Self = u8,
    }
}

impl Word for u16 {
    uint_impl! {
        Self = u16,
    }
}

impl Word for u32 {
    uint_impl! {
        Self = u32,
    }
}

impl Word for u64 {
    uint_impl! {
        Self = u64,
    }
}

impl Word for usize {
    uint_impl! {
        Self = usize,
    }
}
