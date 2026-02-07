pub trait Slice {
    fn bit_width(self) -> usize;
}

pub trait SliceMut: Slice {}
