//! Shingled Erasure Coding

use std::num::NonZeroU32;

pub struct Shec {
    /// Number of data shards
    pub data_shards: NonZeroU32,
    /// Number of parity shards
    pub parity_shards: NonZeroU32,
    /// Durability estimator
    pub durability_estimator: NonZeroU32,
}
