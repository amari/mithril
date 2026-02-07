use std::{io, num::NonZeroU32};
use thiserror::Error;

#[derive(Clone)]
pub struct Replication {
    factor: NonZeroU32,
}

impl Replication {
    pub const fn new(factor: NonZeroU32) -> Self {
        Replication { factor }
    }

    pub fn encode<'a>(&self, data: &'a [u8]) -> Encoded<'a> {
        Encoded {
            data: data,
            factor: self.factor,
        }
    }
}

pub struct Encoded<'a> {
    data: &'a [u8],
    factor: NonZeroU32,
}

impl<'a> Encoded<'a> {
    pub fn get_replica_bytes(&self, idx: u32) -> Option<&'a [u8]> {
        if idx < self.factor.get() {
            Some(self.data)
        } else {
            None
        }
    }

    pub fn replica_size(&self) -> usize {
        self.data.len()
    }

    pub fn replica_iter(&self) -> impl Iterator<Item = (u32, &'a [u8])> {
        (0..self.factor.get()).map(|idx| (idx, self.data))
    }

    pub fn write_replica_to<W: io::Write>(&self, idx: u32, w: &mut W) -> Result<(), EncodeError> {
        if idx < self.factor.get() {
            w.write_all(self.data)?;

            Ok(())
        } else {
            Err(EncodeError::InvalidReplicaIndex {
                idx,
                replicas: self.factor.get(),
            })
        }
    }

    pub fn copy_replica_to_slice(&self, idx: u32, dst: &mut [u8]) {
        self.try_copy_replica_to_slice(idx, dst).unwrap()
    }

    pub fn try_copy_replica_to_slice(&self, idx: u32, dst: &mut [u8]) -> Result<(), EncodeError> {
        if let Some(data) = self.get_replica_bytes(idx) {
            let data_len = data.len();

            // Validate buffer size
            if dst.len() != data_len {
                return Err(EncodeError::InvalidBufferSize {
                    expected: data_len,
                    actual: dst.len(),
                });
            }

            dst[..data_len].copy_from_slice(data);

            Ok(())
        } else {
            Err(EncodeError::InvalidReplicaIndex {
                idx,
                replicas: self.factor.get(),
            })
        }
    }
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("replica index {idx} is invalid; encoder exposes {replicas} replicas")]
    InvalidReplicaIndex { idx: u32, replicas: u32 },

    #[error("buffer has incorrect size: expected {expected}, got {actual}")]
    InvalidBufferSize { expected: usize, actual: usize },

    #[error("i/o")]
    IoError(#[from] io::Error),
}
