use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use thiserror::Error;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum CodingScheme {
    /// Simple replication
    Replication {
        /// Number of replicas
        factor: u32,
    },
    /// XOR-based erasure coding
    Xor {
        /// Number of data shards per stripe
        data_shards: u32,
    },
    /// Reed-Solomon erasure coding
    ReedSolomon {
        /// Number of data shards per stripe
        data_shards: u32,
        /// Number of parity shards per stripe
        parity_shards: u32,
        /// Reed-Solomon flavor
        flavor: ReedSolomonFlavor,
    },
    /// Microsoft LRC (Local Reconstruction Codes) erasure coding
    MicrosoftLrc {
        /// Number of data shards per stripe
        data_shards: u32,
        /// Number of local parity shards per stripe
        local_shards: u32,
        /// Number of global parity shards per stripe
        global_shards: u32,
    },
    /// SHEC (Shingled Erasure Code) erasure coding
    Shec {
        /// Number of data shards per stripe
        data_shards: u32,
        /// Number of parity shards per stripe
        parity_shards: u32,
        /// Durability estimator
        durability_estimator: u32,
    },
}

impl CodingScheme {
    /// Get all roles for this coding scheme
    pub fn roles(&self) -> Vec<Role> {
        match self {
            CodingScheme::Replication { factor } => (0..*factor).map(Role::Replica).collect(),
            CodingScheme::ReedSolomon {
                data_shards: data_chunks,
                parity_shards: parity_chunks,
                ..
            } => {
                let mut roles = Vec::new();
                roles.extend((0..*data_chunks).map(Role::Data));
                roles.extend((0..*parity_chunks).map(Role::Parity));
                roles
            }
            CodingScheme::MicrosoftLrc {
                data_shards: data_chunks,
                local_shards: local_chunks,
                global_shards: global_chunks,
            } => {
                let mut roles = Vec::new();
                roles.extend((0..*data_chunks).map(Role::LocalData));
                roles.extend((0..*local_chunks).map(Role::LocalParity));
                roles.extend((0..*global_chunks).map(Role::GlobalParity));
                roles
            }
            // ... others
            _ => todo!(),
        }
    }

    /// Total number of chunks required
    pub fn total_chunks(&self) -> u32 {
        match self {
            CodingScheme::Replication { factor } => *factor,
            CodingScheme::ReedSolomon {
                data_shards: data_chunks,
                parity_shards: parity_chunks,
                ..
            } => data_chunks + parity_chunks,
            CodingScheme::MicrosoftLrc {
                data_shards: data_chunks,
                local_shards: local_chunks,
                global_shards: global_chunks,
            } => data_chunks + local_chunks + global_chunks,
            CodingScheme::Xor {
                data_shards: data_chunks,
            } => data_chunks + 1,
            CodingScheme::Shec {
                data_shards: data_chunks,
                parity_shards: parity_chunks,
                ..
            } => data_chunks + parity_chunks,
        }
    }

    /// Failure tolerance (how many chunks can be lost)
    pub fn fault_tolerance(&self) -> u32 {
        match self {
            CodingScheme::Replication { factor } => factor - 1,
            CodingScheme::ReedSolomon {
                parity_shards: parity_chunks,
                ..
            } => *parity_chunks,
            CodingScheme::MicrosoftLrc {
                global_shards: global_chunks,
                ..
            } => {
                // LRC can tolerate local_chunks failures within a group
                // or global_chunks failures across groups
                *global_chunks
            }
            CodingScheme::Xor { .. } => 1,
            CodingScheme::Shec {
                parity_shards: parity_chunks,
                ..
            } => *parity_chunks,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum ReedSolomonFlavor {
    /// The "reed-solomon-simd" crate
    ///
    /// https://crates.io/crates/reed-solomon-simd
    ReedSolomonSimdCrate,
    /// Intel Intelligent Storage Acceleration Library (ISA-L)
    ///
    /// https://github.com/intel/isa-l
    IntelIsaL,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Compression {
    Lz4 { level: u32 },
    Zstd { level: i32 },
    Snappy,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Checksum {
    Crc32c,
    Blake3_128,
    Blake3_256,
    Blake3_512,
    Sha2_256,
    Sha2_512,
    XxHash3_64,
    XxHash3_128,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum ChecksumValue {
    Crc32c([u8; 4]),
    Blake3_128([u8; 16]),
    Blake3_256([u8; 32]),
    Blake3_512([u8; 64]),
    Sha2_256([u8; 32]),
    Sha2_512([u8; 64]),
    XxHash3_64([u8; 8]),
    XxHash3_128([u8; 16]),
}

pub struct Block {
    pub id: uuid::Uuid,
    pub write_key: Box<[u8]>,
    pub granularity: u64,
    pub capacity: u64,
    pub compression: Option<Compression>,
    pub strip_checksum: Option<Checksum>,
    pub fragment_checksum: Option<Checksum>,
    pub coding: CodingScheme,
    pub chunks: BTreeMap<Role, ChunkID>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkID([u8; 16]);

impl ChunkID {
    /// Construct ChunkID from parts
    pub fn new(timestamp_ms: u64, node_id: u32, volume_id: u16, sequence: u16) -> Self {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&timestamp_ms.to_be_bytes());
        bytes[8..12].copy_from_slice(&node_id.to_be_bytes());
        bytes[12..14].copy_from_slice(&volume_id.to_be_bytes());
        bytes[14..16].copy_from_slice(&sequence.to_be_bytes());
        ChunkID(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Unix milliseconds (timestamp)
    pub fn as_unix_millis(&self) -> u64 {
        unsafe { u64::from_be_bytes(self.0[0..8].try_into().unwrap_unchecked()) }
    }

    /// SystemTime
    pub fn as_time(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(self.as_unix_millis())
    }

    /// Node ID (32-bit)
    pub fn as_node_id(&self) -> u32 {
        unsafe { u32::from_be_bytes(self.0[8..12].try_into().unwrap_unchecked()) }
    }

    /// Volume ID (16-bit)
    pub fn as_volume_id(&self) -> u16 {
        u16::from_be_bytes(self.0[12..14].try_into().unwrap())
    }

    /// Sequence number (16-bit)
    pub fn as_sequence(&self) -> u16 {
        u16::from_be_bytes(self.0[14..16].try_into().unwrap())
    }

    pub fn to_string(self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }
}

impl fmt::Debug for ChunkID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkID({})", self.to_string())
    }
}

impl fmt::Display for ChunkID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Error, Debug)]
pub enum ChunkIDError {
    #[error("Base64 decoding error: {0}")]
    Base64DecodeError(#[from] base64::DecodeError),
    #[error("Invalid length for ChunkID")]
    InvalidLength,
}

impl TryFrom<&str> for ChunkID {
    type Error = ChunkIDError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = URL_SAFE_NO_PAD.decode(value)?;
        let array: [u8; 16] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| ChunkIDError::InvalidLength)?;
        Ok(ChunkID(array))
    }
}

impl TryFrom<String> for ChunkID {
    type Error = ChunkIDError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ChunkID::try_from(value.as_str())
    }
}

impl<'a> TryFrom<&'a [u8]> for ChunkID {
    type Error = ChunkIDError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let array: [u8; 16] = value.try_into().map_err(|_| ChunkIDError::InvalidLength)?;
        Ok(ChunkID(array))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Role {
    Replica(u32),
    Data(u32),
    Parity(u32),
    LocalData(u32),
    LocalParity(u32),
    GlobalParity(u32),
}

impl Role {
    pub fn as_replica(&self) -> Option<u32> {
        if let Role::Replica(n) = self {
            Some(*n)
        } else {
            None
        }
    }

    pub fn as_data(&self) -> Option<u32> {
        if let Role::Data(n) = self {
            Some(*n)
        } else {
            None
        }
    }

    pub fn as_parity(&self) -> Option<u32> {
        if let Role::Parity(n) = self {
            Some(*n)
        } else {
            None
        }
    }

    /// Get the numeric index regardless of variant
    pub fn index(&self) -> u32 {
        match self {
            Role::Replica(n) => *n,
            Role::Data(n) => *n,
            Role::Parity(n) => *n,
            Role::LocalData(n) => *n,
            Role::LocalParity(n) => *n,
            Role::GlobalParity(n) => *n,
        }
    }

    /// Is this a data-bearing role (not parity)?
    pub fn is_data(&self) -> bool {
        matches!(self, Role::Replica(_) | Role::Data(_) | Role::LocalData(_))
    }

    /// Is this a parity role?
    pub fn is_parity(&self) -> bool {
        matches!(
            self,
            Role::Parity(_) | Role::LocalParity(_) | Role::GlobalParity(_)
        )
    }
}

pub struct Chunk {
    pub block_id: uuid::Uuid,
    pub role: Role,

    pub id: ChunkID,
    pub write_key: Box<[u8]>,
    pub version: u64,
    pub size: u64,
}

/// A horizontal slice of a block across all chunks
pub struct Stripe {
    pub block_id: uuid::Uuid,
    pub index: u64,
    pub compression: Option<Compression>,
    pub checksum: Option<ChecksumValue>,
}

/// A single unit within a stripe, stored in one chunk
pub struct Fragment {
    pub block_id: uuid::Uuid,
    pub role: Role,
    pub stripe_index: u64,
    pub stripe_granularity: u64,

    pub chunk_id: ChunkID,
    pub chunk_offset: u64,
    pub chunk_length: u64,
    pub checksum: Option<ChecksumValue>,
}
