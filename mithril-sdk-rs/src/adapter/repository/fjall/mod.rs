use crate::adapter::fdb_tuple::Pack;
use crate::domain::Role;
use std::ops::Range;

pub struct BlockRepository {}

pub struct ChunkRepository {}

pub struct StripRepository {}

pub struct FragmentRepository {}

pub fn block_key(block_id: &uuid::Uuid) -> impl Pack {
    (b"B", block_id)
}

pub fn block_key_range() -> Range<Vec<u8>> {
    (b"B",).pack_prefix_range_to_vec_range()
}

fn chunk_role(chunk_role: &Role) -> impl Pack {
    match *chunk_role {
        Role::Replica(i) => (1, i),
        Role::Data(i) => (2, i),
        Role::Parity(i) => (3, i),
        Role::LocalData(i) => (4, i),
        Role::LocalParity(i) => (5, i),
        Role::GlobalParity(i) => (6, i),
    }
}

pub fn chunk_key(block_id: &uuid::Uuid, chunk_role_: &Role) -> impl Pack {
    (b"B", block_id, b"C", chunk_role(chunk_role_))
}

pub fn strip_key(block_id: &uuid::Uuid, strip_index: u64) -> impl Pack {
    (b"B", block_id, b"S", strip_index)
}

pub fn fragment_key(block_id: &uuid::Uuid, chunk_role_: &Role, strip_index: u64) -> impl Pack {
    (b"B", block_id, b"F", chunk_role(chunk_role_), strip_index)
}

pub fn fragment_key_range(block_id: &uuid::Uuid) -> Range<Vec<u8>> {
    (b"B", block_id, b"F").pack_prefix_range_to_vec_range()
}
