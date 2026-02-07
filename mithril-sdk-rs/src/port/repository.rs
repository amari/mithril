use crate::domain::{Block, Chunk, Fragment, Role, Stripe};
use std::ops::RangeBounds;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlockRepositoryError {}

pub trait BlockRepository {
    fn get_block(&self, block_id: &uuid::Uuid) -> Result<Block, BlockRepositoryError>;

    fn insert_block(&self, block: &Block) -> Result<(), BlockRepositoryError>;
}

#[derive(Error, Debug)]
pub enum ChunkRepositoryError {}

pub trait ChunkRepository {
    fn get_chunk(&self, block_id: &uuid::Uuid, role: Role) -> Result<Chunk, ChunkRepositoryError>;
    fn get_chunks(
        &self,
        block_id: &uuid::Uuid,
        role_iter: impl Iterator<Item = Role>,
    ) -> Result<Vec<Chunk>, ChunkRepositoryError>;

    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), ChunkRepositoryError>;
}

#[derive(Error, Debug)]
pub enum StripRepositoryError {}

pub trait StripRepository {
    fn get_strip(&self, block_id: &uuid::Uuid, index: u64) -> Result<Stripe, StripRepositoryError>;
    fn get_strip_range(
        &self,
        block_id: &uuid::Uuid,
        index_range: impl RangeBounds<u64>,
    ) -> Result<Vec<Stripe>, StripRepositoryError>;
    fn get_strips(
        &self,
        block_id: &uuid::Uuid,
        index_iter: impl Iterator<Item = u64>,
    ) -> Result<Vec<Stripe>, StripRepositoryError>;

    fn get_last_strip(&self, block_id: &uuid::Uuid) -> Result<Stripe, StripRepositoryError>;

    fn insert_strip(&self, strip: &Stripe) -> Result<(), StripRepositoryError>;
}

#[derive(Error, Debug)]
pub enum FragmentRepositoryError {}

pub trait FragmentRepository {
    fn get_fragment(
        &self,
        block_id: &uuid::Uuid,
        chunk_role: &Role,
        strip_index: u64,
    ) -> Result<Fragment, FragmentRepositoryError>;
    fn get_fragment_range(
        &self,
        block_id: &uuid::Uuid,
        chunk_role: &Role,
        strip_index_range: impl RangeBounds<u64>,
    ) -> Result<Vec<Fragment>, FragmentRepositoryError>;
    fn get_fragments(
        &self,
        block_id: &uuid::Uuid,
        chunk_role: &Role,
        strip_index_iter: impl Iterator<Item = u64>,
    ) -> Result<Vec<Fragment>, FragmentRepositoryError>;
    fn get_fragments_with_strip(
        &self,
        block_id: &uuid::Uuid,
        strip_index: u64,
        chunk_role_iter: impl Iterator<Item = Role>,
    ) -> Result<Vec<Fragment>, FragmentRepositoryError>;
    fn get_all_fragments_with_strip(
        &self,
        block_id: &uuid::Uuid,
        strip_index: u64,
    ) -> Result<Vec<Fragment>, FragmentRepositoryError>;

    fn insert_fragment(&self, fragment: &Fragment) -> Result<(), FragmentRepositoryError>;
}
