use crate::domain::*;
use crate::port::chunk_service::*;
use mithril_proto::chunkstore::v1::{CreateChunkRequest, CreateChunkResponse};
use std::pin::Pin;
use tonic::transport::Channel;

pub struct GrpcChunkService {
    // FIXME: this should use one `client` per node
    pub client: mithril_proto::chunkstore::v1::chunk_service_client::ChunkServiceClient<Channel>,
}

impl ChunkService for GrpcChunkService {
    type CreateFut<'a> =
        Pin<Box<dyn Future<Output = Result<ActualChunkState, ChunkCreateError>> + Send + 'a>>;
    type PutFut<'a> =
        Pin<Box<dyn Future<Output = Result<ActualChunkState, ChunkPutError>> + Send + 'a>>;
    type AppendFut<'a> =
        Pin<Box<dyn Future<Output = Result<ActualChunkState, ChunkAppendError>> + Send + 'a>>;
    type ReadFut<'a> = Pin<
        Box<dyn Future<Output = Result<(usize, ActualChunkState), ChunkReadError>> + Send + 'a>,
    >;
    type StatFut<'a> =
        Pin<Box<dyn Future<Output = Result<ActualChunkState, ChunkStatError>> + Send + 'a>>;
    type DeleteFut<'a> = Pin<Box<dyn Future<Output = Result<(), ChunkDeleteError>> + Send + 'a>>;
    type ShrinkTailSlackFut<'a> = Pin<
        Box<dyn Future<Output = Result<ActualChunkState, ChunkShrinkTailSlackError>> + Send + 'a>,
    >;

    fn create<'a>(&self, write_key: &'a [u8], min_tail_slack_size: u64) -> Self::CreateFut<'a> {
        let mut client = self.client.clone();

        Box::pin(async move {
            let write_key = prost::bytes::Bytes::copy_from_slice(write_key);

            // TODO: set timeout
            let resp = client
                .create_chunk(CreateChunkRequest {
                    write_key: write_key,
                    min_tail_slack_size: Some(min_tail_slack_size),
                })
                .await;

            match resp.map(|r| r.into_inner()) {
                Ok(CreateChunkResponse { chunk: Some(chunk) }) => {
                    let chunk_id: ChunkID = chunk
                        .id
                        .as_ref()
                        .try_into()
                        .map_err(|_| ChunkCreateError::Internal { actual: None })?;

                    Ok(ActualChunkState {
                        chunk_id: chunk_id,
                        version: chunk.version,
                        size: chunk.size,
                    })
                }
                Ok(CreateChunkResponse { chunk: None }) => {
                    Err(ChunkCreateError::Internal { actual: None })
                }
                Err(status) => {
                    // TODO: map the status code to an error

                    // TODO: decode error details from a `google.rpc.Status`
                    let _ = status.details();

                    unimplemented!()
                }
            }
        })
    }

    fn put<'a>(
        &self,
        _write_key: &'a [u8],
        _data: &'a [u8],
        _min_tail_slack_size: u64,
    ) -> Self::PutFut<'a> {
        todo!()
    }

    fn append<'a>(
        &self,
        _chunk: &'a Chunk,
        _data: &'a [u8],
        _min_tail_slack_size: u64,
    ) -> Self::AppendFut<'a> {
        todo!()
    }

    fn read<'a>(&self, _chunk: &'a Chunk, _buf: &'a mut [u8]) -> Self::ReadFut<'a> {
        todo!()
    }

    fn stat<'a>(&self, _chunk: &'a Chunk) -> Self::StatFut<'a> {
        todo!()
    }

    fn delete<'a>(&self, _chunk: &'a Chunk) -> Self::DeleteFut<'a> {
        todo!()
    }

    fn shrink_tail_slack<'a>(
        &self,
        _chunk: &'a Chunk,
        _max_tail_slack_size: u64,
    ) -> Self::ShrinkTailSlackFut<'a> {
        todo!()
    }
}
