package chunk

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/amari/mithril/chunk-node/adapter/tuple"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/chunk"
	"github.com/cockroachdb/pebble/v2"
)

type pebbleChunkRepository struct {
	DB *pebble.DB
}

var _ chunk.ChunkRepository = (*pebbleChunkRepository)(nil)

func NewPebbleChunkRepository(db *pebble.DB) chunk.ChunkRepository {
	return &pebbleChunkRepository{
		DB: db,
	}
}

func (r *pebbleChunkRepository) chunkByIDKey(id domain.ChunkID) tuple.Tuple {
	return tuple.Tuple{
		"chunk",
		"by_id",
		[]byte(id[:]),
	}
}

func (r *pebbleChunkRepository) chunkByWriteKeyKey(writeKey []byte) tuple.Tuple {
	return tuple.Tuple{
		"chunk",
		"by_write_key",
		writeKey,
	}
}

func (r *pebbleChunkRepository) getByWriterKey(writerKey []byte, getter interface {
	Get([]byte) ([]byte, io.Closer, error)
}) (*PebbleChunkModel, error) {
	value, closer, err := getter.Get(r.chunkByWriteKeyKey(writerKey).Pack())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, chunkerrors.ErrNotFound
		}

		return nil, err
	}
	defer closer.Close()

	var model PebbleChunkModel

	if _, err := model.UnmarshalMsg(value); err != nil {
		return nil, err
	}

	return &model, nil
}

func (r *pebbleChunkRepository) getByID(id domain.ChunkID, getter interface {
	Get([]byte) ([]byte, io.Closer, error)
}) (*PebbleChunkModel, error) {
	value, closer, err := getter.Get(r.chunkByIDKey(id).Pack())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, chunkerrors.ErrNotFound
		}

		return nil, err
	}
	defer closer.Close()

	var model PebbleChunkModel

	if _, err := model.UnmarshalMsg(value); err != nil {
		return nil, err
	}

	return &model, nil
}

func (r *pebbleChunkRepository) setByWriterKey(writerKey []byte, model *PebbleChunkModel, setter interface {
	Set([]byte, []byte, *pebble.WriteOptions) error
}) error {
	encoded, err := model.MarshalMsg(nil)
	if err != nil {
		return err
	}

	if err := setter.Set(r.chunkByWriteKeyKey(writerKey).Pack(), encoded, nil); err != nil {
		return err
	}

	return nil
}

func (r *pebbleChunkRepository) setByID(id domain.ChunkID, model *PebbleChunkModel, setter interface {
	Set([]byte, []byte, *pebble.WriteOptions) error
}) error {
	encoded, err := model.MarshalMsg(nil)
	if err != nil {
		return err
	}

	if err := setter.Set(r.chunkByIDKey(id).Pack(), encoded, nil); err != nil {
		return err
	}

	return nil
}

func (r *pebbleChunkRepository) deleteByWriterKey(writerKey []byte, deleter interface {
	Delete([]byte, *pebble.WriteOptions) error
}) error {
	if err := deleter.Delete(r.chunkByWriteKeyKey(writerKey).Pack(), nil); err != nil {
		return err
	}

	return nil
}

func (r *pebbleChunkRepository) deleteByID(id domain.ChunkID, deleter interface {
	Delete([]byte, *pebble.WriteOptions) error
}) error {
	if err := deleter.Delete(r.chunkByIDKey(id).Pack(), nil); err != nil {
		return err
	}

	return nil
}

func (r *pebbleChunkRepository) Get(ctx context.Context, id domain.ChunkID) (domain.Chunk, error) {
	model, err := r.getByID(id, r.DB)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, chunkerrors.ErrNotFound
		}

		return nil, err
	}

	return model.ToDomain()
}

func (r *pebbleChunkRepository) GetByWriterKey(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
	model, err := r.getByWriterKey(writerKey, r.DB)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, chunkerrors.ErrNotFound
		}

		return nil, err
	}

	return model.ToDomain()
}

func (r *pebbleChunkRepository) Store(ctx context.Context, chunk domain.Chunk) error {
	batch := r.DB.NewIndexedBatch()
	defer batch.Close()

	model := pebbleChunkModelFromDomain(chunk)

	if err := r.setByWriterKey(model.WriterKey, model, batch); err != nil {
		return err
	}

	if err := r.setByID(model.ID, model, batch); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	return nil
}

func (r *pebbleChunkRepository) Delete(ctx context.Context, id domain.ChunkID) error {
	batch := r.DB.NewIndexedBatch()
	defer batch.Close()

	model, err := r.getByID(id, batch)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}

		return err
	}

	if err := r.deleteByID(id, batch); err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
	}

	if err := r.deleteByWriterKey(model.WriterKey, batch); err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	return nil
}

func (r *pebbleChunkRepository) ListExpired(ctx context.Context, now time.Time, limit int) ([]domain.Chunk, error) {
	var chunks []domain.Chunk

	if limit == 0 {
		return chunks, nil
	}

	iter, err := r.DB.NewIter(&pebble.IterOptions{
		LowerBound: tuple.Tuple{"chunk", "by_expiration"}.Pack(),
		UpperBound: tuple.Tuple{"chunk", "by_expiration", now.UTC().UnixMilli() + 1}.Pack(),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for ok := iter.First(); ok && len(chunks) < limit; ok = iter.Next() {
		value := iter.Value()

		var model PebbleChunkModel

		if _, err := model.UnmarshalMsg(value); err != nil {
			return nil, err
		}

		domainChunk, err := model.ToDomain()
		if err != nil {
			return nil, err
		}

		chunks = append(chunks, domainChunk)
	}

	return chunks, nil
}
