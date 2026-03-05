package adapterspebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/amari/mithril/mithril-node-go/internal/adapters/fdbtuple"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/cockroachdb/pebble/v2"
)

type ChunkModel struct {
	Type      ChunkModelType
	ID        domain.ChunkID
	WriterKey []byte
	CreatedAt time.Time
	ExpiresAt time.Time
	UpdatedAt time.Time
	Size      int64
	Version   uint64
	DeletedAt time.Time
}

func (m *ChunkModel) ToDomain() domain.Chunk {
	switch m.Type {
	case ChunkModelTypePending:
		return domain.NewPendingChunk(m.ID, m.WriterKey, m.CreatedAt, m.ExpiresAt)
	case ChunkModelTypeReady:
		return domain.NewReadyChunk(m.ID, m.WriterKey, m.CreatedAt, m.UpdatedAt, m.Size, m.Version)
	case ChunkModelTypeDeleted:
		return domain.NewDeletedChunk(m.ID, m.WriterKey, m.CreatedAt, m.DeletedAt)
	default:
		return nil
	}
}

func must[T any](v T, _ bool) T {
	return v
}

func ChunkModelFromDomain(chunk domain.Chunk) *ChunkModel {
	switch c := chunk.(type) {
	case *domain.PendingChunk:
		return &ChunkModel{
			Type:      ChunkModelTypePending,
			ID:        c.ID(),
			WriterKey: c.WriterKey(),
			CreatedAt: c.CreatedAt(),
			ExpiresAt: must(c.ExpiresAt()),
		}
	case *domain.ReadyChunk:
		return &ChunkModel{
			Type:      ChunkModelTypeReady,
			ID:        c.ID(),
			WriterKey: c.WriterKey(),
			CreatedAt: c.CreatedAt(),
			UpdatedAt: must(c.UpdatedAt()),
			Size:      must(c.Size()),
			Version:   must(c.Version()),
		}
	case *domain.DeletedChunk:
		return &ChunkModel{
			Type:      ChunkModelTypeDeleted,
			ID:        c.ID(),
			WriterKey: c.WriterKey(),
			CreatedAt: c.CreatedAt(),
			DeletedAt: must(c.DeletedAt()),
		}
	default:
		return nil
	}
}

type ChunkModelType int

const (
	ChunkModelTypePending ChunkModelType = iota
	ChunkModelTypeReady
	ChunkModelTypeDeleted
)

type ChunkRepository struct {
	db *pebble.DB
}

var _ domain.ChunkRepository = (*ChunkRepository)(nil)

func NewChunkRepository(db *pebble.DB) *ChunkRepository {
	return &ChunkRepository{
		db: db,
	}
}

func (r *ChunkRepository) Start() error {
	// TODO: start background goroutine for cleaning up expired pending chunks

	return nil
}

func (r *ChunkRepository) Stop() error {
	// TODO: stop background goroutine for cleaning up expired pending chunks

	return nil
}

func (r *ChunkRepository) getModel(_ context.Context, id domain.ChunkID) (*ChunkModel, error) {
	key := fdbtuple.Tuple{"a", id.Bytes()}.Pack()

	value, closer, err := r.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, domain.ErrChunkNotFound
		}

		return nil, fmt.Errorf("%w: %w", ErrPebbleOperationFailed, err)
	}
	defer closer.Close()

	var model ChunkModel

	if err := json.Unmarshal(value, &model); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPebbleModelDecodingFailed, err)
	}

	return &model, nil
}

func (r *ChunkRepository) Get(ctx context.Context, id domain.ChunkID) (domain.Chunk, error) {
	model, err := r.getModel(ctx, id)
	if err != nil {
		return nil, err
	}

	return model.ToDomain(), nil
}

func (r *ChunkRepository) getModelWithWriterKey(_ context.Context, writerKey []byte) (*ChunkModel, error) {
	key := fdbtuple.Tuple{"b", writerKey}.Pack()

	value, closer, err := r.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, domain.ErrChunkNotFound
		}

		return nil, fmt.Errorf("%w: %w", ErrPebbleOperationFailed, err)
	}
	defer closer.Close()

	var model ChunkModel

	if err := json.Unmarshal(value, &model); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPebbleModelDecodingFailed, err)
	}

	return &model, nil
}

func (r *ChunkRepository) GetWithWriterKey(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
	model, err := r.getModelWithWriterKey(ctx, writerKey)
	if err != nil {
		return nil, err
	}

	return model.ToDomain(), nil
}

func (r *ChunkRepository) Upsert(ctx context.Context, chunk domain.Chunk) error {
	keys := [][]byte{
		fdbtuple.Tuple{"a", chunk.ID().Bytes()}.Pack(),
		fdbtuple.Tuple{"b", chunk.WriterKey()}.Pack(),
	}

	model := ChunkModelFromDomain(chunk)
	value, err := json.Marshal(&model)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPebbleModelEncodingFailed, err)
	}

	batch := r.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		_ = batch.Set(key, value, nil)
	}

	switch model.Type {
	case ChunkModelTypePending:
		_ = batch.Set(fdbtuple.Tuple{"c", model.ExpiresAt.UnixMilli(), chunk.ID().Bytes()}.Pack(), value, nil)
	case ChunkModelTypeDeleted:
		_ = batch.Set(fdbtuple.Tuple{"c", model.DeletedAt.UnixMilli(), chunk.ID().Bytes()}.Pack(), value, nil)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("%w: %w", ErrPebbleBatchCommitFailed, err)
	}

	return nil
}

func (r *ChunkRepository) Delete(ctx context.Context, id domain.ChunkID) error {
	key := fdbtuple.Tuple{"a", id.Bytes()}.Pack()

	value, closer, err := r.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// idempotent
			return nil
		}

		return fmt.Errorf("%w: %w", ErrPebbleOperationFailed, err)
	}
	defer closer.Close()

	var model ChunkModel

	if err := json.Unmarshal(value, &model); err != nil {
		return fmt.Errorf("%w: %w", ErrPebbleModelDecodingFailed, err)
	}

	keys := [][]byte{
		key,
		fdbtuple.Tuple{"b", model.WriterKey}.Pack(),
	}

	switch model.Type {
	case ChunkModelTypePending:
		keys = append(keys, fdbtuple.Tuple{"c", model.ExpiresAt.UnixMilli(), model.ID.Bytes()}.Pack(), value, nil)
	case ChunkModelTypeDeleted:
		keys = append(keys, fdbtuple.Tuple{"c", model.DeletedAt.UnixMilli(), model.ID.Bytes()}.Pack(), value, nil)
	}

	batch := r.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		_ = batch.Delete(key, nil)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("%w: %w", ErrPebbleBatchCommitFailed, err)
	}

	return nil
}
