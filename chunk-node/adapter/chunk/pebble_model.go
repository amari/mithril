//go:generate msgp

package chunk

import (
	"errors"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
)

type PebbleChunkModel struct {
	ID        [16]byte  `msg:"id"`
	WriterKey []byte    `msg:"writer_key"`
	State     string    `msg:"state"`
	Size      int64     `msg:"size,omitempty"`
	Version   uint64    `msg:"version,omitempty"`
	CreatedAt time.Time `msg:"created_at,omitempty"`
	UpdatedAt time.Time `msg:"updated_at,omitempty"`
	DeletedAt time.Time `msg:"deleted_at,omitempty"`
	ExpiresAt time.Time `msg:"expires_at,omitempty"`
}

const (
	ChunkStateTemp      = "temp"
	ChunkStateAvailable = "available"
	ChunkStateDeleted   = "deleted"
)

func pebbleChunkModelFromDomain(chunk domain.Chunk) *PebbleChunkModel {
	if chunk == nil {
		return nil
	}

	switch v := chunk.(type) {
	case *domain.AvailableChunk:
		return pebbleAvailableChunkModelFromDomain(v)
	case *domain.DeletedChunk:
		return pebbleDeletedChunkModelFromDomain(v)
	case *domain.TempChunk:
		return pebbleTempChunkModelFromDomain(v)
	default:
		return nil
	}
}

func pebbleTempChunkModelFromDomain(chunk *domain.TempChunk) *PebbleChunkModel {
	if chunk == nil {
		return nil
	}

	return &PebbleChunkModel{
		State:     ChunkStateTemp,
		ID:        [16]byte(chunk.ID),
		WriterKey: chunk.WriterKey,
		ExpiresAt: chunk.ExpiresAt,
		CreatedAt: chunk.CreatedAt,
	}
}

func pebbleAvailableChunkModelFromDomain(chunk *domain.AvailableChunk) *PebbleChunkModel {
	if chunk == nil {
		return nil
	}

	return &PebbleChunkModel{
		State:     ChunkStateAvailable,
		ID:        [16]byte(chunk.ID),
		WriterKey: chunk.WriterKey,
		Size:      chunk.Size,
		Version:   chunk.Version,
		CreatedAt: chunk.CreatedAt,
		UpdatedAt: chunk.UpdatedAt,
	}
}

func pebbleDeletedChunkModelFromDomain(chunk *domain.DeletedChunk) *PebbleChunkModel {
	if chunk == nil {
		return nil
	}

	return &PebbleChunkModel{
		State:     ChunkStateDeleted,
		ID:        [16]byte(chunk.ID),
		WriterKey: chunk.WriterKey,
		ExpiresAt: chunk.ExpiresAt,
		DeletedAt: chunk.DeletedAt,
	}
}

func (m *PebbleChunkModel) ToDomain() (domain.Chunk, error) {
	if m == nil {
		return nil, nil
	}

	switch m.State {
	case ChunkStateAvailable:
		return &domain.AvailableChunk{
			ID:        domain.ChunkID(m.ID),
			WriterKey: m.WriterKey,
			Size:      m.Size,
			Version:   m.Version,
			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		}, nil
	case ChunkStateDeleted:
		return &domain.DeletedChunk{
			ID:        domain.ChunkID(m.ID),
			WriterKey: m.WriterKey,
			ExpiresAt: m.ExpiresAt,
			DeletedAt: m.DeletedAt,
		}, nil
	case ChunkStateTemp:
		return &domain.TempChunk{
			ID:        domain.ChunkID(m.ID),
			WriterKey: m.WriterKey,
			ExpiresAt: m.ExpiresAt,
			CreatedAt: m.CreatedAt,
		}, nil
	default:
		return nil, errors.New("invalid chunk state")
	}
}
