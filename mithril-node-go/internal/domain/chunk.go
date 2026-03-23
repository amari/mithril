package domain

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"time"
)

type Chunk interface {
	isChunk()

	ID() ChunkID
	WriterKey() []byte
	CreatedAt() time.Time
	ExpiresAt() (time.Time, bool)
	UpdatedAt() (time.Time, bool)
	Size() (int64, bool)
	Version() (uint64, bool)
	DeletedAt() (time.Time, bool)
	State() ChunkState
}

type PendingChunk struct {
	id        ChunkID
	writerKey []byte
	createdAt time.Time
	expiresAt time.Time
}

type ReadyChunk struct {
	id        ChunkID
	writerKey []byte
	createdAt time.Time
	updatedAt time.Time
	size      int64
	version   uint64
}

type DeletedChunk struct {
	id        ChunkID
	writerKey []byte
	createdAt time.Time
	deletedAt time.Time
}

var (
	_ Chunk = (*PendingChunk)(nil)
	_ Chunk = (*ReadyChunk)(nil)
	_ Chunk = (*DeletedChunk)(nil)
)

func NewPendingChunk(id ChunkID, writerKey []byte, createdAt time.Time, expiresAt time.Time) *PendingChunk {
	return &PendingChunk{
		id:        id,
		writerKey: writerKey,
		createdAt: createdAt,
		expiresAt: expiresAt,
	}
}
func (*PendingChunk) isChunk()                       {}
func (c *PendingChunk) ID() ChunkID                  { return c.id }
func (c *PendingChunk) WriterKey() []byte            { return c.writerKey }
func (c *PendingChunk) CreatedAt() time.Time         { return c.createdAt }
func (c *PendingChunk) ExpiresAt() (time.Time, bool) { return c.expiresAt, true }
func (c *PendingChunk) UpdatedAt() (time.Time, bool) { return time.Time{}, false }
func (c *PendingChunk) Size() (int64, bool)          { return 0, false }
func (c *PendingChunk) Version() (uint64, bool)      { return 0, false }
func (c *PendingChunk) DeletedAt() (time.Time, bool) { return time.Time{}, false }
func (*PendingChunk) State() ChunkState              { return ChunkStatePending }

func NewReadyChunk(id ChunkID, writerKey []byte, createdAt time.Time, updatedAt time.Time, size int64, version uint64) *ReadyChunk {
	return &ReadyChunk{
		id:        id,
		writerKey: writerKey,
		createdAt: createdAt,
		updatedAt: updatedAt,
		size:      size,
		version:   version,
	}
}
func (*ReadyChunk) isChunk()                       {}
func (c *ReadyChunk) ID() ChunkID                  { return c.id }
func (c *ReadyChunk) WriterKey() []byte            { return c.writerKey }
func (c *ReadyChunk) CreatedAt() time.Time         { return c.createdAt }
func (c *ReadyChunk) ExpiresAt() (time.Time, bool) { return time.Time{}, false }
func (c *ReadyChunk) UpdatedAt() (time.Time, bool) { return c.updatedAt, true }
func (c *ReadyChunk) Size() (int64, bool)          { return c.size, true }
func (c *ReadyChunk) Version() (uint64, bool)      { return c.version, true }
func (c *ReadyChunk) DeletedAt() (time.Time, bool) { return time.Time{}, false }
func (*ReadyChunk) State() ChunkState              { return ChunkStateReady }

func NewDeletedChunk(id ChunkID, writerKey []byte, createdAt time.Time, deletedAt time.Time) *DeletedChunk {
	return &DeletedChunk{
		id:        id,
		writerKey: writerKey,
		createdAt: createdAt,
		deletedAt: deletedAt,
	}
}
func (*DeletedChunk) isChunk()                       {}
func (c *DeletedChunk) ID() ChunkID                  { return c.id }
func (c *DeletedChunk) WriterKey() []byte            { return c.writerKey }
func (c *DeletedChunk) CreatedAt() time.Time         { return c.createdAt }
func (c *DeletedChunk) ExpiresAt() (time.Time, bool) { return time.Time{}, false }
func (c *DeletedChunk) UpdatedAt() (time.Time, bool) { return time.Time{}, false }
func (c *DeletedChunk) Size() (int64, bool)          { return 0, false }
func (c *DeletedChunk) Version() (uint64, bool)      { return 0, false }
func (c *DeletedChunk) DeletedAt() (time.Time, bool) { return c.deletedAt, true }
func (*DeletedChunk) State() ChunkState              { return ChunkStateDeleted }

type ChunkState int

const (
	ChunkStateUnknown ChunkState = iota
	ChunkStatePending
	ChunkStateReady
	ChunkStateDeleted
)

type ChunkRepository interface {
	Get(ctx context.Context, id ChunkID) (Chunk, error)
	GetWithWriterKey(ctx context.Context, writerKey []byte) (Chunk, error)
	Upsert(ctx context.Context, chunk Chunk) error
	Delete(ctx context.Context, id ChunkID) error
}

type ChunkStorage interface {
	Open(ctx context.Context, id ChunkID) (ChunkHandle, error)
	Create(ctx context.Context, id ChunkID, opts CreateChunkOptions) error
	Put(ctx context.Context, id ChunkID, r io.Reader, n int64, opts PutChunkOptions) error
	Append(ctx context.Context, id ChunkID, knownSize int64, r io.Reader, n int64, opts AppendChunkOptions) error
	Delete(ctx context.Context, id ChunkID) error
	ShrinkToFit(ctx context.Context, id ChunkID, knownSize int64, opts ShrinkChunkToFitOptions) error
	Exists(ctx context.Context, id ChunkID) (bool, error)
}

type CreateChunkOptions struct {
	MinTailSlackLength int64
}

type PutChunkOptions struct {
	MinTailSlackLength int64
}

type AppendChunkOptions struct {
	MinTailSlackLength int64
}

type ShrinkChunkToFitOptions struct {
	MaxTailSlackLength int64
}

type ChunkHandle interface {
	io.Closer

	OpenReader(ctx context.Context) (ChunkReader, error)
	OpenRangeReader(ctx context.Context, offset, length int64) (ChunkRangeReader, error)
	OpenReaderAt(ctx context.Context) (ChunkReaderAt, error)
}

type ChunkReader interface {
	io.Closer
	io.Reader
}

type ChunkRangeReader interface {
	io.Closer
	io.Reader
}

type ChunkReaderAt interface {
	io.Closer
	io.ReaderAt
}

type ChunkID [16]byte

func NewChunkID(unixMillis int64, node NodeID, volume VolumeID, sequence uint16) ChunkID {
	var id ChunkID

	binary.BigEndian.PutUint64(id[0:], uint64(unixMillis))
	binary.BigEndian.PutUint32(id[8:], uint32(node))
	binary.BigEndian.PutUint16(id[12:], uint16(volume))
	binary.BigEndian.PutUint16(id[14:], sequence)

	return id
}

func (id ChunkID) UnixMilli() int64 {
	return int64(binary.BigEndian.Uint64(id[0:]))
}

func (id ChunkID) Time() time.Time {
	return time.UnixMilli(id.UnixMilli())
}

func (id ChunkID) NodeID() NodeID {
	return NodeID(binary.BigEndian.Uint32(id[8:]))
}

func (id ChunkID) VolumeID() VolumeID {
	return VolumeID(binary.BigEndian.Uint16(id[12:]))
}

func (id ChunkID) Sequence() uint16 {
	return binary.BigEndian.Uint16(id[14:])
}

func (id ChunkID) Bytes() []byte {
	return id[:]
}

func ParseChunkID(s string) (ChunkID, error) {
	data, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ChunkID{}, err
	}

	if len(data) != 16 {
		return ChunkID{}, ErrChunkInvalidID
	}

	var id ChunkID

	copy(id[:], data)

	return id, nil
}

func (id ChunkID) String() string {
	return base64.RawURLEncoding.EncodeToString(id[:])
}

func (id ChunkID) MarshalBinary() ([]byte, error) {
	return []byte(id[:]), nil
}

func (id *ChunkID) UnmarshalBinary(data []byte) error {
	if len(data) != 16 {
		return ErrChunkInvalidID
	}

	copy(id[:], data)

	return nil
}

func (id ChunkID) MarshalText() ([]byte, error) {
	return []byte(base64.RawURLEncoding.EncodeToString(id[:])), nil
}

func (id *ChunkID) UnmarshalText(data []byte) error {
	newID, err := ParseChunkID(string(data))
	if err != nil {
		return err
	}

	copy(id[:], newID[:])

	return nil
}

type ChunkIDGenerator interface {
	Generate(volume VolumeID) (ChunkID, error)
}
