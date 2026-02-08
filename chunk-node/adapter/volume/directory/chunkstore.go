package directory

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/amari/mithril/chunk-node/unix"
	"github.com/rs/zerolog"
)

type directoryChunkStore struct {
	root       *Root
	bufferPool *sync.Pool
}

var _ port.ChunkStore = (*directoryChunkStore)(nil)

func NewChunkStore(root *Root, bufferSize int) *directoryChunkStore {
	return &directoryChunkStore{
		root: root,
		bufferPool: &sync.Pool{
			New: func() any {
				return make([]byte, max(bufferSize, 32*1024))
			},
		},
	}
}

func chunkPath(id domain.ChunkID, state domain.ChunkState) string {
	timestamp := id.UnixMilli()
	sequence := id.Sequence()

	var bytes [10]byte
	binary.BigEndian.PutUint64(bytes[0:8], uint64(timestamp))
	binary.BigEndian.PutUint16(bytes[8:10], sequence)

	var b strings.Builder

	switch state {
	case domain.ChunkStateTemp:
		b.WriteString("tmp")
	case domain.ChunkStateAvailable:
		b.WriteString("available")
	case domain.ChunkStateDeleted:
		b.WriteString("deleted")
	default:
		b.WriteString("unknown")
	}

	tmp := make([]byte, 0, 8)

	// top 32 bits (4 bytes) of timestamp change once every ~50 days
	b.WriteByte('/')
	tmp = hex.AppendEncode(tmp, bytes[0:4])
	b.Write(tmp)
	tmp = tmp[:0]

	for i := 4; i < 10; i += 1 {
		b.WriteByte('/')
		tmp = hex.AppendEncode(tmp, bytes[i:][:1])
		b.Write(tmp)
		tmp = tmp[:0]
	}

	return b.String()
}

func (s *directoryChunkStore) Close() error {
	return s.root.Close()
}

func (s *directoryChunkStore) ChunkExists(ctx context.Context, id domain.ChunkID) (bool, error) {
	path := chunkPath(id, domain.ChunkStateAvailable)

	if st, err := s.root.Stat(path); err == nil && st.Mode().IsRegular() {
		return true, nil
	}

	return false, nil
}

func (s *directoryChunkStore) OpenChunk(ctx context.Context, id domain.ChunkID) (port.Chunk, error) {
	path := chunkPath(id, domain.ChunkStateAvailable)

	f, err := s.root.OpenFile(path, os.O_RDONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", chunkerrors.ErrNotFound, id.String())
		}
		return nil, fmt.Errorf("opening chunk file: %w", err)
	}

	return &fileChunk{
		id:   id,
		file: f,
	}, nil
}

func (s *directoryChunkStore) CreateChunk(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error {
	path := chunkPath(id, domain.ChunkStateAvailable)

	// does the chunk exist already?
	_, err := s.root.Stat(path)
	if err == nil {
		return fmt.Errorf("%w: %s", chunkerrors.ErrWriterKeyConflict, id.String())
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("checking chunk existence: %w", err)
	}

	// create necessary directories
	syncBuffer := NewSyncBuffer()
	defer func() {
		// TODO: handle error
		_ = syncBuffer.Close()
	}()

	sb, err := s.root.MkdirAll(filepath.Dir(path), 0o777)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Str("path", filepath.Dir(path)).Msg("Failed to create chunk directory")

		return fmt.Errorf("creating chunk directory: %w", err)
	}
	MergeSyncBuffers(syncBuffer, sb)

	// create file
	f, err := s.root.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("creating chunk file: %w", err)
	}
	defer f.Close()

	// reserve space
	fileSize := max(minTailSlackSize, 0)
	if err := unix.Fallocate(int(f.Fd()), 0, 0, fileSize); err != nil {
		return fmt.Errorf("reserving space for chunk file: %w", err)
	}

	// sync file
	if err := unix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("syncing chunk file: %w", err)
	}

	// sync directories
	if err := syncBuffer.Flush(); err != nil {
		return fmt.Errorf("syncing chunk directories: %w", err)
	}

	return nil
}

func (s *directoryChunkStore) PutChunk(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
	firstPath := chunkPath(id, domain.ChunkStateTemp)
	finalPath := chunkPath(id, domain.ChunkStateAvailable)

	// does the chunk exist already?
	_, err := s.root.Stat(firstPath)
	if err == nil {
		return fmt.Errorf("%w: %s", chunkerrors.ErrWriterKeyConflict, id.String())
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("checking chunk existence: %w", err)
	}

	_, err = s.root.Stat(finalPath)
	if err == nil {
		return fmt.Errorf("%w: %s", chunkerrors.ErrWriterKeyConflict, id.String())
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("checking chunk existence: %w", err)
	}

	// create necessary directories
	syncBuffer := NewSyncBuffer()
	defer func() {
		// TODO: handle error
		_ = syncBuffer.Close()
	}()

	sb, err := s.root.MkdirAll(filepath.Dir(firstPath), 0o777)
	if err != nil {
		return fmt.Errorf("creating chunk directory: %w", err)
	}
	MergeSyncBuffers(syncBuffer, sb)

	sb, err = s.root.MkdirAll(filepath.Dir(finalPath), 0o777)
	if err != nil {
		return fmt.Errorf("creating chunk directory: %w", err)
	}
	MergeSyncBuffers(syncBuffer, sb)

	// create file
	f, err := s.root.OpenFile(firstPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("creating chunk file: %w", err)
	}
	defer f.Close()

	// reserve space
	fileSize := max(n, 0) + max(minTailSlackSize, 0)
	if err := unix.Fallocate(int(f.Fd()), 0, 0, fileSize); err != nil {
		return fmt.Errorf("reserving space for chunk file: %w", err)
	}

	// write file
	buf := s.bufferPool.Get().([]byte)
	defer s.bufferPool.Put(buf)

	written, err := io.CopyBuffer(f, r, buf)
	if err != nil {
		return fmt.Errorf("writing chunk file: %w", err)
	}
	if written != n {
		return fmt.Errorf("written size mismatch: expected %d, got %d", n, written)
	}

	// sync file
	if err := unix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("syncing chunk file: %w", err)
	}

	// rename to final path
	if err := s.root.Rename(firstPath, finalPath); err != nil {
		return fmt.Errorf("renaming chunk file to final path: %w", err)
	}

	// sync directories
	if err := syncBuffer.Flush(); err != nil {
		return fmt.Errorf("syncing chunk directories: %w", err)
	}

	return nil
}

func (s *directoryChunkStore) AppendChunk(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error {
	path := chunkPath(id, domain.ChunkStateAvailable)

	// does the chunk exist?
	st, err := s.root.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s", chunkerrors.ErrNotFound, id.String())
		}

		return fmt.Errorf("checking chunk existence: %w", err)
	}

	if st.Size() < logicalSize {
		return fmt.Errorf("chunk file size (%d) is smaller than logical size (%d)", st.Size(), logicalSize)
	}

	// open file
	f, err := s.root.OpenFile(path, os.O_RDWR, 0o666)
	if err != nil {
		return fmt.Errorf("opening chunk file: %w", err)
	}
	defer f.Close()

	// reserve space if needed
	minFileSize := logicalSize + max(n, 0) + max(minTailSlackSize, 0)
	if st.Size() < minFileSize {
		if err := unix.Fallocate(int(f.Fd()), 0, st.Size(), minFileSize-st.Size()); err != nil {
			return fmt.Errorf("reserving space for chunk file: %w", err)
		}
	}

	// seek to logical end
	_, err = f.Seek(logicalSize, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking chunk file: %w", err)
	}

	// write data
	buf := s.bufferPool.Get().([]byte)
	defer s.bufferPool.Put(buf)

	written, err := io.CopyBuffer(f, r, buf)
	if err != nil {
		return fmt.Errorf("appending to chunk file: %w", err)
	}
	if written != n {
		return fmt.Errorf("written size mismatch: expected %d, got %d", n, written)
	}

	// sync file
	if err := unix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("syncing chunk file: %w", err)
	}

	return nil
}

func (s *directoryChunkStore) DeleteChunk(ctx context.Context, id domain.ChunkID) error {
	firstPath := chunkPath(id, domain.ChunkStateAvailable)
	finalPath := chunkPath(id, domain.ChunkStateDeleted)

	// does the chunk exist?
	_, err := s.root.Stat(firstPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("checking chunk existence: %w", err)
		}
		return fmt.Errorf("%w: %s", chunkerrors.ErrNotFound, id.String())
	}

	// create dest directory
	syncBuffer := NewSyncBuffer()
	defer func() {
		// TODO: handle error
		_ = syncBuffer.Close()
	}()

	sb, err := s.root.MkdirAll(filepath.Dir(finalPath), 0o777)
	if err != nil {
		return fmt.Errorf("creating chunk directory: %w", err)
	}
	MergeSyncBuffers(syncBuffer, sb)

	// open src directory
	dirF, err := s.root.OpenFile(filepath.Dir(firstPath), os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("opening chunk directory: %w", err)
	}
	defer dirF.Close()
	syncBuffer.AddSyncFd(int(dirF.Fd()))

	// rename file to final path
	if err := s.root.Rename(firstPath, finalPath); err != nil {
		return fmt.Errorf("renaming chunk file to final path: %w", err)
	}

	// sync directories
	if err := syncBuffer.Flush(); err != nil {
		return fmt.Errorf("syncing chunk directories: %w", err)
	}

	return nil
}

func (s *directoryChunkStore) ShrinkChunkTailSlack(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error {
	path := chunkPath(id, domain.ChunkStateAvailable)

	// does the chunk exist?
	st, err := s.root.Stat(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("checking chunk existence: %w", err)
		}
		return fmt.Errorf("%w: %s", chunkerrors.ErrNotFound, id.String())
	}

	// is shrinking needed?
	maxFileSize := logicalSize + max(maxTailSlackSize, 0)
	if st.Size() <= maxFileSize {
		// nothing to do
		return nil
	}

	// shrink file
	f, err := s.root.OpenFile(path, os.O_RDWR, 0o666)
	if err != nil {
		return fmt.Errorf("opening chunk file: %w", err)
	}
	defer f.Close()

	if err := unix.Ftruncate(int(f.Fd()), maxFileSize); err != nil {
		return fmt.Errorf("truncating chunk file: %w", err)
	}

	// sync file
	if err := unix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("syncing chunk file: %w", err)
	}

	return nil
}
