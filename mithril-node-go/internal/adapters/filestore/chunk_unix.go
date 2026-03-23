//go:build unix
// +build unix

package adaptersfilestore

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

	adaptersfilesystem "github.com/amari/mithril/mithril-node-go/internal/adapters/filesystem"
	adaptersunix "github.com/amari/mithril/mithril-node-go/internal/adapters/unix"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type SysChunkHandle struct {
	file *os.File
	id   domain.ChunkID
}

var _ domain.ChunkHandle = (*SysChunkHandle)(nil)

func (h *SysChunkHandle) Close() error {
	if err := h.file.Close(); err != nil {
		// FIXME
		return err
	}
	return nil
}

func (h *SysChunkHandle) OpenReader(ctx context.Context) (domain.ChunkReader, error) {
	return io.NopCloser(h.file), nil
}

func (h *SysChunkHandle) OpenRangeReader(ctx context.Context, offset, length int64) (domain.ChunkRangeReader, error) {
	return io.NopCloser(io.NewSectionReader(h.file, offset, length)), nil
}

func (h *SysChunkHandle) OpenReaderAt(ctx context.Context) (domain.ChunkReaderAt, error) {
	return &SysChunkReaderAt{
		File: h.file,
	}, nil
}

type SysChunkReaderAt struct {
	*os.File
}

func (f *SysChunkReaderAt) Close() error {
	return nil
}

var _ domain.ChunkReaderAt = (*SysChunkReaderAt)(nil)

type SysChunkStorage struct {
	root         *adaptersfilesystem.Root
	writeBufPool *sync.Pool
}

var _ domain.ChunkStorage = (*SysChunkStorage)(nil)

func NewSysChunkStorage(root *adaptersfilesystem.Root, bufferSize int) (*SysChunkStorage, error) {
	fdSet := adaptersfilesystem.NewFdSet()

	_, err := root.Stat("chunks")
	if err != nil {
		if err := root.MkdirWithFdSet("chunks", 0o700, fdSet); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFSMkdirFailed, err)
		}

		fdSet := adaptersfilesystem.NewFdSet()

		if err := root.MkdirWithFdSet("chunks", 0o755, fdSet); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFSMkdirFailed, err)
		}

		if err := fdSet.Flush(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
		}

		if err := fdSet.Close(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFSCloseFailed, err)
		}
	}

	newRoot, err := root.OpenRoot("chunks")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}

	// sync directory tree
	if err := fdSet.Flush(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	// close directory tree files
	if err := fdSet.Close(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFSCloseFailed, err)
	}

	return &SysChunkStorage{
		root: newRoot,
		writeBufPool: &sync.Pool{
			New: func() any {
				return make([]byte, max(bufferSize, 32*1024))
			},
		},
	}, nil
}

func (s *SysChunkStorage) start() error {
	return nil
}

func (s *SysChunkStorage) stop() error {
	s.root.Close()

	return nil
}

func chunkPath(id domain.ChunkID, state domain.ChunkState) string {
	timestamp := id.UnixMilli()
	sequence := id.Sequence()

	var bytes [10]byte
	binary.BigEndian.PutUint64(bytes[0:8], uint64(timestamp))
	binary.BigEndian.PutUint16(bytes[8:10], sequence)

	var b strings.Builder

	switch state {
	case domain.ChunkStatePending:
		b.WriteString("pending")
	case domain.ChunkStateReady:
		b.WriteString("ready")
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

func (s *SysChunkStorage) Open(ctx context.Context, id domain.ChunkID) (domain.ChunkHandle, error) {
	path := chunkPath(id, domain.ChunkStateReady)

	f, err := s.root.OpenFile(path, os.O_RDONLY, 0o600)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
		}

		return nil, domain.ErrChunkNotFound
	}

	zerolog.Ctx(ctx).Info().Str("chunk.file.path", path).Msg("opened chunk")

	return &SysChunkHandle{
		file: f,
	}, nil
}

func (s *SysChunkStorage) Create(ctx context.Context, id domain.ChunkID, opts domain.CreateChunkOptions) error {
	path := chunkPath(id, domain.ChunkStateReady)

	_, err := s.root.Stat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %w", ErrFSStatFailed, err)
		}
	} else {
		// Don't clobber existing file
		return domain.ErrChunkAlreadyExists
	}

	// create necessary directories
	fdSet := adaptersfilesystem.NewFdSet()

	err = s.root.MkdirAllWithFdSet(filepath.Dir(path), 0o700, fdSet)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSMkdirFailed, err)
	}

	// create file
	f, err := s.root.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer f.Close()

	// reserve space
	fileSize := max(0, opts.MinTailSlackLength)
	if fileSize > 0 {
		if err := adaptersunix.Fallocate(int(f.Fd()), 0, 0, fileSize); err != nil {
			return fmt.Errorf("%w: %w", ErrFSFallocateFailed, err)
		}
	}

	// sync file
	if err := adaptersunix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	// sync directory tree
	if err := fdSet.Flush(); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	// close directory tree files
	if err := fdSet.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrFSCloseFailed, err)
	}

	return nil
}

func (s *SysChunkStorage) Put(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, opts domain.PutChunkOptions) error {
	firstPath := chunkPath(id, domain.ChunkStatePending)
	finalPath := chunkPath(id, domain.ChunkStateReady)

	// does the chunk exist already?
	_, err := s.root.Stat(firstPath)
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: %w", ErrFSStatFailed, err)
	}

	_, err = s.root.Stat(finalPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %w", ErrFSStatFailed, err)
		}
	} else {
		// Don't clobber existing file
		return domain.ErrChunkAlreadyExists
	}

	// create necessary directories
	fdSet := adaptersfilesystem.NewFdSet()

	err = s.root.MkdirAllWithFdSet(filepath.Dir(firstPath), 0o700, fdSet)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSMkdirFailed, err)
	}

	err = s.root.MkdirAllWithFdSet(filepath.Dir(finalPath), 0o700, fdSet)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSMkdirFailed, err)
	}

	// create file
	f, err := s.root.OpenFile(firstPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer f.Close()

	// reserve space
	fileSize := max(n, 0) + max(0, opts.MinTailSlackLength)
	if fileSize > 0 {
		if err := adaptersunix.Fallocate(int(f.Fd()), 0, 0, fileSize); err != nil {
			return fmt.Errorf("%w: %w", ErrFSFallocateFailed, err)
		}
	}

	// write file
	buf := s.writeBufPool.Get().([]byte)
	defer s.writeBufPool.Put(buf)

	written, err := io.CopyBuffer(f, r, buf)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSWriteFailed, err)
	}
	if written != n {
		return domain.ErrChunkWriteIncomplete
	}

	// sync file
	if err := adaptersunix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	// rename to final path
	if err := s.root.Rename(firstPath, finalPath); err != nil {
		return fmt.Errorf("%w: %w", ErrFSRenameFailed, err)
	}

	// sync directory tree
	if err := fdSet.Flush(); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	// close directory tree files
	if err := fdSet.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrFSCloseFailed, err)
	}

	return nil
}

func (s *SysChunkStorage) Append(ctx context.Context, id domain.ChunkID, knownSize int64, r io.Reader, n int64, opts domain.AppendChunkOptions) error {
	path := chunkPath(id, domain.ChunkStateReady)

	// does the chunk exist?
	st, err := s.root.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return domain.ErrChunkCorrupted
		}

		return fmt.Errorf("%w: %w", ErrFSStatFailed, err)
	}

	if st.Size() < knownSize {
		return domain.ErrChunkCorrupted
	}

	// open file
	f, err := s.root.OpenFile(path, os.O_RDWR, 0o666)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer f.Close()

	// reserve space if needed
	minFileSize := knownSize + max(n, 0) + max(0, opts.MinTailSlackLength)
	if st.Size() < minFileSize {
		if err := adaptersunix.Fallocate(int(f.Fd()), 0, st.Size(), minFileSize-st.Size()); err != nil {
			return fmt.Errorf("%w: %w", ErrFSFallocateFailed, err)
		}
	}

	// seek to logical end
	_, err = f.Seek(knownSize, io.SeekStart)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSSeekFailed, err)
	}

	// write data
	buf := s.writeBufPool.Get().([]byte)
	defer s.writeBufPool.Put(buf)

	written, err := io.CopyBuffer(f, r, buf)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSWriteFailed, err)
	}
	if written != n {
		return domain.ErrChunkWriteIncomplete
	}

	// sync file
	if err := adaptersunix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	return nil
}

func (s *SysChunkStorage) Delete(ctx context.Context, id domain.ChunkID) error {
	firstPath := chunkPath(id, domain.ChunkStateReady)
	finalPath := chunkPath(id, domain.ChunkStateDeleted)

	// does the chunk exist?
	_, err := s.root.Stat(firstPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return domain.ErrChunkCorrupted
		}

		return fmt.Errorf("%w: %w", ErrFSStatFailed, err)
	}

	// create necessary directories
	fdSet := adaptersfilesystem.NewFdSet()

	// create dest directory
	err = s.root.MkdirAllWithFdSet(filepath.Dir(finalPath), 0o700, fdSet)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSMkdirFailed, err)
	}

	// open src directory
	dirF, err := s.root.OpenFile(filepath.Dir(firstPath), os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer dirF.Close()

	// rename file to final path
	if err := s.root.Rename(firstPath, finalPath); err != nil {
		return fmt.Errorf("%w: %w", ErrFSRenameFailed, err)
	}

	// sync directory tree
	if err := fdSet.Flush(); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	// close directory tree files
	if err := fdSet.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrFSCloseFailed, err)
	}

	return nil
}

func (s *SysChunkStorage) ShrinkToFit(ctx context.Context, id domain.ChunkID, knownSize int64, opts domain.ShrinkChunkToFitOptions) error {
	path := chunkPath(id, domain.ChunkStateReady)

	// does the chunk exist?
	st, err := s.root.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return domain.ErrChunkCorrupted
		}

		return fmt.Errorf("%w: %w", ErrFSStatFailed, err)
	}

	// is shrinking needed?
	maxFileSize := knownSize + max(0, opts.MaxTailSlackLength)
	if st.Size() <= maxFileSize {
		// nothing to do
		return nil
	}

	// shrink file
	f, err := s.root.OpenFile(path, os.O_RDWR, 0o666)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer f.Close()

	if err := adaptersunix.Ftruncate(int(f.Fd()), maxFileSize); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFtruncateFailed, err)
	}

	// sync file
	if err := adaptersunix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	return nil
}

func (s *SysChunkStorage) Exists(ctx context.Context, id domain.ChunkID) (bool, error) {
	path := chunkPath(id, domain.ChunkStateReady)

	st, err := s.root.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("%w: %w", ErrFSStatFailed, err)
	}

	if !st.Mode().IsRegular() {
		return false, domain.ErrChunkCorrupted
	}

	return true, nil
}
