package command

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	portremotechunknode "github.com/amari/mithril/chunk-node/port/remotechunknode"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
)

// --- Mock RemoteChunkClient ---

type mockRemoteChunkClient struct {
	readChunkRangeFunc func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error)
	statChunkFunc      func(ctx context.Context, chunkID domain.ChunkID) (*chunkv1.Chunk, *chunkv1.Volume, error)
}

var _ portremotechunknode.RemoteChunkClient = (*mockRemoteChunkClient)(nil)

func (m *mockRemoteChunkClient) ReadChunkRange(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
	if m.readChunkRangeFunc != nil {
		return m.readChunkRangeFunc(ctx, chunkID, offset, length)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *mockRemoteChunkClient) StatChunk(ctx context.Context, chunkID domain.ChunkID) (*chunkv1.Chunk, *chunkv1.Volume, error) {
	if m.statChunkFunc != nil {
		return m.statChunkFunc(ctx, chunkID)
	}
	return nil, nil, nil
}

// --- AppendFromChunk Test Helpers ---

type appendFromTestSetup struct {
	handler           *AppendFromChunkHandler
	repo              *mockChunkRepository
	chunkStore        *mockChunkStore
	healthChecker     *mockVolumeHealthChecker
	remoteChunkClient *mockRemoteChunkClient
}

func newAppendFromTestHandler(opts ...func(*appendFromTestOptions)) *appendFromTestSetup {
	o := &appendFromTestOptions{
		repo:              &mockChunkRepository{},
		chunkStore:        &mockChunkStore{},
		healthChecker:     &mockVolumeHealthChecker{},
		remoteChunkClient: &mockRemoteChunkClient{},
		volumeID:          domain.VolumeID(1),
		nowFunc:           func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	handler := &AppendFromChunkHandler{
		Repo:                o.repo,
		VolumeHealthChecker: o.healthChecker,
		VolumeManager:       volumeManager,
		RemoteChunkClient:   o.remoteChunkClient,
		NowFunc:             o.nowFunc,
	}

	return &appendFromTestSetup{
		handler:           handler,
		repo:              o.repo,
		chunkStore:        o.chunkStore,
		healthChecker:     o.healthChecker,
		remoteChunkClient: o.remoteChunkClient,
	}
}

type appendFromTestOptions struct {
	repo              *mockChunkRepository
	chunkStore        *mockChunkStore
	healthChecker     *mockVolumeHealthChecker
	remoteChunkClient *mockRemoteChunkClient
	volumeID          domain.VolumeID
	nowFunc           func() time.Time
}

func appendFromWithRepo(repo *mockChunkRepository) func(*appendFromTestOptions) {
	return func(o *appendFromTestOptions) { o.repo = repo }
}

func appendFromWithChunkStore(store *mockChunkStore) func(*appendFromTestOptions) {
	return func(o *appendFromTestOptions) { o.chunkStore = store }
}

func appendFromWithHealthChecker(checker *mockVolumeHealthChecker) func(*appendFromTestOptions) {
	return func(o *appendFromTestOptions) { o.healthChecker = checker }
}

func appendFromWithRemoteChunkClient(client *mockRemoteChunkClient) func(*appendFromTestOptions) {
	return func(o *appendFromTestOptions) { o.remoteChunkClient = client }
}

func appendFromWithVolumeID(id domain.VolumeID) func(*appendFromTestOptions) {
	return func(o *appendFromTestOptions) { o.volumeID = id }
}

func appendFromWithNowFunc(f func() time.Time) func(*appendFromTestOptions) {
	return func(o *appendFromTestOptions) { o.nowFunc = f }
}

// validRemoteChunkIDBytes returns a valid 16-byte remote chunk ID.
func validRemoteChunkIDBytes() []byte {
	id := domain.NewChunkID(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), 2, 5, 0)
	return id[:]
}

// --- Tests ---

func TestAppendFromChunkHandler_Success(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 3)
	remoteData := "remote-chunk-data"

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(remoteData)), nil
			},
		}),
	)

	output, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   3,
		MinTailSlackSize:  512,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkOffset: 0,
		RemoteChunkLength: int64(len(remoteData)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	if output.Chunk.Size != 1000+int64(len(remoteData)) {
		t.Errorf("expected size %d, got %d", 1000+len(remoteData), output.Chunk.Size)
	}

	if output.Chunk.Version != 4 {
		t.Errorf("expected version 4, got %d", output.Chunk.Version)
	}

	if string(output.Chunk.WriterKey) != "wk" {
		t.Errorf("expected writer key 'wk', got %q", output.Chunk.WriterKey)
	}

	if output.Chunk.CreatedAt != existing.CreatedAt {
		t.Errorf("expected createdAt preserved, got %v", output.Chunk.CreatedAt)
	}

	if output.Chunk.UpdatedAt != time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) {
		t.Errorf("expected updatedAt from NowFunc, got %v", output.Chunk.UpdatedAt)
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}

	if output.VolumeHealth.State != domain.VolumeStateOK {
		t.Errorf("expected volume state OK, got %v", output.VolumeHealth.State)
	}
}

func TestAppendFromChunkHandler_Success_VerifyAppendArgs(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 1)
	remoteData := "data-from-remote"

	var (
		appendedID          domain.ChunkID
		appendedLogicalSize int64
		appendedBodySize    int64
		appendedSlack       int64
		appendedBody        []byte
	)

	chunkStore := &mockChunkStore{
		appendChunkFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error {
			appendedID = id
			appendedLogicalSize = logicalSize
			appendedBodySize = n
			appendedSlack = minTailSlackSize
			appendedBody, _ = io.ReadAll(r)
			return nil
		},
	}

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithChunkStore(chunkStore),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(remoteData)), nil
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		MinTailSlackSize:  2048,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkOffset: 100,
		RemoteChunkLength: int64(len(remoteData)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if appendedID != existing.ID {
		t.Errorf("expected chunk ID %v, got %v", existing.ID, appendedID)
	}

	if appendedLogicalSize != 500 {
		t.Errorf("expected logical size 500, got %d", appendedLogicalSize)
	}

	if appendedBodySize != int64(len(remoteData)) {
		t.Errorf("expected body size %d, got %d", len(remoteData), appendedBodySize)
	}

	if appendedSlack != 2048 {
		t.Errorf("expected slack 2048, got %d", appendedSlack)
	}

	if string(appendedBody) != remoteData {
		t.Errorf("expected body %q, got %q", remoteData, appendedBody)
	}
}

func TestAppendFromChunkHandler_Success_VerifyRemoteReadArgs(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 1)
	remoteIDBytes := validRemoteChunkIDBytes()

	var (
		readID     domain.ChunkID
		readOffset int64
		readLength int64
	)

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				readID = chunkID
				readOffset = offset
				readLength = length
				return io.NopCloser(strings.NewReader("x")), nil
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		RemoteChunkID:     remoteIDBytes,
		RemoteChunkOffset: 42,
		RemoteChunkLength: 1024,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var expectedID domain.ChunkID
	copy(expectedID[:], remoteIDBytes)

	if readID != expectedID {
		t.Errorf("expected remote chunk ID %v, got %v", expectedID, readID)
	}

	if readOffset != 42 {
		t.Errorf("expected remote offset 42, got %d", readOffset)
	}

	if readLength != 1024 {
		t.Errorf("expected remote length 1024, got %d", readLength)
	}
}

func TestAppendFromChunkHandler_InvalidRemoteChunkID(t *testing.T) {
	setup := newAppendFromTestHandler()

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:      []byte("wk"),
		RemoteChunkID: []byte("too-short"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrInvalidID) {
		t.Errorf("expected ErrInvalidID, got %v", err)
	}
}

func TestAppendFromChunkHandler_WriterKeyNotFound(t *testing.T) {
	setup := newAppendFromTestHandler() // default repo returns ErrNotFound

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("nonexistent"),
		ExpectedVersion: 1,
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestAppendFromChunkHandler_WrongState_TempChunk(t *testing.T) {
	temp := &domain.TempChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	setup := newAppendFromTestHandler(appendFromWithRepo(repoReturningChunk(temp)))

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestAppendFromChunkHandler_WrongState_DeletedChunk(t *testing.T) {
	deleted := &domain.DeletedChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	setup := newAppendFromTestHandler(appendFromWithRepo(repoReturningChunk(deleted)))

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestAppendFromChunkHandler_VersionMismatch(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 5)

	setup := newAppendFromTestHandler(appendFromWithRepo(repoReturningChunk(existing)))

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 3, // actual is 5
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrVersionMismatch) {
		t.Errorf("expected ErrVersionMismatch, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context")
	}

	if chunkErr.ChunkVersion() != 5 {
		t.Errorf("expected chunk version 5 in error, got %d", chunkErr.ChunkVersion())
	}

	if chunkErr.ChunkSize() != 1000 {
		t.Errorf("expected chunk size 1000 in error, got %d", chunkErr.ChunkSize())
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatal("expected StateError context")
	}
}

func TestAppendFromChunkHandler_VolumeNotFound(t *testing.T) {
	existing := &domain.AvailableChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
		WriterKey: []byte("wk"),
		Size:      100,
		Version:   1,
	}

	setup := newAppendFromTestHandler(appendFromWithRepo(repoReturningChunk(existing)))

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrNotFound) {
		t.Errorf("expected volume ErrNotFound, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context on volume not found")
	}
}

func TestAppendFromChunkHandler_VolumeDegraded(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrDegraded) {
		t.Errorf("expected ErrDegraded, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context")
	}
}

func TestAppendFromChunkHandler_VolumeFailed(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateFailed}
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrFailed) {
		t.Errorf("expected ErrFailed, got %v", err)
	}
}

func TestAppendFromChunkHandler_RemoteReadError(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)
	remoteErr := errors.New("remote node unavailable")

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return nil, remoteErr
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkOffset: 0,
		RemoteChunkLength: 100,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, remoteErr) {
		t.Errorf("expected remote error, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context on remote read error")
	}
}

func TestAppendFromChunkHandler_AppendChunkStoreError(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)
	diskErr := errors.New("disk I/O error")

	chunkStore := &mockChunkStore{
		appendChunkFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error {
			return diskErr
		},
	}

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithChunkStore(chunkStore),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("data")), nil
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkOffset: 0,
		RemoteChunkLength: 4,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, diskErr) {
		t.Errorf("expected disk I/O error, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context")
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatal("expected StateError context")
	}
}

func TestAppendFromChunkHandler_RepoStoreError(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)
	repoErr := errors.New("storage backend unavailable")

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
		storeFunc: func(ctx context.Context, chunk domain.Chunk) error {
			return repoErr
		},
	}

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repo),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("data")), nil
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkOffset: 0,
		RemoteChunkLength: 4,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context")
	}

	// Error should reference the OLD version/size (pre-append)
	if chunkErr.ChunkVersion() != 1 {
		t.Errorf("expected version 1 in error, got %d", chunkErr.ChunkVersion())
	}

	if chunkErr.ChunkSize() != 1000 {
		t.Errorf("expected size 1000 in error, got %d", chunkErr.ChunkSize())
	}
}

func TestAppendFromChunkHandler_RepoLookupError_Passthrough(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	setup := newAppendFromTestHandler(appendFromWithRepo(repo))

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		RemoteChunkID:   validRemoteChunkIDBytes(),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}

	// Repo lookup errors are NOT wrapped with ChunkError (no chunk context yet)
	var chunkErr *chunkerrors.ChunkError
	if errors.As(err, &chunkErr) {
		t.Error("repo lookup error should NOT have ChunkError context")
	}
}
