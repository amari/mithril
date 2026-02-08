package command

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

// --- Append Test Helpers ---

type appendTestSetup struct {
	handler       *AppendChunkHandler
	repo          *mockChunkRepository
	chunkStore    *mockChunkStore
	healthChecker *mockVolumeHealthChecker
}

func newAppendTestHandler(opts ...func(*appendTestOptions)) *appendTestSetup {
	o := &appendTestOptions{
		repo:          &mockChunkRepository{},
		chunkStore:    &mockChunkStore{},
		healthChecker: &mockVolumeHealthChecker{},
		volumeID:      domain.VolumeID(1),
		nowFunc:       func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	handler := &AppendChunkHandler{
		Repo:                o.repo,
		VolumeHealthChecker: o.healthChecker,
		VolumeManager:       volumeManager,
		NowFunc:             o.nowFunc,
	}

	return &appendTestSetup{
		handler:       handler,
		repo:          o.repo,
		chunkStore:    o.chunkStore,
		healthChecker: o.healthChecker,
	}
}

type appendTestOptions struct {
	repo          *mockChunkRepository
	chunkStore    *mockChunkStore
	healthChecker *mockVolumeHealthChecker
	volumeID      domain.VolumeID
	nowFunc       func() time.Time
}

func appendWithRepo(repo *mockChunkRepository) func(*appendTestOptions) {
	return func(o *appendTestOptions) { o.repo = repo }
}

func appendWithChunkStore(store *mockChunkStore) func(*appendTestOptions) {
	return func(o *appendTestOptions) { o.chunkStore = store }
}

func appendWithHealthChecker(checker *mockVolumeHealthChecker) func(*appendTestOptions) {
	return func(o *appendTestOptions) { o.healthChecker = checker }
}

func appendWithVolumeID(id domain.VolumeID) func(*appendTestOptions) {
	return func(o *appendTestOptions) { o.volumeID = id }
}

func appendWithNowFunc(f func() time.Time) func(*appendTestOptions) {
	return func(o *appendTestOptions) { o.nowFunc = f }
}

// makeAvailableChunk creates an AvailableChunk on volumeID 1 with sensible defaults.
func makeAvailableChunk(writerKey []byte, size int64, version uint64) *domain.AvailableChunk {
	return &domain.AvailableChunk{
		ID:        domain.NewChunkID(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 1, 0),
		WriterKey: writerKey,
		Size:      size,
		Version:   version,
		CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func repoReturningChunk(c domain.Chunk) *mockChunkRepository {
	return &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return c, nil
		},
	}
}

// --- Tests ---

func TestAppendChunkHandler_Success(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 3)
	body := []byte("hello world")

	setup := newAppendTestHandler(
		appendWithRepo(repoReturningChunk(existing)),
	)

	output, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  3,
		MinTailSlackSize: 512,
		Body:             bytes.NewReader(body),
		BodySize:         int64(len(body)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk.Size != 1000+int64(len(body)) {
		t.Errorf("expected size %d, got %d", 1000+len(body), output.Chunk.Size)
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

func TestAppendChunkHandler_Success_VerifyAppendArgs(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 1)
	body := []byte("data")

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

	setup := newAppendTestHandler(
		appendWithRepo(repoReturningChunk(existing)),
		appendWithChunkStore(chunkStore),
	)

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  1,
		MinTailSlackSize: 2048,
		Body:             bytes.NewReader(body),
		BodySize:         int64(len(body)),
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

	if appendedBodySize != int64(len(body)) {
		t.Errorf("expected body size %d, got %d", len(body), appendedBodySize)
	}

	if appendedSlack != 2048 {
		t.Errorf("expected slack 2048, got %d", appendedSlack)
	}

	if !bytes.Equal(appendedBody, body) {
		t.Errorf("expected body %q, got %q", body, appendedBody)
	}
}

func TestAppendChunkHandler_WriterKeyNotFound(t *testing.T) {
	setup := newAppendTestHandler() // default repo returns ErrNotFound

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("nonexistent"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestAppendChunkHandler_WrongState_TempChunk(t *testing.T) {
	temp := &domain.TempChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	setup := newAppendTestHandler(appendWithRepo(repoReturningChunk(temp)))

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestAppendChunkHandler_WrongState_DeletedChunk(t *testing.T) {
	deleted := &domain.DeletedChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	setup := newAppendTestHandler(appendWithRepo(repoReturningChunk(deleted)))

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestAppendChunkHandler_VersionMismatch(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 5)

	setup := newAppendTestHandler(appendWithRepo(repoReturningChunk(existing)))

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 3, // actual is 5
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrVersionMismatch) {
		t.Errorf("expected ErrVersionMismatch, got %v", err)
	}

	// Verify ChunkError context
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

	// Verify StateError context
	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatal("expected StateError context")
	}
}

func TestAppendChunkHandler_VolumeNotFound(t *testing.T) {
	// Chunk on volume 99 which isn't registered in VolumeManager
	existing := &domain.AvailableChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
		WriterKey: []byte("wk"),
		Size:      100,
		Version:   1,
	}

	setup := newAppendTestHandler(appendWithRepo(repoReturningChunk(existing)))

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// Should still wrap with ChunkError
	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context on volume not found")
	}
}

func TestAppendChunkHandler_VolumeDegraded(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	setup := newAppendTestHandler(
		appendWithRepo(repoReturningChunk(existing)),
		appendWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
			},
		}),
	)

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader(nil),
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

func TestAppendChunkHandler_VolumeFailed(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	setup := newAppendTestHandler(
		appendWithRepo(repoReturningChunk(existing)),
		appendWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateFailed}
			},
		}),
	)

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrFailed) {
		t.Errorf("expected ErrFailed, got %v", err)
	}
}

func TestAppendChunkHandler_AppendChunkStoreError(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)
	diskErr := errors.New("disk I/O error")

	chunkStore := &mockChunkStore{
		appendChunkFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error {
			return diskErr
		},
	}

	setup := newAppendTestHandler(
		appendWithRepo(repoReturningChunk(existing)),
		appendWithChunkStore(chunkStore),
	)

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader([]byte("data")),
		BodySize:        4,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, diskErr) {
		t.Errorf("expected disk I/O error, got %v", err)
	}

	// Should have both ChunkError and StateError context
	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context")
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatal("expected StateError context")
	}
}

func TestAppendChunkHandler_RepoStoreError(t *testing.T) {
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

	setup := newAppendTestHandler(appendWithRepo(repo))

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader([]byte("data")),
		BodySize:        4,
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

func TestAppendChunkHandler_ZeroBodySize(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 2)

	setup := newAppendTestHandler(appendWithRepo(repoReturningChunk(existing)))

	output, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 2,
		Body:            bytes.NewReader(nil),
		BodySize:        0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk.Size != 500 {
		t.Errorf("expected size 500, got %d", output.Chunk.Size)
	}

	if output.Chunk.Version != 3 {
		t.Errorf("expected version 3, got %d", output.Chunk.Version)
	}
}

func TestAppendChunkHandler_VersionMismatch_IncludesVolumeState(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 5)

	setup := newAppendTestHandler(
		appendWithRepo(repoReturningChunk(existing)),
		appendWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
			},
		}),
	)

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 3,
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	// Version mismatch should still include volume state
	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatal("expected StateError context")
	}

	if stateErr.State() != domain.VolumeStateDegraded {
		t.Errorf("expected degraded state in error, got %v", stateErr.State())
	}
}

func TestAppendChunkHandler_RepoError_Passthrough(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	setup := newAppendTestHandler(appendWithRepo(repo))

	_, err := setup.handler.HandleAppendChunk(context.Background(), &AppendChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
		Body:            bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	// Repo lookup errors are NOT wrapped with ChunkError (no chunk context yet)
	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if errors.As(err, &chunkErr) {
		t.Error("repo lookup error should NOT have ChunkError context")
	}
}
