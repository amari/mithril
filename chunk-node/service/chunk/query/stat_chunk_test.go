package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
)

// --- Stat Test Helpers ---

type statTestSetup struct {
	handler       *StatChunkHandler
	healthChecker *mockVolumeHealthChecker
}

func newStatTestHandler(opts ...func(*statTestOptions)) *statTestSetup {
	o := &statTestOptions{
		repo:          &mockChunkRepository{},
		healthChecker: &mockVolumeHealthChecker{},
	}

	for _, opt := range opts {
		opt(o)
	}

	handler := &StatChunkHandler{
		Repo:                o.repo,
		VolumeHealthChecker: o.healthChecker,
	}

	return &statTestSetup{
		handler:       handler,
		healthChecker: o.healthChecker,
	}
}

type statTestOptions struct {
	repo          *mockChunkRepository
	healthChecker *mockVolumeHealthChecker
}

func statWithRepo(repo *mockChunkRepository) func(*statTestOptions) {
	return func(o *statTestOptions) { o.repo = repo }
}

func statWithHealthChecker(checker *mockVolumeHealthChecker) func(*statTestOptions) {
	return func(o *statTestOptions) { o.healthChecker = checker }
}

// --- Tests ---

func TestStatChunkHandler_Success_ByChunkID(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 3)

	repo := &mockChunkRepository{
		getFunc: func(ctx context.Context, id domain.ChunkID) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newStatTestHandler(statWithRepo(repo))

	output, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		ChunkID: existing.ID[:],
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	if output.Chunk.Size != 1000 {
		t.Errorf("expected size 1000, got %d", output.Chunk.Size)
	}

	if output.Chunk.Version != 3 {
		t.Errorf("expected version 3, got %d", output.Chunk.Version)
	}

	if string(output.Chunk.WriterKey) != "wk" {
		t.Errorf("expected writer key 'wk', got %q", output.Chunk.WriterKey)
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}

	if output.VolumeHealth.State != domain.VolumeStateOK {
		t.Errorf("expected volume state OK, got %v", output.VolumeHealth.State)
	}
}

func TestStatChunkHandler_Success_ByWriterKey(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 1)

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newStatTestHandler(statWithRepo(repo))

	output, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		WriterKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	if output.Chunk.Size != 500 {
		t.Errorf("expected size 500, got %d", output.Chunk.Size)
	}
}

func TestStatChunkHandler_Success_ChunkIDTakesPrecedence(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 3)

	var getByIDCalled, getByWriterKeyCalled bool

	repo := &mockChunkRepository{
		getFunc: func(ctx context.Context, id domain.ChunkID) (domain.Chunk, error) {
			getByIDCalled = true
			return existing, nil
		},
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			getByWriterKeyCalled = true
			return existing, nil
		},
	}

	setup := newStatTestHandler(statWithRepo(repo))

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		ChunkID:   existing.ID[:],
		WriterKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !getByIDCalled {
		t.Error("expected Get (by ID) to be called")
	}

	if getByWriterKeyCalled {
		t.Error("expected GetByWriterKey NOT to be called when ChunkID is provided")
	}
}

func TestStatChunkHandler_InvalidChunkID(t *testing.T) {
	setup := newStatTestHandler()

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		ChunkID: []byte("too-short"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrInvalidID) {
		t.Errorf("expected ErrInvalidID, got %v", err)
	}
}

func TestStatChunkHandler_ChunkIDNotFound(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)

	setup := newStatTestHandler() // default repo.Get returns ErrNotFound

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		ChunkID: chunkID[:],
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStatChunkHandler_WriterKeyNotFound(t *testing.T) {
	setup := newStatTestHandler() // default repo.GetByWriterKey returns ErrNotFound

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		WriterKey: []byte("nonexistent"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStatChunkHandler_NeitherProvided(t *testing.T) {
	setup := newStatTestHandler()

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWriterKeyMismatch) {
		t.Errorf("expected ErrWriterKeyMismatch, got %v", err)
	}
}

func TestStatChunkHandler_WrongState_TempChunk(t *testing.T) {
	temp := &domain.TempChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return temp, nil
		},
	}

	setup := newStatTestHandler(statWithRepo(repo))

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestStatChunkHandler_WrongState_DeletedChunk(t *testing.T) {
	deleted := &domain.DeletedChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return deleted, nil
		},
	}

	setup := newStatTestHandler(statWithRepo(repo))

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestStatChunkHandler_VolumeDegraded_StillReturns(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newStatTestHandler(
		statWithRepo(repo),
		statWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
			},
		}),
	)

	output, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		WriterKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("stat should succeed on degraded volumes, got error: %v", err)
	}

	if output.VolumeHealth.State != domain.VolumeStateDegraded {
		t.Errorf("expected degraded volume health in output, got %v", output.VolumeHealth.State)
	}
}

func TestStatChunkHandler_VolumeFailed_StillReturns(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newStatTestHandler(
		statWithRepo(repo),
		statWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateFailed}
			},
		}),
	)

	output, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		WriterKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("stat should succeed even on failed volumes, got error: %v", err)
	}

	if output.VolumeHealth.State != domain.VolumeStateFailed {
		t.Errorf("expected failed volume health in output, got %v", output.VolumeHealth.State)
	}
}

func TestStatChunkHandler_RepoLookupError_Passthrough(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	setup := newStatTestHandler(statWithRepo(repo))

	_, err := setup.handler.HandleStatChunk(context.Background(), &StatChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}
}
