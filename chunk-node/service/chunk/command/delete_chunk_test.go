package command

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

// --- Delete Test Helpers ---

type deleteTestSetup struct {
	handler       *DeleteChunkHandler
	repo          *mockChunkRepository
	chunkStore    *mockChunkStore
	healthChecker *mockVolumeHealthChecker
}

func newDeleteTestHandler(opts ...func(*deleteTestOptions)) *deleteTestSetup {
	o := &deleteTestOptions{
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

	handler := &DeleteChunkHandler{
		Repo:                o.repo,
		VolumeHealthChecker: o.healthChecker,
		VolumeManager:       volumeManager,
		NowFunc:             o.nowFunc,
	}

	return &deleteTestSetup{
		handler:       handler,
		repo:          o.repo,
		chunkStore:    o.chunkStore,
		healthChecker: o.healthChecker,
	}
}

type deleteTestOptions struct {
	repo          *mockChunkRepository
	chunkStore    *mockChunkStore
	healthChecker *mockVolumeHealthChecker
	volumeID      domain.VolumeID
	nowFunc       func() time.Time
}

func deleteWithRepo(repo *mockChunkRepository) func(*deleteTestOptions) {
	return func(o *deleteTestOptions) { o.repo = repo }
}

func deleteWithChunkStore(store *mockChunkStore) func(*deleteTestOptions) {
	return func(o *deleteTestOptions) { o.chunkStore = store }
}

func deleteWithHealthChecker(checker *mockVolumeHealthChecker) func(*deleteTestOptions) {
	return func(o *deleteTestOptions) { o.healthChecker = checker }
}

func deleteWithVolumeID(id domain.VolumeID) func(*deleteTestOptions) {
	return func(o *deleteTestOptions) { o.volumeID = id }
}

func deleteWithNowFunc(f func() time.Time) func(*deleteTestOptions) {
	return func(o *deleteTestOptions) { o.nowFunc = f }
}

// --- Tests ---

func TestDeleteChunkHandler_Success(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 3)
	fixedNow := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	var deletedFromStoreID domain.ChunkID
	chunkStore := &mockChunkStore{
		deleteChunkFunc: func(ctx context.Context, id domain.ChunkID) error {
			deletedFromStoreID = id
			return nil
		},
	}

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
		deleteWithChunkStore(chunkStore),
		deleteWithNowFunc(func() time.Time { return fixedNow }),
	)

	output, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected deleted chunk in output")
	}

	if output.Chunk.ID != existing.ID {
		t.Errorf("expected chunk ID %v, got %v", existing.ID, output.Chunk.ID)
	}

	if string(output.Chunk.WriterKey) != "wk" {
		t.Errorf("expected writer key 'wk', got %q", output.Chunk.WriterKey)
	}

	if output.Chunk.DeletedAt != fixedNow {
		t.Errorf("expected deletedAt %v, got %v", fixedNow, output.Chunk.DeletedAt)
	}

	// Verify the chunk was deleted from the store with correct ID
	if deletedFromStoreID != existing.ID {
		t.Errorf("expected store delete for chunk %v, got %v", existing.ID, deletedFromStoreID)
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}

	if output.VolumeHealth.State != domain.VolumeStateOK {
		t.Errorf("expected volume state OK, got %v", output.VolumeHealth.State)
	}
}

func TestDeleteChunkHandler_Success_RepoStoresCalled(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 1)

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
	)

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify a DeletedChunk was stored in the repo
	if len(setup.repo.storedChunks) != 1 {
		t.Fatalf("expected 1 stored chunk, got %d", len(setup.repo.storedChunks))
	}

	stored := setup.repo.storedChunks[0]
	deleted, ok := stored.AsDeleted()
	if !ok {
		t.Fatal("expected stored chunk to be DeletedChunk")
	}

	if deleted.ID != existing.ID {
		t.Errorf("expected stored chunk ID %v, got %v", existing.ID, deleted.ID)
	}

	if string(deleted.WriterKey) != "wk" {
		t.Errorf("expected stored writer key 'wk', got %q", deleted.WriterKey)
	}
}

func TestDeleteChunkHandler_WriterKeyNotFound(t *testing.T) {
	setup := newDeleteTestHandler() // default repo returns ErrNotFound

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("nonexistent"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// Repo lookup errors should NOT be wrapped with ChunkError
	var chunkErr *chunkerrors.ChunkError
	if errors.As(err, &chunkErr) {
		t.Error("repo lookup error should NOT have ChunkError context")
	}
}

func TestDeleteChunkHandler_WrongState_TempChunk(t *testing.T) {
	temp := &domain.TempChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	setup := newDeleteTestHandler(deleteWithRepo(repoReturningChunk(temp)))

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestDeleteChunkHandler_WrongState_DeletedChunk(t *testing.T) {
	alreadyDeleted := &domain.DeletedChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
		DeletedAt: time.Now(),
	}

	setup := newDeleteTestHandler(deleteWithRepo(repoReturningChunk(alreadyDeleted)))

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestDeleteChunkHandler_VolumeNotFound(t *testing.T) {
	// Chunk on volume 99 which is not registered in VolumeManager
	existing := &domain.AvailableChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
		WriterKey: []byte("wk"),
		Size:      100,
		Version:   1,
	}

	setup := newDeleteTestHandler(deleteWithRepo(repoReturningChunk(existing)))

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrNotFound) {
		t.Errorf("expected volume ErrNotFound, got %v", err)
	}
}

func TestDeleteChunkHandler_VolumeDegraded(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
		deleteWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
			},
		}),
	)

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrDegraded) {
		t.Errorf("expected ErrDegraded, got %v", err)
	}

	// Admission errors should be wrapped with ChunkError
	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context on degraded volume")
	}

	if chunkErr.ChunkVersion() != 1 {
		t.Errorf("expected chunk version 1 in error, got %d", chunkErr.ChunkVersion())
	}

	if chunkErr.ChunkSize() != 1000 {
		t.Errorf("expected chunk size 1000 in error, got %d", chunkErr.ChunkSize())
	}
}

func TestDeleteChunkHandler_VolumeFailed(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 2)

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
		deleteWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateFailed}
			},
		}),
	)

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrFailed) {
		t.Errorf("expected ErrFailed, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context on failed volume")
	}
}

func TestDeleteChunkHandler_ChunkStoreDeleteError(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)
	diskErr := errors.New("disk I/O error on delete")

	chunkStore := &mockChunkStore{
		deleteChunkFunc: func(ctx context.Context, id domain.ChunkID) error {
			return diskErr
		},
	}

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
		deleteWithChunkStore(chunkStore),
	)

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, diskErr) {
		t.Errorf("expected disk I/O error, got %v", err)
	}
}

func TestDeleteChunkHandler_ChunkStoreDeleteNotFound_Tolerated(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	chunkStore := &mockChunkStore{
		deleteChunkFunc: func(ctx context.Context, id domain.ChunkID) error {
			return chunkerrors.ErrNotFound // chunk already gone from disk
		},
	}

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
		deleteWithChunkStore(chunkStore),
	)

	output, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("expected ErrNotFound from store to be tolerated, got: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected deleted chunk in output")
	}

	if output.Chunk.ID != existing.ID {
		t.Errorf("expected chunk ID %v, got %v", existing.ID, output.Chunk.ID)
	}
}

func TestDeleteChunkHandler_RepoStoreError(t *testing.T) {
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

	setup := newDeleteTestHandler(deleteWithRepo(repo))

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}
}

func TestDeleteChunkHandler_RepoStoreNotFound_Tolerated(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
		storeFunc: func(ctx context.Context, chunk domain.Chunk) error {
			return chunkerrors.ErrNotFound
		},
	}

	setup := newDeleteTestHandler(deleteWithRepo(repo))

	output, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("expected ErrNotFound from repo store to be tolerated, got: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected deleted chunk in output")
	}
}

func TestDeleteChunkHandler_RepoLookupError_Passthrough(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	setup := newDeleteTestHandler(deleteWithRepo(repo))

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}

	// Repo lookup errors should NOT be wrapped with ChunkError (no chunk context yet)
	var chunkErr *chunkerrors.ChunkError
	if errors.As(err, &chunkErr) {
		t.Error("repo lookup error should NOT have ChunkError context")
	}
}
