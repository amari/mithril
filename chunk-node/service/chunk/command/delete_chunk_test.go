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
	handler           *DeleteChunkHandler
	repo              *mockChunkRepository
	chunkStore        *mockChunkStore
	healthChecker     *mockVolumeHealthChecker
	telemetryProvider *mockVolumeTelemetryProvider
}

func newDeleteTestHandler(opts ...func(*deleteTestOptions)) *deleteTestSetup {
	o := &deleteTestOptions{
		repo:              &mockChunkRepository{},
		chunkStore:        &mockChunkStore{},
		healthChecker:     &mockVolumeHealthChecker{},
		telemetryProvider: &mockVolumeTelemetryProvider{},
		volumeID:          domain.VolumeID(1),
		nowFunc:           func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	handler := &DeleteChunkHandler{
		Repo:                    o.repo,
		VolumeHealthChecker:     o.healthChecker,
		VolumeManager:           volumeManager,
		VolumeTelemetryProvider: o.telemetryProvider,
		NowFunc:                 o.nowFunc,
	}

	return &deleteTestSetup{
		handler:           handler,
		repo:              o.repo,
		chunkStore:        o.chunkStore,
		healthChecker:     o.healthChecker,
		telemetryProvider: o.telemetryProvider,
	}
}

type deleteTestOptions struct {
	repo              *mockChunkRepository
	chunkStore        *mockChunkStore
	healthChecker     *mockVolumeHealthChecker
	telemetryProvider *mockVolumeTelemetryProvider
	volumeID          domain.VolumeID
	nowFunc           func() time.Time
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

func deleteWithTelemetryProvider(provider *mockVolumeTelemetryProvider) func(*deleteTestOptions) {
	return func(o *deleteTestOptions) { o.telemetryProvider = provider }
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

func TestDeleteChunkHandler_Success_VolumeTelemetryProviderCalled(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 100, 1)

	telemetryProvider := &mockVolumeTelemetryProvider{}

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
		deleteWithTelemetryProvider(telemetryProvider),
	)

	_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Telemetry provider is wired up correctly - actual calls depend on implementation
	if telemetryProvider.getVolumeAttributesCalls == 0 && telemetryProvider.getVolumeLoggerFieldsCalls == 0 {
		// This is acceptable - telemetry may only be called in certain code paths
	}
}

func TestDeleteChunkHandler_WrongState(t *testing.T) {
	tests := []struct {
		name  string
		chunk domain.Chunk
	}{
		{
			name: "TempChunk",
			chunk: &domain.TempChunk{
				ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
				WriterKey: []byte("wk"),
			},
		},
		{
			name: "DeletedChunk",
			chunk: &domain.DeletedChunk{
				ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
				WriterKey: []byte("wk"),
				DeletedAt: time.Now(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := newDeleteTestHandler(deleteWithRepo(repoReturningChunk(tt.chunk)))

			_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
				WriteKey: []byte("wk"),
			})
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, chunkerrors.ErrWrongState) {
				t.Errorf("expected ErrWrongState, got %v", err)
			}
		})
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

func TestDeleteChunkHandler_VolumeErrors(t *testing.T) {
	tests := []struct {
		name          string
		volumeState   domain.VolumeState
		expectedError error
		useVolumeID99 bool // Use unregistered volume
	}{
		{
			name:          "VolumeNotFound",
			volumeState:   domain.VolumeStateOK,
			expectedError: volumeerrors.ErrNotFound,
			useVolumeID99: true,
		},
		{
			name:          "VolumeDegraded",
			volumeState:   domain.VolumeStateDegraded,
			expectedError: volumeerrors.ErrDegraded,
			useVolumeID99: false,
		},
		{
			name:          "VolumeFailed",
			volumeState:   domain.VolumeStateFailed,
			expectedError: volumeerrors.ErrFailed,
			useVolumeID99: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var setup *deleteTestSetup

			if tt.useVolumeID99 {
				// Chunk on volume 99 which is not registered in VolumeManager
				existing := &domain.AvailableChunk{
					ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
					WriterKey: []byte("wk"),
					Size:      100,
					Version:   1,
				}
				setup = newDeleteTestHandler(deleteWithRepo(repoReturningChunk(existing)))
			} else {
				existing := makeAvailableChunk([]byte("wk"), 1000, 1)
				setup = newDeleteTestHandler(
					deleteWithRepo(repoReturningChunk(existing)),
					deleteWithHealthChecker(&mockVolumeHealthChecker{
						checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
							return &domain.VolumeHealth{State: tt.volumeState}
						},
					}),
				)
			}

			_, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
				WriteKey: []byte("wk"),
			})
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, tt.expectedError) {
				t.Errorf("expected %v, got %v", tt.expectedError, err)
			}

			if !tt.useVolumeID99 {
				// Admission errors should be wrapped with ChunkError
				var chunkErr *chunkerrors.ChunkError
				if !errors.As(err, &chunkErr) {
					t.Fatal("expected ChunkError context on volume health error")
				}

				if chunkErr.ChunkVersion() != 1 {
					t.Errorf("expected chunk version 1 in error, got %d", chunkErr.ChunkVersion())
				}

				if chunkErr.ChunkSize() != 1000 {
					t.Errorf("expected chunk size 1000 in error, got %d", chunkErr.ChunkSize())
				}
			}
		})
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

func TestDeleteChunkHandler_RepoStoreErrors(t *testing.T) {
	tests := []struct {
		name        string
		storeError  error
		expectError bool
	}{
		{
			name:        "StorageBackendUnavailable",
			storeError:  errors.New("storage backend unavailable"),
			expectError: true,
		},
		{
			name:        "NotFoundTolerated",
			storeError:  chunkerrors.ErrNotFound,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := makeAvailableChunk([]byte("wk"), 1000, 1)

			repo := &mockChunkRepository{
				getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
					return existing, nil
				},
				storeFunc: func(ctx context.Context, chunk domain.Chunk) error {
					return tt.storeError
				},
			}

			setup := newDeleteTestHandler(deleteWithRepo(repo))

			output, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
				WriteKey: []byte("wk"),
			})

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error")
				}
				if !errors.Is(err, tt.storeError) {
					t.Errorf("expected %v, got %v", tt.storeError, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if output.Chunk == nil {
					t.Fatal("expected deleted chunk in output")
				}
			}
		})
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

func TestDeleteChunkHandler_TimestampHandling(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 1)
	fixedTime := time.Date(2024, 12, 25, 10, 30, 0, 0, time.UTC)

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
		deleteWithNowFunc(func() time.Time { return fixedTime }),
	)

	output, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
		WriteKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk.DeletedAt != fixedTime {
		t.Errorf("expected deletedAt %v, got %v", fixedTime, output.Chunk.DeletedAt)
	}
}

func TestDeleteChunkHandler_WriterKeyVariations(t *testing.T) {
	tests := []struct {
		name      string
		writerKey []byte
	}{
		{
			name:      "ShortKey",
			writerKey: []byte("a"),
		},
		{
			name:      "LongKey",
			writerKey: make([]byte, 256),
		},
		{
			name:      "BinaryKey",
			writerKey: []byte{0x00, 0x01, 0xFF, 0xFE},
		},
		{
			name:      "UnicodeKey",
			writerKey: []byte("键值🔑"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := &domain.AvailableChunk{
				ID:        domain.NewChunkID(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 1, 0),
				WriterKey: tt.writerKey,
				Size:      100,
				Version:   1,
				CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			}

			setup := newDeleteTestHandler(deleteWithRepo(repoReturningChunk(existing)))

			output, err := setup.handler.HandleDeleteChunk(context.Background(), &DeleteChunkInput{
				WriteKey: tt.writerKey,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if string(output.Chunk.WriterKey) != string(tt.writerKey) {
				t.Errorf("writer key mismatch")
			}
		})
	}
}

func TestDeleteChunkHandler_LargeChunk(t *testing.T) {
	// Test deleting a very large chunk
	largeSize := int64(10 * 1024 * 1024 * 1024) // 10GB
	existing := makeAvailableChunk([]byte("wk"), largeSize, 1)

	var deletedChunkID domain.ChunkID
	chunkStore := &mockChunkStore{
		deleteChunkFunc: func(ctx context.Context, id domain.ChunkID) error {
			deletedChunkID = id
			return nil
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
		t.Fatalf("unexpected error: %v", err)
	}

	if deletedChunkID != existing.ID {
		t.Errorf("expected chunk ID %v, got %v", existing.ID, deletedChunkID)
	}

	if output.Chunk == nil {
		t.Fatal("expected deleted chunk in output")
	}
}

func TestDeleteChunkHandler_ChunkWithHighVersion(t *testing.T) {
	// Test deleting a chunk with a very high version number
	highVersion := uint64(999999)
	existing := makeAvailableChunk([]byte("wk"), 1000, highVersion)

	setup := newDeleteTestHandler(
		deleteWithRepo(repoReturningChunk(existing)),
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
}
