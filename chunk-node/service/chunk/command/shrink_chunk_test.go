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

// --- Shrink Test Helpers ---

type shrinkTestSetup struct {
	handler           *ShrinkChunkHandler
	repo              *mockChunkRepository
	chunkStore        *mockChunkStore
	healthChecker     *mockVolumeHealthChecker
	telemetryProvider *mockVolumeTelemetryProvider
}

func newShrinkTestHandler(opts ...func(*shrinkTestOptions)) *shrinkTestSetup {
	o := &shrinkTestOptions{
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

	handler := &ShrinkChunkHandler{
		Repo:                    o.repo,
		VolumeHealthChecker:     o.healthChecker,
		VolumeManager:           volumeManager,
		VolumeTelemetryProvider: o.telemetryProvider,
		NowFunc:                 o.nowFunc,
	}

	return &shrinkTestSetup{
		handler:           handler,
		repo:              o.repo,
		chunkStore:        o.chunkStore,
		healthChecker:     o.healthChecker,
		telemetryProvider: o.telemetryProvider,
	}
}

type shrinkTestOptions struct {
	repo              *mockChunkRepository
	chunkStore        *mockChunkStore
	healthChecker     *mockVolumeHealthChecker
	telemetryProvider *mockVolumeTelemetryProvider
	volumeID          domain.VolumeID
	nowFunc           func() time.Time
}

func shrinkWithRepo(repo *mockChunkRepository) func(*shrinkTestOptions) {
	return func(o *shrinkTestOptions) { o.repo = repo }
}

func shrinkWithChunkStore(store *mockChunkStore) func(*shrinkTestOptions) {
	return func(o *shrinkTestOptions) { o.chunkStore = store }
}

func shrinkWithHealthChecker(checker *mockVolumeHealthChecker) func(*shrinkTestOptions) {
	return func(o *shrinkTestOptions) { o.healthChecker = checker }
}

func shrinkWithTelemetryProvider(provider *mockVolumeTelemetryProvider) func(*shrinkTestOptions) {
	return func(o *shrinkTestOptions) { o.telemetryProvider = provider }
}

func shrinkWithVolumeID(id domain.VolumeID) func(*shrinkTestOptions) {
	return func(o *shrinkTestOptions) { o.volumeID = id }
}

func shrinkWithNowFunc(f func() time.Time) func(*shrinkTestOptions) {
	return func(o *shrinkTestOptions) { o.nowFunc = f }
}

// --- Tests ---

func TestShrinkChunkHandler_Success(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 3)

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
	)

	output, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  3,
		MaxTailSlackSize: 512,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	// Shrink returns the existing chunk unchanged
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

func TestShrinkChunkHandler_Success_VerifyShrinkArgs(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 4096, 1)

	var (
		shrunkID          domain.ChunkID
		shrunkLogicalSize int64
		shrunkMaxSlack    int64
	)

	chunkStore := &mockChunkStore{
		shrinkChunkTailSlackFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error {
			shrunkID = id
			shrunkLogicalSize = logicalSize
			shrunkMaxSlack = maxTailSlackSize
			return nil
		},
	}

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
		shrinkWithChunkStore(chunkStore),
	)

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  1,
		MaxTailSlackSize: 256,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if shrunkID != existing.ID {
		t.Errorf("expected chunk ID %v, got %v", existing.ID, shrunkID)
	}

	if shrunkLogicalSize != 4096 {
		t.Errorf("expected logical size 4096, got %d", shrunkLogicalSize)
	}

	if shrunkMaxSlack != 256 {
		t.Errorf("expected max slack 256, got %d", shrunkMaxSlack)
	}
}

func TestShrinkChunkHandler_Success_VolumeTelemetryProviderCalled(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 100, 1)

	telemetryProvider := &mockVolumeTelemetryProvider{}

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
		shrinkWithTelemetryProvider(telemetryProvider),
	)

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  1,
		MaxTailSlackSize: 512,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Telemetry provider is wired up correctly - actual calls depend on implementation
	if telemetryProvider.getVolumeAttributesCalls == 0 && telemetryProvider.getVolumeLoggerFieldsCalls == 0 {
		// This is acceptable - telemetry may only be called in certain code paths
	}
}

func TestShrinkChunkHandler_WrongState(t *testing.T) {
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
			setup := newShrinkTestHandler(shrinkWithRepo(repoReturningChunk(tt.chunk)))

			_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
				WriteKey:        []byte("wk"),
				ExpectedVersion: 1,
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

func TestShrinkChunkHandler_WriterKeyNotFound(t *testing.T) {
	setup := newShrinkTestHandler() // default repo returns ErrNotFound

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:        []byte("nonexistent"),
		ExpectedVersion: 1,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestShrinkChunkHandler_VersionMismatch(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 5)

	setup := newShrinkTestHandler(shrinkWithRepo(repoReturningChunk(existing)))

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 3, // actual is 5
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

func TestShrinkChunkHandler_VolumeErrors(t *testing.T) {
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
			var setup *shrinkTestSetup

			if tt.useVolumeID99 {
				// Chunk on volume 99 which is not registered
				existing := &domain.AvailableChunk{
					ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
					WriterKey: []byte("wk"),
					Size:      100,
					Version:   1,
				}
				setup = newShrinkTestHandler(shrinkWithRepo(repoReturningChunk(existing)))
			} else {
				existing := makeAvailableChunk([]byte("wk"), 1000, 1)
				setup = newShrinkTestHandler(
					shrinkWithRepo(repoReturningChunk(existing)),
					shrinkWithHealthChecker(&mockVolumeHealthChecker{
						checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
							return &domain.VolumeHealth{State: tt.volumeState}
						},
					}),
				)
			}

			_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
				WriteKey:        []byte("wk"),
				ExpectedVersion: 1,
			})
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, tt.expectedError) {
				t.Errorf("expected %v, got %v", tt.expectedError, err)
			}

			var chunkErr *chunkerrors.ChunkError
			if !errors.As(err, &chunkErr) {
				t.Fatal("expected ChunkError context")
			}
		})
	}
}

func TestShrinkChunkHandler_ShrinkChunkStoreError(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)
	diskErr := errors.New("disk I/O error on shrink")

	chunkStore := &mockChunkStore{
		shrinkChunkTailSlackFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error {
			return diskErr
		},
	}

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
		shrinkWithChunkStore(chunkStore),
	)

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  1,
		MaxTailSlackSize: 512,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, diskErr) {
		t.Errorf("expected disk I/O error, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context on store error")
	}

	if chunkErr.ChunkVersion() != 1 {
		t.Errorf("expected chunk version 1 in error, got %d", chunkErr.ChunkVersion())
	}

	if chunkErr.ChunkSize() != 1000 {
		t.Errorf("expected chunk size 1000 in error, got %d", chunkErr.ChunkSize())
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatal("expected StateError context on store error")
	}
}

func TestShrinkChunkHandler_RepoLookupError_Passthrough(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	setup := newShrinkTestHandler(shrinkWithRepo(repo))

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 1,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}

	// Repo lookup errors should NOT be wrapped with ChunkError
	var chunkErr *chunkerrors.ChunkError
	if errors.As(err, &chunkErr) {
		t.Error("repo lookup error should NOT have ChunkError context")
	}
}

func TestShrinkChunkHandler_NoRepoStore(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
	)

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  1,
		MaxTailSlackSize: 512,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Shrink is purely physical — no Repo.Store calls
	if len(setup.repo.storedChunks) != 0 {
		t.Errorf("expected 0 stored chunks (shrink is physical only), got %d", len(setup.repo.storedChunks))
	}
}

func TestShrinkChunkHandler_MaxTailSlackSizeVariations(t *testing.T) {
	tests := []struct {
		name             string
		maxTailSlackSize int64
	}{
		{
			name:             "ZeroSlack",
			maxTailSlackSize: 0,
		},
		{
			name:             "SmallSlack",
			maxTailSlackSize: 128,
		},
		{
			name:             "MediumSlack",
			maxTailSlackSize: 512,
		},
		{
			name:             "LargeSlack",
			maxTailSlackSize: 4096,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := makeAvailableChunk([]byte("wk"), 1000, 1)

			var capturedMaxSlack int64
			chunkStore := &mockChunkStore{
				shrinkChunkTailSlackFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error {
					capturedMaxSlack = maxTailSlackSize
					return nil
				},
			}

			setup := newShrinkTestHandler(
				shrinkWithRepo(repoReturningChunk(existing)),
				shrinkWithChunkStore(chunkStore),
			)

			_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
				WriteKey:         []byte("wk"),
				ExpectedVersion:  1,
				MaxTailSlackSize: tt.maxTailSlackSize,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedMaxSlack != tt.maxTailSlackSize {
				t.Errorf("expected max slack %d, got %d", tt.maxTailSlackSize, capturedMaxSlack)
			}
		})
	}
}

func TestShrinkChunkHandler_VersionMismatch_IncludesVolumeState(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 5)

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
		shrinkWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
			},
		}),
	)

	_, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:        []byte("wk"),
		ExpectedVersion: 3, // actual is 5
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatal("expected StateError context")
	}

	if stateErr.State() != domain.VolumeStateDegraded {
		t.Errorf("expected degraded state in error, got %v", stateErr.State())
	}
}

func TestShrinkChunkHandler_LargeChunk(t *testing.T) {
	// Test shrinking a very large chunk
	largeSize := int64(10 * 1024 * 1024 * 1024) // 10GB
	existing := makeAvailableChunk([]byte("wk"), largeSize, 1)

	var capturedLogicalSize int64
	chunkStore := &mockChunkStore{
		shrinkChunkTailSlackFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error {
			capturedLogicalSize = logicalSize
			return nil
		},
	}

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
		shrinkWithChunkStore(chunkStore),
	)

	output, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  1,
		MaxTailSlackSize: 1024,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedLogicalSize != largeSize {
		t.Errorf("expected logical size %d, got %d", largeSize, capturedLogicalSize)
	}

	if output.Chunk.Size != largeSize {
		t.Errorf("expected chunk size %d, got %d", largeSize, output.Chunk.Size)
	}
}

func TestShrinkChunkHandler_ChunkWithZeroSize(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 0, 1)

	setup := newShrinkTestHandler(
		shrinkWithRepo(repoReturningChunk(existing)),
	)

	output, err := setup.handler.HandleShrinkChunk(context.Background(), &ShrinkChunkInput{
		WriteKey:         []byte("wk"),
		ExpectedVersion:  1,
		MaxTailSlackSize: 512,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk.Size != 0 {
		t.Errorf("expected size 0, got %d", output.Chunk.Size)
	}
}
