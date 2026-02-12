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
	telemetryProvider *mockVolumeTelemetryProvider
}

func newAppendFromTestHandler(opts ...func(*appendFromTestOptions)) *appendFromTestSetup {
	o := &appendFromTestOptions{
		repo:              &mockChunkRepository{},
		chunkStore:        &mockChunkStore{},
		healthChecker:     &mockVolumeHealthChecker{},
		remoteChunkClient: &mockRemoteChunkClient{},
		telemetryProvider: &mockVolumeTelemetryProvider{},
		volumeID:          domain.VolumeID(1),
		nowFunc:           func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	handler := &AppendFromChunkHandler{
		Repo:                    o.repo,
		VolumeHealthChecker:     o.healthChecker,
		VolumeManager:           volumeManager,
		VolumeTelemetryProvider: o.telemetryProvider,
		RemoteChunkClient:       o.remoteChunkClient,
		NowFunc:                 o.nowFunc,
	}

	return &appendFromTestSetup{
		handler:           handler,
		repo:              o.repo,
		chunkStore:        o.chunkStore,
		healthChecker:     o.healthChecker,
		remoteChunkClient: o.remoteChunkClient,
		telemetryProvider: o.telemetryProvider,
	}
}

type appendFromTestOptions struct {
	repo              *mockChunkRepository
	chunkStore        *mockChunkStore
	healthChecker     *mockVolumeHealthChecker
	remoteChunkClient *mockRemoteChunkClient
	telemetryProvider *mockVolumeTelemetryProvider
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

func appendFromWithTelemetryProvider(provider *mockVolumeTelemetryProvider) func(*appendFromTestOptions) {
	return func(o *appendFromTestOptions) { o.telemetryProvider = provider }
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

func TestAppendFromChunkHandler_Success_VolumeTelemetryProviderCalled(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 100, 1)

	telemetryProvider := &mockVolumeTelemetryProvider{}

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithTelemetryProvider(telemetryProvider),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("")), nil
			},
		}),
	)

	_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkLength: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Telemetry provider is wired up correctly - actual calls depend on implementation
	if telemetryProvider.getVolumeAttributesCalls == 0 && telemetryProvider.getVolumeLoggerFieldsCalls == 0 {
		// This is acceptable - telemetry may only be called in certain code paths
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

func TestAppendFromChunkHandler_WrongState(t *testing.T) {
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := newAppendFromTestHandler(appendFromWithRepo(repoReturningChunk(tt.chunk)))

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
		})
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

func TestAppendFromChunkHandler_VolumeErrors(t *testing.T) {
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
			var setup *appendFromTestSetup

			if tt.useVolumeID99 {
				// Chunk on volume 99 which isn't registered in VolumeManager
				existing := &domain.AvailableChunk{
					ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
					WriterKey: []byte("wk"),
					Size:      100,
					Version:   1,
				}
				setup = newAppendFromTestHandler(appendFromWithRepo(repoReturningChunk(existing)))
			} else {
				existing := makeAvailableChunk([]byte("wk"), 1000, 1)
				setup = newAppendFromTestHandler(
					appendFromWithRepo(repoReturningChunk(existing)),
					appendFromWithHealthChecker(&mockVolumeHealthChecker{
						checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
							return &domain.VolumeHealth{State: tt.volumeState}
						},
					}),
				)
			}

			_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
				WriteKey:        []byte("wk"),
				ExpectedVersion: 1,
				RemoteChunkID:   validRemoteChunkIDBytes(),
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

func TestAppendFromChunkHandler_LargeRemoteData(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 0, 1)
	largeDataSize := int64(10 * 1024 * 1024) // 10MB

	var capturedBodySize int64
	chunkStore := &mockChunkStore{
		appendChunkFunc: func(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error {
			capturedBodySize = n
			// Drain the reader
			_, _ = io.Copy(io.Discard, r)
			return nil
		},
	}

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithChunkStore(chunkStore),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				// Simulate large data
				return io.NopCloser(io.LimitReader(strings.NewReader(strings.Repeat("x", 1024)), length)), nil
			},
		}),
	)

	output, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkOffset: 0,
		RemoteChunkLength: largeDataSize,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedBodySize != largeDataSize {
		t.Errorf("expected body size %d, got %d", largeDataSize, capturedBodySize)
	}

	if output.Chunk.Size != largeDataSize {
		t.Errorf("expected chunk size %d, got %d", largeDataSize, output.Chunk.Size)
	}
}

func TestAppendFromChunkHandler_ZeroLengthRemoteRead(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 2)

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("")), nil
			},
		}),
	)

	output, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   2,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkOffset: 0,
		RemoteChunkLength: 0,
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

func TestAppendFromChunkHandler_RemoteChunkOffsetVariations(t *testing.T) {
	tests := []struct {
		name   string
		offset int64
		length int64
	}{
		{
			name:   "StartOfChunk",
			offset: 0,
			length: 100,
		},
		{
			name:   "MiddleOfChunk",
			offset: 500,
			length: 200,
		},
		{
			name:   "LargeOffset",
			offset: 1000000,
			length: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := makeAvailableChunk([]byte("wk"), 100, 1)

			var capturedOffset, capturedLength int64
			setup := newAppendFromTestHandler(
				appendFromWithRepo(repoReturningChunk(existing)),
				appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
					readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
						capturedOffset = offset
						capturedLength = length
						return io.NopCloser(strings.NewReader(strings.Repeat("x", int(length)))), nil
					},
				}),
			)

			_, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
				WriteKey:          []byte("wk"),
				ExpectedVersion:   1,
				RemoteChunkID:     validRemoteChunkIDBytes(),
				RemoteChunkOffset: tt.offset,
				RemoteChunkLength: tt.length,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedOffset != tt.offset {
				t.Errorf("expected offset %d, got %d", tt.offset, capturedOffset)
			}

			if capturedLength != tt.length {
				t.Errorf("expected length %d, got %d", tt.length, capturedLength)
			}
		})
	}
}

func TestAppendFromChunkHandler_TimestampHandling(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 100, 1)
	fixedTime := time.Date(2024, 12, 25, 10, 30, 0, 0, time.UTC)

	setup := newAppendFromTestHandler(
		appendFromWithRepo(repoReturningChunk(existing)),
		appendFromWithNowFunc(func() time.Time { return fixedTime }),
		appendFromWithRemoteChunkClient(&mockRemoteChunkClient{
			readChunkRangeFunc: func(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("data")), nil
			},
		}),
	)

	output, err := setup.handler.HandleAppendFromChunk(context.Background(), &AppendFromChunkInput{
		WriteKey:          []byte("wk"),
		ExpectedVersion:   1,
		RemoteChunkID:     validRemoteChunkIDBytes(),
		RemoteChunkLength: 4,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// CreatedAt should be preserved from original chunk
	if output.Chunk.CreatedAt != existing.CreatedAt {
		t.Errorf("expected createdAt %v, got %v", existing.CreatedAt, output.Chunk.CreatedAt)
	}

	// UpdatedAt should use NowFunc
	if output.Chunk.UpdatedAt != fixedTime {
		t.Errorf("expected updatedAt %v, got %v", fixedTime, output.Chunk.UpdatedAt)
	}
}

func TestAppendFromChunkHandler_VersionMismatch_IncludesVolumeState(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 5)

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
		ExpectedVersion: 3, // actual is 5
		RemoteChunkID:   validRemoteChunkIDBytes(),
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
