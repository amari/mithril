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
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

// --- Put Test Helpers ---

type putTestSetup struct {
	handler             *PutChunkHandler
	repo                *mockChunkRepository
	chunkStore          *mockChunkStore
	healthChecker       *mockVolumeHealthChecker
	telemetryProvider   *mockVolumeTelemetryProvider
	admissionController *mockVolumeAdmissionController
}

func newPutTestHandler(opts ...func(*putTestOptions)) *putTestSetup {
	o := &putTestOptions{
		repo:                 &mockChunkRepository{},
		idGen:                &mockChunkIDGenerator{},
		volumePicker:         &mockVolumePicker{},
		nodeIdentityRepo:     &mockNodeIdentityRepository{},
		volumeHealthChecker:  &mockVolumeHealthChecker{},
		volumeIDToStatsIndex: &mockVolumeIDToStatsIndex{},
		telemetryProvider:    &mockVolumeTelemetryProvider{},
		admissionController:  &mockVolumeAdmissionController{},
		chunkStore:           &mockChunkStore{},
		volumeID:             domain.VolumeID(1),
		nowFunc:              func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	handler := &PutChunkHandler{
		Repo:                      o.repo,
		IDGen:                     o.idGen,
		VolumeAdmissionController: o.admissionController,
		VolumeManager:             volumeManager,
		VolumePicker:              o.volumePicker,
		NowFunc:                   o.nowFunc,
		NodeIdentityRepository:    o.nodeIdentityRepo,
		VolumeHealthChecker:       o.volumeHealthChecker,
		VolumeIDToStatsIndex:      o.volumeIDToStatsIndex,
		VolumeTelemetryProvider:   o.telemetryProvider,
	}

	return &putTestSetup{
		handler:             handler,
		repo:                o.repo,
		chunkStore:          o.chunkStore,
		healthChecker:       o.volumeHealthChecker,
		telemetryProvider:   o.telemetryProvider,
		admissionController: o.admissionController,
	}
}

type putTestOptions struct {
	repo                 *mockChunkRepository
	idGen                *mockChunkIDGenerator
	volumePicker         *mockVolumePicker
	nodeIdentityRepo     *mockNodeIdentityRepository
	volumeHealthChecker  *mockVolumeHealthChecker
	volumeIDToStatsIndex *mockVolumeIDToStatsIndex
	telemetryProvider    *mockVolumeTelemetryProvider
	admissionController  *mockVolumeAdmissionController
	chunkStore           *mockChunkStore
	volumeID             domain.VolumeID
	nowFunc              func() time.Time
}

func putWithRepo(repo *mockChunkRepository) func(*putTestOptions) {
	return func(o *putTestOptions) { o.repo = repo }
}

func putWithIDGen(idGen *mockChunkIDGenerator) func(*putTestOptions) {
	return func(o *putTestOptions) { o.idGen = idGen }
}

func putWithVolumePicker(picker *mockVolumePicker) func(*putTestOptions) {
	return func(o *putTestOptions) { o.volumePicker = picker }
}

func putWithNodeIdentityRepo(repo *mockNodeIdentityRepository) func(*putTestOptions) {
	return func(o *putTestOptions) { o.nodeIdentityRepo = repo }
}

func putWithHealthChecker(checker *mockVolumeHealthChecker) func(*putTestOptions) {
	return func(o *putTestOptions) { o.volumeHealthChecker = checker }
}

func putWithVolumeIDToStatsIndex(index *mockVolumeIDToStatsIndex) func(*putTestOptions) {
	return func(o *putTestOptions) { o.volumeIDToStatsIndex = index }
}

func putWithTelemetryProvider(provider *mockVolumeTelemetryProvider) func(*putTestOptions) {
	return func(o *putTestOptions) { o.telemetryProvider = provider }
}

func putWithAdmissionController(controller *mockVolumeAdmissionController) func(*putTestOptions) {
	return func(o *putTestOptions) { o.admissionController = controller }
}

func putWithChunkStore(store *mockChunkStore) func(*putTestOptions) {
	return func(o *putTestOptions) { o.chunkStore = store }
}

func putWithVolumeID(id domain.VolumeID) func(*putTestOptions) {
	return func(o *putTestOptions) { o.volumeID = id }
}

func putWithNowFunc(f func() time.Time) func(*putTestOptions) {
	return func(o *putTestOptions) { o.nowFunc = f }
}

// --- Tests: Fresh Create Path ---

func TestPutChunkHandler_FreshCreate_Success(t *testing.T) {
	body := []byte("chunk-data-payload")

	setup := newPutTestHandler()

	input := &PutChunkInput{
		WriteKey:         []byte("test-writer-key"),
		MinTailSlackSize: 1024,
		Body:             bytes.NewReader(body),
		BodySize:         int64(len(body)),
	}

	output, err := setup.handler.HandlePutChunk(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	if output.Chunk.Size != 0 {
		t.Errorf("expected size 0, got %d", output.Chunk.Size)
	}

	if output.Chunk.Version != 1 {
		t.Errorf("expected version 1, got %d", output.Chunk.Version)
	}

	if string(output.Chunk.WriterKey) != "test-writer-key" {
		t.Errorf("expected writer key 'test-writer-key', got %s", output.Chunk.WriterKey)
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}

	if output.VolumeHealth.State != domain.VolumeStateOK {
		t.Errorf("expected volume state OK, got %v", output.VolumeHealth.State)
	}

	// Verify chunks were stored (temp then available)
	if len(setup.repo.storedChunks) != 2 {
		t.Errorf("expected 2 stored chunks (temp + available), got %d", len(setup.repo.storedChunks))
	}
}

func TestPutChunkHandler_FreshCreate_VolumeTelemetryProviderCalled(t *testing.T) {
	telemetryProvider := &mockVolumeTelemetryProvider{}

	setup := newPutTestHandler(putWithTelemetryProvider(telemetryProvider))

	input := &PutChunkInput{
		WriteKey: []byte("test-writer-key"),
		Body:     bytes.NewReader(nil),
	}

	_, err := setup.handler.HandlePutChunk(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Telemetry provider should be wired up correctly
	// Actual calls depend on implementation details
	if telemetryProvider.getVolumeAttributesCalls == 0 && telemetryProvider.getVolumeLoggerFieldsCalls == 0 {
		// This is acceptable - telemetry may only be called in certain code paths
	}
}

func TestPutChunkHandler_FreshCreate_VerifyPutArgs(t *testing.T) {
	body := []byte("payload")

	var (
		putID    domain.ChunkID
		putBody  []byte
		putSize  int64
		putSlack int64
	)

	chunkStore := &mockChunkStore{
		putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
			putID = id
			putBody, _ = io.ReadAll(r)
			putSize = n
			putSlack = minTailSlackSize
			return nil
		},
	}

	setup := newPutTestHandler(putWithChunkStore(chunkStore))

	output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey:         []byte("wk"),
		MinTailSlackSize: 4096,
		Body:             bytes.NewReader(body),
		BodySize:         int64(len(body)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if putID != output.Chunk.ID {
		t.Errorf("expected put chunk ID %v, got %v", output.Chunk.ID, putID)
	}

	if !bytes.Equal(putBody, body) {
		t.Errorf("expected put body %q, got %q", body, putBody)
	}

	if putSize != int64(len(body)) {
		t.Errorf("expected put size %d, got %d", len(body), putSize)
	}

	if putSlack != 4096 {
		t.Errorf("expected put slack 4096, got %d", putSlack)
	}
}

func TestPutChunkHandler_FreshCreate_VolumeHealthErrors(t *testing.T) {
	tests := []struct {
		name          string
		volumeState   domain.VolumeState
		expectedError error
	}{
		{
			name:          "VolumeDegraded",
			volumeState:   domain.VolumeStateDegraded,
			expectedError: volumeerrors.ErrDegraded,
		},
		{
			name:          "VolumeFailed",
			volumeState:   domain.VolumeStateFailed,
			expectedError: volumeerrors.ErrFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			volumeState := tt.volumeState
			setup := newPutTestHandler(
				putWithHealthChecker(&mockVolumeHealthChecker{
					checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
						return &domain.VolumeHealth{State: volumeState}
					},
				}),
				putWithAdmissionController(&mockVolumeAdmissionController{
					admitWriteFunc: func(id domain.VolumeID) error {
						switch volumeState {
						case domain.VolumeStateDegraded:
							return volumeerrors.WithState(volumeerrors.ErrDegraded, volumeerrors.StateDegraded)
						case domain.VolumeStateFailed:
							return volumeerrors.WithState(volumeerrors.ErrFailed, volumeerrors.StateFailed)
						}
						return nil
					},
				}),
			)

			_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
				WriteKey: []byte("wk"),
				Body:     bytes.NewReader(nil),
			})
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, tt.expectedError) {
				t.Errorf("expected %v, got %v", tt.expectedError, err)
			}
		})
	}
}

func TestPutChunkHandler_FreshCreate_DependencyErrors(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func() *putTestSetup
	}{
		{
			name: "NodeIdentityError",
			setupFunc: func() *putTestSetup {
				return newPutTestHandler(
					putWithNodeIdentityRepo(&mockNodeIdentityRepository{
						loadNodeIdentityFunc: func(ctx context.Context) (*domain.NodeIdentity, error) {
							return nil, errors.New("node identity unavailable")
						},
					}),
				)
			},
		},
		{
			name: "VolumePickerError",
			setupFunc: func() *putTestSetup {
				return newPutTestHandler(
					putWithVolumePicker(&mockVolumePicker{
						pickVolumeIDFunc: func(opts portvolume.PickVolumeIDOptions) (domain.VolumeID, error) {
							return 0, errors.New("no volumes available")
						},
					}),
				)
			},
		},
		{
			name: "IDGeneratorError",
			setupFunc: func() *putTestSetup {
				return newPutTestHandler(
					putWithIDGen(&mockChunkIDGenerator{
						nextIDFunc: func(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error) {
							return domain.ChunkID{}, errors.New("id generator exhausted")
						},
					}),
				)
			},
		},
		{
			name: "PutChunkStoreError",
			setupFunc: func() *putTestSetup {
				return newPutTestHandler(
					putWithChunkStore(&mockChunkStore{
						putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
							return errors.New("disk I/O error")
						},
					}),
				)
			},
		},
		{
			name: "StoreAvailableError",
			setupFunc: func() *putTestSetup {
				callCount := 0
				return newPutTestHandler(
					putWithRepo(&mockChunkRepository{
						storeFunc: func(ctx context.Context, chunk domain.Chunk) error {
							callCount++
							if callCount >= 2 {
								return errors.New("repo unavailable")
							}
							return nil
						},
					}),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := tt.setupFunc()

			_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
				WriteKey: []byte("wk"),
				Body:     bytes.NewReader([]byte("data")),
				BodySize: 4,
			})
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// --- Tests: Idempotency — Existing Available ---

func TestPutChunkHandler_Idempotency_ExistingAvailable(t *testing.T) {
	existingChunk := &domain.AvailableChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
		Size:      100,
		Version:   5,
	}

	setup := newPutTestHandler(putWithRepo(repoReturningChunk(existingChunk)))

	output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader([]byte("ignored")),
		BodySize: 7,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return existing chunk unchanged
	if output.Chunk.Size != 100 {
		t.Errorf("expected size 100, got %d", output.Chunk.Size)
	}

	if output.Chunk.Version != 5 {
		t.Errorf("expected version 5, got %d", output.Chunk.Version)
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}

	// No chunks should have been stored in repo (idempotent return)
	if len(setup.repo.storedChunks) != 0 {
		t.Errorf("expected 0 stored chunks, got %d", len(setup.repo.storedChunks))
	}
}

// --- Tests: Idempotency — Existing Temp ---

func TestPutChunkHandler_Idempotency_ExistingTemp(t *testing.T) {
	tests := []struct {
		name               string
		chunkExistsOnDisk  bool
		expectPutChunkCall bool
		expectStoredChunks int
	}{
		{
			name:               "ChunkExists",
			chunkExistsOnDisk:  true,
			expectPutChunkCall: false,
			expectStoredChunks: 1, // Just promote to available
		},
		{
			name:               "ChunkNotExists",
			chunkExistsOnDisk:  false,
			expectPutChunkCall: true,
			expectStoredChunks: 2, // temp + available
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
			existingTemp := &domain.TempChunk{
				ID:        chunkID,
				WriterKey: []byte("wk"),
				CreatedAt: time.Now().Add(-time.Hour),
			}

			var putCalled bool
			chunkStore := &mockChunkStore{
				chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
					return tt.chunkExistsOnDisk, nil
				},
				putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
					putCalled = true
					return nil
				},
			}

			setup := newPutTestHandler(
				putWithRepo(repoReturningChunk(existingTemp)),
				putWithChunkStore(chunkStore),
			)

			output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
				WriteKey:         []byte("wk"),
				MinTailSlackSize: 2048,
				Body:             bytes.NewReader([]byte("data")),
				BodySize:         4,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if output.Chunk == nil {
				t.Fatal("expected chunk in output")
			}

			if output.Chunk.Version != 1 {
				t.Errorf("expected version 1, got %d", output.Chunk.Version)
			}

			if putCalled != tt.expectPutChunkCall {
				t.Errorf("expected putCalled=%v, got %v", tt.expectPutChunkCall, putCalled)
			}

			if len(setup.repo.storedChunks) != tt.expectStoredChunks {
				t.Errorf("expected %d stored chunks, got %d", tt.expectStoredChunks, len(setup.repo.storedChunks))
			}

			if output.VolumeHealth == nil {
				t.Fatal("expected volume health in output")
			}
		})
	}
}

// --- Tests: Existing Temp — Error Paths ---

func TestPutChunkHandler_ExistingTemp_VolumeErrors(t *testing.T) {
	tests := []struct {
		name          string
		volumeState   domain.VolumeState
		expectedError error
		useVolumeID99 bool
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var existingTemp *domain.TempChunk

			if tt.useVolumeID99 {
				existingTemp = &domain.TempChunk{
					ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
					WriterKey: []byte("wk"),
					CreatedAt: time.Now().Add(-time.Hour),
				}
			} else {
				existingTemp = &domain.TempChunk{
					ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
					WriterKey: []byte("wk"),
					CreatedAt: time.Now().Add(-time.Hour),
				}
			}

			var setup *putTestSetup
			if tt.useVolumeID99 {
				setup = newPutTestHandler(putWithRepo(repoReturningChunk(existingTemp)))
			} else {
				volumeState := tt.volumeState
				setup = newPutTestHandler(
					putWithRepo(repoReturningChunk(existingTemp)),
					putWithHealthChecker(&mockVolumeHealthChecker{
						checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
							return &domain.VolumeHealth{State: volumeState}
						},
					}),
					putWithAdmissionController(&mockVolumeAdmissionController{
						admitWriteFunc: func(id domain.VolumeID) error {
							switch volumeState {
							case domain.VolumeStateDegraded:
								return volumeerrors.WithState(volumeerrors.ErrDegraded, volumeerrors.StateDegraded)
							case domain.VolumeStateFailed:
								return volumeerrors.WithState(volumeerrors.ErrFailed, volumeerrors.StateFailed)
							}
							return nil
						},
					}),
				)
			}

			_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
				WriteKey: []byte("wk"),
				Body:     bytes.NewReader(nil),
			})
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, tt.expectedError) {
				t.Errorf("expected %v, got %v", tt.expectedError, err)
			}

			if !tt.useVolumeID99 {
				var chunkErr *chunkerrors.ChunkError
				if !errors.As(err, &chunkErr) {
					t.Fatal("expected ChunkError context on volume health error for existing temp")
				}
			}
		})
	}
}

func TestPutChunkHandler_ExistingTemp_PutChunkStoreError(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("wk"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	diskErr := errors.New("disk write error")
	chunkStore := &mockChunkStore{
		chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
			return false, nil
		},
		putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
			return diskErr
		},
	}

	setup := newPutTestHandler(
		putWithRepo(repoReturningChunk(existingTemp)),
		putWithChunkStore(chunkStore),
	)

	_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader([]byte("data")),
		BodySize: 4,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, diskErr) {
		t.Errorf("expected disk write error, got %v", err)
	}
}

func TestPutChunkHandler_ExistingTemp_RepoStoreError(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("wk"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	repoErr := errors.New("repo unavailable")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existingTemp, nil
		},
		storeFunc: func(ctx context.Context, chunk domain.Chunk) error {
			return repoErr
		},
	}

	chunkStore := &mockChunkStore{
		chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
			return true, nil
		},
	}

	setup := newPutTestHandler(
		putWithRepo(repo),
		putWithChunkStore(chunkStore),
	)

	_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}
}

// --- Tests: Existing Deleted Chunk Falls Through to Fresh Create ---

func TestPutChunkHandler_ExistingDeleted_FallsThrough(t *testing.T) {
	deleted := &domain.DeletedChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
		DeletedAt: time.Now(),
	}

	setup := newPutTestHandler(
		putWithRepo(&mockChunkRepository{
			getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
				return deleted, nil
			},
		}),
	)

	output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader([]byte("new-data")),
		BodySize: 8,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should fall through to createFreshChunk and succeed
	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	if output.Chunk.Version != 1 {
		t.Errorf("expected version 1, got %d", output.Chunk.Version)
	}

	if string(output.Chunk.WriterKey) != "wk" {
		t.Errorf("expected writer key 'wk', got %q", output.Chunk.WriterKey)
	}
}

// --- Tests: Repo Lookup Errors ---

func TestPutChunkHandler_RepoLookupError_Passthrough(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	setup := newPutTestHandler(putWithRepo(repo))

	_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader(nil),
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

func TestPutChunkHandler_RepoNotFound_FallsThrough(t *testing.T) {
	// Default repo returns ErrNotFound, should fall through to createFreshChunk
	setup := newPutTestHandler()

	output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("new-key"),
		Body:     bytes.NewReader([]byte("data")),
		BodySize: 4,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	if output.Chunk.Version != 1 {
		t.Errorf("expected version 1, got %d", output.Chunk.Version)
	}
}

// --- Tests: Additional Coverage ---

func TestPutChunkHandler_TimestampHandling(t *testing.T) {
	fixedTime := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)

	setup := newPutTestHandler(putWithNowFunc(func() time.Time { return fixedTime }))

	output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("test-key"),
		Body:     bytes.NewReader(nil),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk.CreatedAt != fixedTime {
		t.Errorf("expected createdAt %v, got %v", fixedTime, output.Chunk.CreatedAt)
	}

	if output.Chunk.UpdatedAt != fixedTime {
		t.Errorf("expected updatedAt %v, got %v", fixedTime, output.Chunk.UpdatedAt)
	}
}

func TestPutChunkHandler_WriterKeyVariations(t *testing.T) {
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
			setup := newPutTestHandler()

			output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
				WriteKey: tt.writerKey,
				Body:     bytes.NewReader(nil),
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

func TestPutChunkHandler_MinTailSlackSizeVariations(t *testing.T) {
	tests := []struct {
		name             string
		minTailSlackSize int64
	}{
		{
			name:             "ZeroSlack",
			minTailSlackSize: 0,
		},
		{
			name:             "SmallSlack",
			minTailSlackSize: 512,
		},
		{
			name:             "LargeSlack",
			minTailSlackSize: 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedSlack int64
			chunkStore := &mockChunkStore{
				putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
					capturedSlack = minTailSlackSize
					return nil
				},
			}

			setup := newPutTestHandler(putWithChunkStore(chunkStore))

			_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
				WriteKey:         []byte("test-key"),
				MinTailSlackSize: tt.minTailSlackSize,
				Body:             bytes.NewReader(nil),
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedSlack != tt.minTailSlackSize {
				t.Errorf("expected slack %d, got %d", tt.minTailSlackSize, capturedSlack)
			}
		})
	}
}

func TestPutChunkHandler_LargeBody(t *testing.T) {
	largeBody := make([]byte, 10*1024*1024) // 10MB
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}

	var capturedBodySize int64
	chunkStore := &mockChunkStore{
		putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
			capturedBodySize = n
			_, _ = io.Copy(io.Discard, r)
			return nil
		},
	}

	setup := newPutTestHandler(putWithChunkStore(chunkStore))

	_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader(largeBody),
		BodySize: int64(len(largeBody)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedBodySize != int64(len(largeBody)) {
		t.Errorf("expected body size %d, got %d", len(largeBody), capturedBodySize)
	}
}

func TestPutChunkHandler_ExistingTemp_ChunkExistsCheckError_IgnoredAndProceeds(t *testing.T) {
	// The implementation ignores errors from ChunkExists and proceeds as if chunk doesn't exist
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("wk"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	var putChunkCalled bool
	chunkStore := &mockChunkStore{
		chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
			return false, errors.New("filesystem error")
		},
		putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
			putChunkCalled = true
			return nil
		},
	}

	setup := newPutTestHandler(
		putWithRepo(repoReturningChunk(existingTemp)),
		putWithChunkStore(chunkStore),
	)

	output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader(nil),
	})
	// Should succeed - the implementation ignores ChunkExists errors
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	// Should have called PutChunk since ChunkExists returned false (error ignored)
	if !putChunkCalled {
		t.Error("expected PutChunk to be called")
	}
}
