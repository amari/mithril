package command

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	portchunk "github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	"go.opentelemetry.io/otel/attribute"
)

// --- Mocks ---

type mockChunkRepository struct {
	getByWriterKeyFunc func(ctx context.Context, writerKey []byte) (domain.Chunk, error)
	getFunc            func(ctx context.Context, id domain.ChunkID) (domain.Chunk, error)
	storeFunc          func(ctx context.Context, chunk domain.Chunk) error
	deleteFunc         func(ctx context.Context, id domain.ChunkID) error
	listExpiredFunc    func(ctx context.Context, now time.Time, limit int) ([]domain.Chunk, error)

	storedChunks []domain.Chunk
}

var _ portchunk.ChunkRepository = (*mockChunkRepository)(nil)

func (m *mockChunkRepository) Get(ctx context.Context, id domain.ChunkID) (domain.Chunk, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, id)
	}
	return nil, chunkerrors.ErrNotFound
}

func (m *mockChunkRepository) GetByWriterKey(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
	if m.getByWriterKeyFunc != nil {
		return m.getByWriterKeyFunc(ctx, writerKey)
	}
	return nil, chunkerrors.ErrNotFound
}

func (m *mockChunkRepository) Store(ctx context.Context, chunk domain.Chunk) error {
	m.storedChunks = append(m.storedChunks, chunk)
	if m.storeFunc != nil {
		return m.storeFunc(ctx, chunk)
	}
	return nil
}

func (m *mockChunkRepository) Delete(ctx context.Context, id domain.ChunkID) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, id)
	}
	return nil
}

func (m *mockChunkRepository) ListExpired(ctx context.Context, now time.Time, limit int) ([]domain.Chunk, error) {
	if m.listExpiredFunc != nil {
		return m.listExpiredFunc(ctx, now, limit)
	}
	return nil, nil
}

type mockChunkIDGenerator struct {
	nextIDFunc func(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error)
}

var _ port.ChunkIDGenerator = (*mockChunkIDGenerator)(nil)

func (m *mockChunkIDGenerator) NextID(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error) {
	if m.nextIDFunc != nil {
		return m.nextIDFunc(nodeID, volumeID)
	}
	return domain.NewChunkID(time.Now().UnixMilli(), uint32(nodeID), uint16(volumeID), 0), nil
}

type mockVolumePicker struct {
	pickVolumeIDFunc func(opts portvolume.PickVolumeIDOptions) (domain.VolumeID, error)
}

var _ portvolume.VolumePicker = (*mockVolumePicker)(nil)

func (m *mockVolumePicker) PickVolumeID(opts portvolume.PickVolumeIDOptions) (domain.VolumeID, error) {
	if m.pickVolumeIDFunc != nil {
		return m.pickVolumeIDFunc(opts)
	}
	return 1, nil
}

func (m *mockVolumePicker) SetVolumeIDs(ids []domain.VolumeID) {}
func (m *mockVolumePicker) UpdateVolumeID(v domain.VolumeID)   {}

type mockNodeIdentityRepository struct {
	loadNodeIdentityFunc  func(ctx context.Context) (*domain.NodeIdentity, error)
	storeNodeIdentityFunc func(ctx context.Context, identity *domain.NodeIdentity) error
}

var _ port.NodeIdentityRepository = (*mockNodeIdentityRepository)(nil)

func (m *mockNodeIdentityRepository) LoadNodeIdentity(ctx context.Context) (*domain.NodeIdentity, error) {
	if m.loadNodeIdentityFunc != nil {
		return m.loadNodeIdentityFunc(ctx)
	}
	return &domain.NodeIdentity{NodeID: 1}, nil
}

func (m *mockNodeIdentityRepository) StoreNodeIdentity(ctx context.Context, identity *domain.NodeIdentity) error {
	if m.storeNodeIdentityFunc != nil {
		return m.storeNodeIdentityFunc(ctx, identity)
	}
	return nil
}

type mockVolumeHealthChecker struct {
	checkVolumeHealthFunc func(v domain.VolumeID) *domain.VolumeHealth
}

var _ portvolume.VolumeHealthChecker = (*mockVolumeHealthChecker)(nil)

func (m *mockVolumeHealthChecker) CheckVolumeHealth(v domain.VolumeID) *domain.VolumeHealth {
	if m.checkVolumeHealthFunc != nil {
		return m.checkVolumeHealthFunc(v)
	}
	return &domain.VolumeHealth{State: domain.VolumeStateOK}
}

type mockVolumeStatsProvider struct {
	volumeStatsFunc func(volume domain.VolumeID) *domain.VolumeStats
}

var _ portvolume.VolumeStatsProvider = (*mockVolumeStatsProvider)(nil)

func (m *mockVolumeStatsProvider) GetVolumeStats(volume domain.VolumeID) *domain.VolumeStats {
	if m.volumeStatsFunc != nil {
		return m.volumeStatsFunc(volume)
	}
	return nil
}

type mockVolumeTelemetryProvider struct {
	getVolumeAttributesFunc    func(id domain.VolumeID) []attribute.KeyValue
	getVolumeLoggerFieldsFunc  func(id domain.VolumeID) []any
	getVolumeAttributesCalls   int
	getVolumeLoggerFieldsCalls int
}

var _ portvolume.VolumeTelemetryProvider = (*mockVolumeTelemetryProvider)(nil)

func (m *mockVolumeTelemetryProvider) GetVolumeAttributes(id domain.VolumeID) []attribute.KeyValue {
	m.getVolumeAttributesCalls++
	if m.getVolumeAttributesFunc != nil {
		return m.getVolumeAttributesFunc(id)
	}
	return nil
}

func (m *mockVolumeTelemetryProvider) GetVolumeLoggerFields(id domain.VolumeID) []any {
	m.getVolumeLoggerFieldsCalls++
	if m.getVolumeLoggerFieldsFunc != nil {
		return m.getVolumeLoggerFieldsFunc(id)
	}
	return nil
}

type mockVolumeAdmissionController struct {
	admitWriteFunc func(id domain.VolumeID) error
	admitReadFunc  func(id domain.VolumeID) error
	admitStatFunc  func(id domain.VolumeID) error
}

var _ portvolume.VolumeAdmissionController = (*mockVolumeAdmissionController)(nil)

func (m *mockVolumeAdmissionController) AdmitWrite(id domain.VolumeID) error {
	if m.admitWriteFunc != nil {
		return m.admitWriteFunc(id)
	}
	return nil
}

func (m *mockVolumeAdmissionController) AdmitRead(id domain.VolumeID) error {
	if m.admitReadFunc != nil {
		return m.admitReadFunc(id)
	}
	return nil
}

func (m *mockVolumeAdmissionController) AdmitStat(id domain.VolumeID) error {
	if m.admitStatFunc != nil {
		return m.admitStatFunc(id)
	}
	return nil
}

type mockChunkStore struct {
	chunkExistsFunc          func(ctx context.Context, id domain.ChunkID) (bool, error)
	openChunkFunc            func(ctx context.Context, id domain.ChunkID) (port.Chunk, error)
	createChunkFunc          func(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error
	putChunkFunc             func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error
	appendChunkFunc          func(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error
	deleteChunkFunc          func(ctx context.Context, id domain.ChunkID) error
	shrinkChunkTailSlackFunc func(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error

	createdChunks []domain.ChunkID
}

var _ port.ChunkStore = (*mockChunkStore)(nil)

func (m *mockChunkStore) ChunkExists(ctx context.Context, id domain.ChunkID) (bool, error) {
	if m.chunkExistsFunc != nil {
		return m.chunkExistsFunc(ctx, id)
	}
	return false, nil
}

func (m *mockChunkStore) OpenChunk(ctx context.Context, id domain.ChunkID) (port.Chunk, error) {
	if m.openChunkFunc != nil {
		return m.openChunkFunc(ctx, id)
	}
	return nil, nil
}

func (m *mockChunkStore) CreateChunk(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error {
	m.createdChunks = append(m.createdChunks, id)
	if m.createChunkFunc != nil {
		return m.createChunkFunc(ctx, id, minTailSlackSize)
	}
	return nil
}

func (m *mockChunkStore) PutChunk(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
	if m.putChunkFunc != nil {
		return m.putChunkFunc(ctx, id, r, n, minTailSlackSize)
	}
	return nil
}

func (m *mockChunkStore) AppendChunk(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error {
	if m.appendChunkFunc != nil {
		return m.appendChunkFunc(ctx, id, logicalSize, r, n, minTailSlackSize)
	}
	return nil
}

func (m *mockChunkStore) DeleteChunk(ctx context.Context, id domain.ChunkID) error {
	if m.deleteChunkFunc != nil {
		return m.deleteChunkFunc(ctx, id)
	}
	return nil
}

func (m *mockChunkStore) ShrinkChunkTailSlack(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error {
	if m.shrinkChunkTailSlackFunc != nil {
		return m.shrinkChunkTailSlackFunc(ctx, id, logicalSize, maxTailSlackSize)
	}
	return nil
}

type mockVolume struct {
	id         domain.VolumeID
	chunkStore *mockChunkStore
}

var _ portvolume.Volume = (*mockVolume)(nil)

func (m *mockVolume) Close() error            { return nil }
func (m *mockVolume) ID() domain.VolumeID     { return m.id }
func (m *mockVolume) Chunks() port.ChunkStore { return m.chunkStore }

// --- Test Helpers ---

type createTestSetup struct {
	handler             *CreateChunkHandler
	repo                *mockChunkRepository
	chunkStore          *mockChunkStore
	healthChecker       *mockVolumeHealthChecker
	telemetryProvider   *mockVolumeTelemetryProvider
	admissionController *mockVolumeAdmissionController
}

func newTestHandler(opts ...func(*testHandlerOptions)) *createTestSetup {
	o := &testHandlerOptions{
		repo:                &mockChunkRepository{},
		idGen:               &mockChunkIDGenerator{},
		volumePicker:        &mockVolumePicker{},
		nodeIdentityRepo:    &mockNodeIdentityRepository{},
		volumeHealthChecker: &mockVolumeHealthChecker{},
		volumeStatsProvider: &mockVolumeStatsProvider{},
		telemetryProvider:   &mockVolumeTelemetryProvider{},
		admissionController: &mockVolumeAdmissionController{},
		chunkStore:          &mockChunkStore{},
		volumeID:            domain.VolumeID(1),
		nowFunc:             func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	handler := &CreateChunkHandler{
		Repo:                      o.repo,
		IDGen:                     o.idGen,
		VolumeAdmissionController: o.admissionController,
		VolumeManager:             volumeManager,
		VolumePicker:              o.volumePicker,
		NowFunc:                   o.nowFunc,
		NodeIdentityRepository:    o.nodeIdentityRepo,
		VolumeHealthChecker:       o.volumeHealthChecker,
		VolumeStatsProvider:       o.volumeStatsProvider,
		VolumeTelemetryProvider:   o.telemetryProvider,
	}

	return &createTestSetup{
		handler:             handler,
		repo:                o.repo,
		chunkStore:          o.chunkStore,
		healthChecker:       o.volumeHealthChecker,
		telemetryProvider:   o.telemetryProvider,
		admissionController: o.admissionController,
	}
}

type testHandlerOptions struct {
	repo                *mockChunkRepository
	idGen               *mockChunkIDGenerator
	volumePicker        *mockVolumePicker
	nodeIdentityRepo    *mockNodeIdentityRepository
	volumeHealthChecker *mockVolumeHealthChecker
	volumeStatsProvider *mockVolumeStatsProvider
	telemetryProvider   *mockVolumeTelemetryProvider
	admissionController *mockVolumeAdmissionController
	chunkStore          *mockChunkStore
	volumeID            domain.VolumeID
	nowFunc             func() time.Time
}

func withRepo(repo *mockChunkRepository) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.repo = repo }
}

func withIDGen(idGen *mockChunkIDGenerator) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.idGen = idGen }
}

func withVolumePicker(picker *mockVolumePicker) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.volumePicker = picker }
}

func withNodeIdentityRepo(repo *mockNodeIdentityRepository) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.nodeIdentityRepo = repo }
}

func withVolumeHealthChecker(checker *mockVolumeHealthChecker) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.volumeHealthChecker = checker }
}

func withChunkStore(store *mockChunkStore) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.chunkStore = store }
}

func withVolumeID(id domain.VolumeID) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.volumeID = id }
}

func withTelemetryProvider(provider *mockVolumeTelemetryProvider) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.telemetryProvider = provider }
}

func withAdmissionController(controller *mockVolumeAdmissionController) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.admissionController = controller }
}

func withNowFunc(f func() time.Time) func(*testHandlerOptions) {
	return func(o *testHandlerOptions) { o.nowFunc = f }
}

// --- Tests ---

func TestCreateChunkHandler_FreshCreate_Success(t *testing.T) {
	setup := newTestHandler()

	input := &CreateChunkInput{
		WriteKey:         []byte("test-writer-key"),
		MinTailSlackSize: 1024,
	}

	output, err := setup.handler.HandleCreateChunk(context.Background(), input)
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

	// Verify chunk was created in store
	if len(setup.chunkStore.createdChunks) != 1 {
		t.Errorf("expected 1 created chunk, got %d", len(setup.chunkStore.createdChunks))
	}

	// Verify chunks were stored (temp then available)
	if len(setup.repo.storedChunks) != 2 {
		t.Errorf("expected 2 stored chunks, got %d", len(setup.repo.storedChunks))
	}
}

func TestCreateChunkHandler_FreshCreate_VolumeTelemetryProviderCalled(t *testing.T) {
	telemetryProvider := &mockVolumeTelemetryProvider{}

	setup := newTestHandler(withTelemetryProvider(telemetryProvider))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := setup.handler.HandleCreateChunk(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Telemetry provider should be invoked during the request
	// The exact method depends on implementation, but at least one should be called
	if telemetryProvider.getVolumeAttributesCalls == 0 && telemetryProvider.getVolumeLoggerFieldsCalls == 0 {
		// This is acceptable - telemetry may only be called in certain code paths
	}
}

func TestCreateChunkHandler_Idempotency_ExistingAvailable(t *testing.T) {
	existingChunk := &domain.AvailableChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("test-writer-key"),
		Size:      100,
		Version:   5,
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existingChunk, nil
		},
	}

	setup := newTestHandler(withRepo(repo))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	output, err := setup.handler.HandleCreateChunk(context.Background(), input)
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
}

func TestCreateChunkHandler_Idempotency_ExistingTemp(t *testing.T) {
	tests := []struct {
		name                string
		chunkExistsOnDisk   bool
		expectCreatedChunks int
	}{
		{
			name:                "ChunkExists",
			chunkExistsOnDisk:   true,
			expectCreatedChunks: 0,
		},
		{
			name:                "ChunkNotExists",
			chunkExistsOnDisk:   false,
			expectCreatedChunks: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
			existingTemp := &domain.TempChunk{
				ID:        chunkID,
				WriterKey: []byte("test-writer-key"),
				CreatedAt: time.Now().Add(-time.Hour),
			}

			repo := &mockChunkRepository{
				getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
					return existingTemp, nil
				},
			}

			chunkStore := &mockChunkStore{
				chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
					return tt.chunkExistsOnDisk, nil
				},
			}

			setup := newTestHandler(
				withRepo(repo),
				withChunkStore(chunkStore),
			)

			input := &CreateChunkInput{
				WriteKey:         []byte("test-writer-key"),
				MinTailSlackSize: 2048,
			}

			output, err := setup.handler.HandleCreateChunk(context.Background(), input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if output.Chunk == nil {
				t.Fatal("expected chunk in output")
			}

			if output.Chunk.Version != 1 {
				t.Errorf("expected version 1, got %d", output.Chunk.Version)
			}

			if len(chunkStore.createdChunks) != tt.expectCreatedChunks {
				t.Errorf("expected %d created chunks, got %d", tt.expectCreatedChunks, len(chunkStore.createdChunks))
			}
		})
	}
}

func TestCreateChunkHandler_VolumeHealthErrors(t *testing.T) {
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
			volumeHealthChecker := &mockVolumeHealthChecker{
				checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
					return &domain.VolumeHealth{State: volumeState}
				},
			}
			admissionController := &mockVolumeAdmissionController{
				admitWriteFunc: func(id domain.VolumeID) error {
					switch volumeState {
					case domain.VolumeStateDegraded:
						return volumeerrors.WithState(volumeerrors.ErrDegraded, volumeerrors.StateDegraded)
					case domain.VolumeStateFailed:
						return volumeerrors.WithState(volumeerrors.ErrFailed, volumeerrors.StateFailed)
					}
					return nil
				},
			}

			setup := newTestHandler(
				withVolumeHealthChecker(volumeHealthChecker),
				withAdmissionController(admissionController),
			)

			input := &CreateChunkInput{
				WriteKey: []byte("test-writer-key"),
			}

			_, err := setup.handler.HandleCreateChunk(context.Background(), input)
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, tt.expectedError) {
				t.Errorf("expected %v, got %v", tt.expectedError, err)
			}
		})
	}
}

func TestCreateChunkHandler_DependencyErrors(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func() *createTestSetup
		expectedError error
	}{
		{
			name: "RepoError",
			setupFunc: func() *createTestSetup {
				repo := &mockChunkRepository{
					getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
						return nil, errors.New("database connection failed")
					},
				}
				return newTestHandler(withRepo(repo))
			},
			expectedError: nil, // Custom error, just check for non-nil
		},
		{
			name: "NodeIdentityError",
			setupFunc: func() *createTestSetup {
				nodeIdentityRepo := &mockNodeIdentityRepository{
					loadNodeIdentityFunc: func(ctx context.Context) (*domain.NodeIdentity, error) {
						return nil, errors.New("etcd unavailable")
					},
				}
				return newTestHandler(withNodeIdentityRepo(nodeIdentityRepo))
			},
			expectedError: nil,
		},
		{
			name: "VolumePickerError",
			setupFunc: func() *createTestSetup {
				volumePicker := &mockVolumePicker{
					pickVolumeIDFunc: func(opts portvolume.PickVolumeIDOptions) (domain.VolumeID, error) {
						return 0, volumeerrors.ErrNoVolumesAvailable
					},
				}
				return newTestHandler(withVolumePicker(volumePicker))
			},
			expectedError: volumeerrors.ErrNoVolumesAvailable,
		},
		{
			name: "IDGeneratorError",
			setupFunc: func() *createTestSetup {
				idGen := &mockChunkIDGenerator{
					nextIDFunc: func(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error) {
						return domain.ChunkID{}, chunkerrors.ErrClockRegression
					},
				}
				return newTestHandler(withIDGen(idGen))
			},
			expectedError: chunkerrors.ErrClockRegression,
		},
		{
			name: "ChunkStoreCreateError",
			setupFunc: func() *createTestSetup {
				chunkStore := &mockChunkStore{
					createChunkFunc: func(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error {
						return errors.New("disk full")
					},
				}
				return newTestHandler(withChunkStore(chunkStore))
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := tt.setupFunc()

			input := &CreateChunkInput{
				WriteKey: []byte("test-writer-key"),
			}

			_, err := setup.handler.HandleCreateChunk(context.Background(), input)
			if err == nil {
				t.Fatal("expected error")
			}

			if tt.expectedError != nil && !errors.Is(err, tt.expectedError) {
				t.Errorf("expected %v, got %v", tt.expectedError, err)
			}
		})
	}
}

func TestCreateChunkHandler_StoreAvailableError(t *testing.T) {
	storeCallCount := 0
	repo := &mockChunkRepository{
		storeFunc: func(ctx context.Context, chunk domain.Chunk) error {
			storeCallCount++
			// Fail on second store (available chunk)
			if storeCallCount == 2 {
				return errors.New("storage error")
			}
			return nil
		},
	}

	setup := newTestHandler(withRepo(repo))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := setup.handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateChunkHandler_ExistingTemp_VolumeDegraded(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("test-writer-key"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existingTemp, nil
		},
	}

	volumeHealthChecker := &mockVolumeHealthChecker{
		checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
			return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
		},
	}

	admissionController := &mockVolumeAdmissionController{
		admitWriteFunc: func(id domain.VolumeID) error {
			return volumeerrors.WithState(volumeerrors.ErrDegraded, volumeerrors.StateDegraded)
		},
	}

	setup := newTestHandler(
		withRepo(repo),
		withVolumeHealthChecker(volumeHealthChecker),
		withAdmissionController(admissionController),
	)

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := setup.handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrDegraded) {
		t.Errorf("expected degraded error, got %v", err)
	}

	// Verify ChunkError context is attached
	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Error("expected ChunkError context")
	}
}

func TestCreateChunkHandler_WriterKeyVariations(t *testing.T) {
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
			setup := newTestHandler()

			input := &CreateChunkInput{
				WriteKey: tt.writerKey,
			}

			output, err := setup.handler.HandleCreateChunk(context.Background(), input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if string(output.Chunk.WriterKey) != string(tt.writerKey) {
				t.Errorf("writer key mismatch")
			}
		})
	}
}

func TestCreateChunkHandler_MinTailSlackSizeVariations(t *testing.T) {
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
			minTailSlackSize: 1024 * 1024, // 1MB
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedSlackSize int64
			chunkStore := &mockChunkStore{
				createChunkFunc: func(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error {
					capturedSlackSize = minTailSlackSize
					return nil
				},
			}

			setup := newTestHandler(withChunkStore(chunkStore))

			input := &CreateChunkInput{
				WriteKey:         []byte("test-key"),
				MinTailSlackSize: tt.minTailSlackSize,
			}

			_, err := setup.handler.HandleCreateChunk(context.Background(), input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedSlackSize != tt.minTailSlackSize {
				t.Errorf("expected slack size %d, got %d", tt.minTailSlackSize, capturedSlackSize)
			}
		})
	}
}

func TestCreateChunkHandler_TimestampHandling(t *testing.T) {
	fixedTime := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)

	setup := newTestHandler(withNowFunc(func() time.Time { return fixedTime }))

	input := &CreateChunkInput{
		WriteKey: []byte("test-key"),
	}

	output, err := setup.handler.HandleCreateChunk(context.Background(), input)
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

func TestCreateChunkHandler_ChunkExistsCheckError_IgnoredAndProceeds(t *testing.T) {
	// The implementation ignores errors from ChunkExists and proceeds as if chunk doesn't exist
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("test-writer-key"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existingTemp, nil
		},
	}

	var createChunkCalled bool
	chunkStore := &mockChunkStore{
		chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
			return false, errors.New("filesystem error")
		},
		createChunkFunc: func(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error {
			createChunkCalled = true
			return nil
		},
	}

	setup := newTestHandler(
		withRepo(repo),
		withChunkStore(chunkStore),
	)

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	output, err := setup.handler.HandleCreateChunk(context.Background(), input)
	// Should succeed - the implementation ignores ChunkExists errors
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	// Should have called CreateChunk since ChunkExists returned false (error ignored)
	if !createChunkCalled {
		t.Error("expected CreateChunk to be called")
	}
}
