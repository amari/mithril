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
	volumeStatsFunc func(ctx context.Context, volume domain.VolumeID) (domain.VolumeStats, error)
}

var _ portvolume.VolumeStatsProvider = (*mockVolumeStatsProvider)(nil)

func (m *mockVolumeStatsProvider) VolumeStats(ctx context.Context, volume domain.VolumeID) (domain.VolumeStats, error) {
	if m.volumeStatsFunc != nil {
		return m.volumeStatsFunc(ctx, volume)
	}
	return domain.VolumeStats{}, nil
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

func newTestHandler(opts ...func(*testHandlerOptions)) *CreateChunkHandler {
	o := &testHandlerOptions{
		repo:                &mockChunkRepository{},
		idGen:               &mockChunkIDGenerator{},
		volumePicker:        &mockVolumePicker{},
		nodeIdentityRepo:    &mockNodeIdentityRepository{},
		volumeHealthChecker: &mockVolumeHealthChecker{},
		volumeStatsProvider: &mockVolumeStatsProvider{},
		chunkStore:          &mockChunkStore{},
		volumeID:            domain.VolumeID(1),
		nowFunc:             func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	return &CreateChunkHandler{
		Repo:                   o.repo,
		IDGen:                  o.idGen,
		VolumeManager:          volumeManager,
		VolumePicker:           o.volumePicker,
		NowFunc:                o.nowFunc,
		NodeIdentityRepository: o.nodeIdentityRepo,
		VolumeHealthChecker:    o.volumeHealthChecker,
		VolumeStatsProvider:    o.volumeStatsProvider,
	}
}

type testHandlerOptions struct {
	repo                *mockChunkRepository
	idGen               *mockChunkIDGenerator
	volumePicker        *mockVolumePicker
	nodeIdentityRepo    *mockNodeIdentityRepository
	volumeHealthChecker *mockVolumeHealthChecker
	volumeStatsProvider *mockVolumeStatsProvider
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

// --- Tests ---

func TestCreateChunkHandler_FreshCreate_Success(t *testing.T) {
	chunkStore := &mockChunkStore{}
	repo := &mockChunkRepository{}

	handler := newTestHandler(
		withRepo(repo),
		withChunkStore(chunkStore),
	)

	input := &CreateChunkInput{
		WriteKey:         []byte("test-writer-key"),
		MinTailSlackSize: 1024,
	}

	output, err := handler.HandleCreateChunk(context.Background(), input)
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
	if len(chunkStore.createdChunks) != 1 {
		t.Errorf("expected 1 created chunk, got %d", len(chunkStore.createdChunks))
	}

	// Verify chunks were stored (temp then available)
	if len(repo.storedChunks) != 2 {
		t.Errorf("expected 2 stored chunks, got %d", len(repo.storedChunks))
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

	handler := newTestHandler(withRepo(repo))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	output, err := handler.HandleCreateChunk(context.Background(), input)
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

func TestCreateChunkHandler_Idempotency_ExistingTemp_ChunkExists(t *testing.T) {
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
			return true, nil // Chunk already exists on disk
		},
	}

	handler := newTestHandler(
		withRepo(repo),
		withChunkStore(chunkStore),
	)

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	output, err := handler.HandleCreateChunk(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should promote to available
	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	if output.Chunk.Version != 1 {
		t.Errorf("expected version 1, got %d", output.Chunk.Version)
	}

	// Should NOT create chunk on disk (already exists)
	if len(chunkStore.createdChunks) != 0 {
		t.Errorf("expected 0 created chunks, got %d", len(chunkStore.createdChunks))
	}
}

func TestCreateChunkHandler_Idempotency_ExistingTemp_ChunkNotExists(t *testing.T) {
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
			return false, nil // Chunk does NOT exist on disk
		},
	}

	handler := newTestHandler(
		withRepo(repo),
		withChunkStore(chunkStore),
	)

	input := &CreateChunkInput{
		WriteKey:         []byte("test-writer-key"),
		MinTailSlackSize: 2048,
	}

	output, err := handler.HandleCreateChunk(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Chunk == nil {
		t.Fatal("expected chunk in output")
	}

	// Should create chunk on disk
	if len(chunkStore.createdChunks) != 1 {
		t.Errorf("expected 1 created chunk, got %d", len(chunkStore.createdChunks))
	}
}

func TestCreateChunkHandler_RepoError(t *testing.T) {
	repoErr := errors.New("database connection failed")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	handler := newTestHandler(withRepo(repo))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}
}

func TestCreateChunkHandler_NodeIdentityError(t *testing.T) {
	nodeIdentityErr := errors.New("etcd unavailable")
	nodeIdentityRepo := &mockNodeIdentityRepository{
		loadNodeIdentityFunc: func(ctx context.Context) (*domain.NodeIdentity, error) {
			return nil, nodeIdentityErr
		},
	}

	handler := newTestHandler(withNodeIdentityRepo(nodeIdentityRepo))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, nodeIdentityErr) {
		t.Errorf("expected node identity error, got %v", err)
	}
}

func TestCreateChunkHandler_VolumePickerError(t *testing.T) {
	volumePicker := &mockVolumePicker{
		pickVolumeIDFunc: func(opts portvolume.PickVolumeIDOptions) (domain.VolumeID, error) {
			return 0, volumeerrors.ErrNoVolumesAvailable
		},
	}

	handler := newTestHandler(withVolumePicker(volumePicker))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrNoVolumesAvailable) {
		t.Errorf("expected no volumes available error, got %v", err)
	}
}

func TestCreateChunkHandler_VolumeDegraded(t *testing.T) {
	volumeHealthChecker := &mockVolumeHealthChecker{
		checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
			return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
		},
	}

	handler := newTestHandler(withVolumeHealthChecker(volumeHealthChecker))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrDegraded) {
		t.Errorf("expected degraded error, got %v", err)
	}
}

func TestCreateChunkHandler_VolumeFailed(t *testing.T) {
	volumeHealthChecker := &mockVolumeHealthChecker{
		checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
			return &domain.VolumeHealth{State: domain.VolumeStateFailed}
		},
	}

	handler := newTestHandler(withVolumeHealthChecker(volumeHealthChecker))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrFailed) {
		t.Errorf("expected failed error, got %v", err)
	}
}

func TestCreateChunkHandler_IDGeneratorError(t *testing.T) {
	idGen := &mockChunkIDGenerator{
		nextIDFunc: func(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error) {
			return domain.ChunkID{}, chunkerrors.ErrClockRegression
		},
	}

	handler := newTestHandler(withIDGen(idGen))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrClockRegression) {
		t.Errorf("expected clock regression error, got %v", err)
	}
}

func TestCreateChunkHandler_ChunkStoreCreateError(t *testing.T) {
	chunkStore := &mockChunkStore{
		createChunkFunc: func(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error {
			return errors.New("disk full")
		},
	}

	handler := newTestHandler(withChunkStore(chunkStore))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
	if err == nil {
		t.Fatal("expected error")
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

	handler := newTestHandler(withRepo(repo))

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
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

	handler := newTestHandler(
		withRepo(repo),
		withVolumeHealthChecker(volumeHealthChecker),
	)

	input := &CreateChunkInput{
		WriteKey: []byte("test-writer-key"),
	}

	_, err := handler.HandleCreateChunk(context.Background(), input)
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
