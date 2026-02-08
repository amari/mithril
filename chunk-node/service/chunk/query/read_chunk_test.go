package query

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

type mockChunkStore struct {
	openChunkFunc func(ctx context.Context, id domain.ChunkID) (port.Chunk, error)
}

func (m *mockChunkStore) ChunkExists(ctx context.Context, id domain.ChunkID) (bool, error) {
	return false, nil
}
func (m *mockChunkStore) OpenChunk(ctx context.Context, id domain.ChunkID) (port.Chunk, error) {
	if m.openChunkFunc != nil {
		return m.openChunkFunc(ctx, id)
	}
	return &mockChunkHandle{id: id}, nil
}
func (m *mockChunkStore) CreateChunk(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error {
	return nil
}
func (m *mockChunkStore) PutChunk(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
	return nil
}
func (m *mockChunkStore) AppendChunk(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error {
	return nil
}
func (m *mockChunkStore) DeleteChunk(ctx context.Context, id domain.ChunkID) error { return nil }
func (m *mockChunkStore) ShrinkChunkTailSlack(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error {
	return nil
}

type mockChunkHandle struct {
	id domain.ChunkID
}

var _ port.Chunk = (*mockChunkHandle)(nil)

func (m *mockChunkHandle) ID() domain.ChunkID { return m.id }
func (m *mockChunkHandle) Close() error       { return nil }
func (m *mockChunkHandle) NewReader(ctx context.Context) (port.ChunkReader, error) {
	return nil, nil
}
func (m *mockChunkHandle) NewRangeReader(ctx context.Context, offset, length int64) (port.ChunkRangeReader, error) {
	return nil, nil
}
func (m *mockChunkHandle) NewReaderAt(ctx context.Context) (port.ChunkReaderAt, error) {
	return nil, nil
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

type readTestSetup struct {
	handler       *ReadChunkHandler
	chunkStore    *mockChunkStore
	healthChecker *mockVolumeHealthChecker
}

func newReadTestHandler(opts ...func(*readTestOptions)) *readTestSetup {
	o := &readTestOptions{
		repo:          &mockChunkRepository{},
		chunkStore:    &mockChunkStore{},
		healthChecker: &mockVolumeHealthChecker{},
		volumeID:      domain.VolumeID(1),
	}

	for _, opt := range opts {
		opt(o)
	}

	volumeManager := volume.NewVolumeManager()
	volumeManager.AddVolume(&mockVolume{id: o.volumeID, chunkStore: o.chunkStore})

	handler := &ReadChunkHandler{
		Repo:                o.repo,
		VolumeManager:       volumeManager,
		VolumeHealthChecker: o.healthChecker,
	}

	return &readTestSetup{
		handler:       handler,
		chunkStore:    o.chunkStore,
		healthChecker: o.healthChecker,
	}
}

type readTestOptions struct {
	repo          *mockChunkRepository
	chunkStore    *mockChunkStore
	healthChecker *mockVolumeHealthChecker
	volumeID      domain.VolumeID
}

func readWithRepo(repo *mockChunkRepository) func(*readTestOptions) {
	return func(o *readTestOptions) { o.repo = repo }
}

func readWithChunkStore(store *mockChunkStore) func(*readTestOptions) {
	return func(o *readTestOptions) { o.chunkStore = store }
}

func readWithHealthChecker(checker *mockVolumeHealthChecker) func(*readTestOptions) {
	return func(o *readTestOptions) { o.healthChecker = checker }
}

func readWithVolumeID(id domain.VolumeID) func(*readTestOptions) {
	return func(o *readTestOptions) { o.volumeID = id }
}

// --- Tests ---

func TestReadChunkHandler_Success_ByChunkID(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 3)

	repo := &mockChunkRepository{
		getFunc: func(ctx context.Context, id domain.ChunkID) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newReadTestHandler(readWithRepo(repo))

	output, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
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

	if output.Handle == nil {
		t.Fatal("expected handle in output")
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}

	if output.VolumeHealth.State != domain.VolumeStateOK {
		t.Errorf("expected volume state OK, got %v", output.VolumeHealth.State)
	}

	if output.CheckVolumeHealthFunc == nil {
		t.Fatal("expected CheckVolumeHealthFunc in output")
	}
}

func TestReadChunkHandler_Success_ByWriterKey(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 500, 1)

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newReadTestHandler(readWithRepo(repo))

	output, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
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

	if output.Handle == nil {
		t.Fatal("expected handle in output")
	}
}

func TestReadChunkHandler_Success_CheckVolumeHealthFunc(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 100, 1)

	callCount := 0
	checker := &mockVolumeHealthChecker{
		checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
			callCount++
			return &domain.VolumeHealth{State: domain.VolumeStateOK}
		},
	}

	repo := &mockChunkRepository{
		getFunc: func(ctx context.Context, id domain.ChunkID) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newReadTestHandler(readWithRepo(repo), readWithHealthChecker(checker))

	output, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		ChunkID: existing.ID[:],
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	countBefore := callCount
	health := output.CheckVolumeHealthFunc()
	if health == nil {
		t.Fatal("expected volume health from func")
	}

	if callCount != countBefore+1 {
		t.Error("expected CheckVolumeHealthFunc to call VolumeHealthChecker")
	}
}

func TestReadChunkHandler_Success_ChunkIDTakesPrecedence(t *testing.T) {
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

	setup := newReadTestHandler(readWithRepo(repo))

	// Both provided — ChunkID should win
	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
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

func TestReadChunkHandler_InvalidChunkID(t *testing.T) {
	setup := newReadTestHandler()

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		ChunkID: []byte("too-short"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrInvalidID) {
		t.Errorf("expected ErrInvalidID, got %v", err)
	}
}

func TestReadChunkHandler_ChunkIDNotFound(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)

	setup := newReadTestHandler() // default repo.Get returns ErrNotFound

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		ChunkID: chunkID[:],
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestReadChunkHandler_WriterKeyNotFound(t *testing.T) {
	setup := newReadTestHandler() // default repo.GetByWriterKey returns ErrNotFound

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("nonexistent"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestReadChunkHandler_NeitherProvided(t *testing.T) {
	setup := newReadTestHandler()

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestReadChunkHandler_WrongState_TempChunk(t *testing.T) {
	temp := &domain.TempChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return temp, nil
		},
	}

	setup := newReadTestHandler(readWithRepo(repo))

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestReadChunkHandler_WrongState_DeletedChunk(t *testing.T) {
	deleted := &domain.DeletedChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0),
		WriterKey: []byte("wk"),
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return deleted, nil
		},
	}

	setup := newReadTestHandler(readWithRepo(repo))

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, chunkerrors.ErrWrongState) {
		t.Errorf("expected ErrWrongState, got %v", err)
	}
}

func TestReadChunkHandler_VolumeNotFound(t *testing.T) {
	existing := &domain.AvailableChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
		WriterKey: []byte("wk"),
		Size:      100,
		Version:   1,
	}

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newReadTestHandler(readWithRepo(repo))

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrNotFound) {
		t.Errorf("expected volume ErrNotFound, got %v", err)
	}
}

func TestReadChunkHandler_VolumeDegraded_Allowed(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newReadTestHandler(
		readWithRepo(repo),
		readWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
			},
		}),
	)

	output, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("wk"),
	})
	if err != nil {
		t.Fatalf("reads should be allowed on degraded volumes, got error: %v", err)
	}

	if output.VolumeHealth.State != domain.VolumeStateDegraded {
		t.Errorf("expected degraded volume health in output, got %v", output.VolumeHealth.State)
	}
}

func TestReadChunkHandler_VolumeFailed(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	setup := newReadTestHandler(
		readWithRepo(repo),
		readWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateFailed}
			},
		}),
	)

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("wk"),
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

	if chunkErr.ChunkVersion() != 1 {
		t.Errorf("expected chunk version 1 in error, got %d", chunkErr.ChunkVersion())
	}

	if chunkErr.ChunkSize() != 1000 {
		t.Errorf("expected chunk size 1000 in error, got %d", chunkErr.ChunkSize())
	}
}

func TestReadChunkHandler_OpenChunkError(t *testing.T) {
	existing := makeAvailableChunk([]byte("wk"), 1000, 1)
	openErr := errors.New("file descriptor exhausted")

	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return existing, nil
		},
	}

	chunkStore := &mockChunkStore{
		openChunkFunc: func(ctx context.Context, id domain.ChunkID) (port.Chunk, error) {
			return nil, openErr
		},
	}

	setup := newReadTestHandler(readWithRepo(repo), readWithChunkStore(chunkStore))

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, openErr) {
		t.Errorf("expected open error, got %v", err)
	}
}

func TestReadChunkHandler_RepoLookupError_Passthrough(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockChunkRepository{
		getByWriterKeyFunc: func(ctx context.Context, writerKey []byte) (domain.Chunk, error) {
			return nil, repoErr
		},
	}

	setup := newReadTestHandler(readWithRepo(repo))

	_, err := setup.handler.HandleReadChunk(context.Background(), &ReadChunkInput{
		WriterKey: []byte("wk"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, repoErr) {
		t.Errorf("expected repo error, got %v", err)
	}

	var chunkErr *chunkerrors.ChunkError
	if errors.As(err, &chunkErr) {
		t.Error("repo lookup error should NOT have ChunkError context")
	}
}
