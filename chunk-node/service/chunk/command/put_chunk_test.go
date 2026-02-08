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
	handler       *PutChunkHandler
	repo          *mockChunkRepository
	chunkStore    *mockChunkStore
	healthChecker *mockVolumeHealthChecker
}

func newPutTestHandler(opts ...func(*putTestOptions)) *putTestSetup {
	o := &putTestOptions{
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

	handler := &PutChunkHandler{
		Repo:                   o.repo,
		IDGen:                  o.idGen,
		VolumeManager:          volumeManager,
		VolumePicker:           o.volumePicker,
		NowFunc:                o.nowFunc,
		NodeIdentityRepository: o.nodeIdentityRepo,
		VolumeHealthChecker:    o.volumeHealthChecker,
		VolumeStatsProvider:    o.volumeStatsProvider,
	}

	return &putTestSetup{
		handler:       handler,
		repo:          o.repo,
		chunkStore:    o.chunkStore,
		healthChecker: o.volumeHealthChecker,
	}
}

type putTestOptions struct {
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

func putWithVolumeStatsProvider(provider *mockVolumeStatsProvider) func(*putTestOptions) {
	return func(o *putTestOptions) { o.volumeStatsProvider = provider }
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
	chunkStore := &mockChunkStore{}
	repo := &mockChunkRepository{}

	setup := newPutTestHandler(
		putWithRepo(repo),
		putWithChunkStore(chunkStore),
	)

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
	if len(repo.storedChunks) != 2 {
		t.Errorf("expected 2 stored chunks (temp + available), got %d", len(repo.storedChunks))
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

func TestPutChunkHandler_FreshCreate_NodeIdentityError(t *testing.T) {
	nodeErr := errors.New("node identity unavailable")

	setup := newPutTestHandler(
		putWithNodeIdentityRepo(&mockNodeIdentityRepository{
			loadNodeIdentityFunc: func(ctx context.Context) (*domain.NodeIdentity, error) {
				return nil, nodeErr
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

	if !errors.Is(err, nodeErr) {
		t.Errorf("expected node identity error, got %v", err)
	}
}

func TestPutChunkHandler_FreshCreate_VolumePickerError(t *testing.T) {
	pickerErr := errors.New("no volumes available")

	setup := newPutTestHandler(
		putWithVolumePicker(&mockVolumePicker{
			pickVolumeIDFunc: func(opts portvolume.PickVolumeIDOptions) (domain.VolumeID, error) {
				return 0, pickerErr
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

	if !errors.Is(err, pickerErr) {
		t.Errorf("expected picker error, got %v", err)
	}
}

func TestPutChunkHandler_FreshCreate_VolumeDegraded(t *testing.T) {
	setup := newPutTestHandler(
		putWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
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

	if !errors.Is(err, volumeerrors.ErrDegraded) {
		t.Errorf("expected ErrDegraded, got %v", err)
	}
}

func TestPutChunkHandler_FreshCreate_VolumeFailed(t *testing.T) {
	setup := newPutTestHandler(
		putWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateFailed}
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

	if !errors.Is(err, volumeerrors.ErrFailed) {
		t.Errorf("expected ErrFailed, got %v", err)
	}
}

func TestPutChunkHandler_FreshCreate_IDGeneratorError(t *testing.T) {
	idErr := errors.New("id generator exhausted")

	setup := newPutTestHandler(
		putWithIDGen(&mockChunkIDGenerator{
			nextIDFunc: func(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error) {
				return domain.ChunkID{}, idErr
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

	if !errors.Is(err, idErr) {
		t.Errorf("expected id generator error, got %v", err)
	}
}

func TestPutChunkHandler_FreshCreate_PutChunkStoreError(t *testing.T) {
	diskErr := errors.New("disk I/O error")

	chunkStore := &mockChunkStore{
		putChunkFunc: func(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error {
			return diskErr
		},
	}

	setup := newPutTestHandler(putWithChunkStore(chunkStore))

	_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader([]byte("data")),
		BodySize: 4,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, diskErr) {
		t.Errorf("expected disk I/O error, got %v", err)
	}
}

func TestPutChunkHandler_FreshCreate_StoreAvailableError(t *testing.T) {
	storeErr := errors.New("repo unavailable")
	callCount := 0

	repo := &mockChunkRepository{
		storeFunc: func(ctx context.Context, chunk domain.Chunk) error {
			callCount++
			// First store (temp) succeeds, second store (available) fails
			if callCount >= 2 {
				return storeErr
			}
			return nil
		},
	}

	setup := newPutTestHandler(putWithRepo(repo))

	_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader([]byte("data")),
		BodySize: 4,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, storeErr) {
		t.Errorf("expected store error, got %v", err)
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

// --- Tests: Idempotency — Existing Temp, Chunk Exists On Disk ---

func TestPutChunkHandler_Idempotency_ExistingTemp_ChunkExists(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("wk"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	chunkStore := &mockChunkStore{
		chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
			return true, nil // Chunk already exists on disk
		},
	}

	setup := newPutTestHandler(
		putWithRepo(repoReturningChunk(existingTemp)),
		putWithChunkStore(chunkStore),
	)

	output, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader([]byte("ignored")),
		BodySize: 7,
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

	if output.Chunk.Size != 0 {
		t.Errorf("expected size 0, got %d", output.Chunk.Size)
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}

	// Should store available (promote), but NOT call PutChunk on disk
	if len(setup.repo.storedChunks) != 1 {
		t.Fatalf("expected 1 stored chunk (available), got %d", len(setup.repo.storedChunks))
	}

	stored := setup.repo.storedChunks[0]
	if !stored.IsAvailable() {
		t.Error("expected stored chunk to be AvailableChunk")
	}
}

// --- Tests: Idempotency — Existing Temp, Chunk NOT On Disk ---

func TestPutChunkHandler_Idempotency_ExistingTemp_ChunkNotExists(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("wk"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	var putCalled bool
	chunkStore := &mockChunkStore{
		chunkExistsFunc: func(ctx context.Context, id domain.ChunkID) (bool, error) {
			return false, nil
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

	if !putCalled {
		t.Error("expected PutChunk to be called on chunk store")
	}

	// Should store temp + available = 2 stored chunks
	if len(setup.repo.storedChunks) != 2 {
		t.Errorf("expected 2 stored chunks (temp + available), got %d", len(setup.repo.storedChunks))
	}

	if output.VolumeHealth == nil {
		t.Fatal("expected volume health in output")
	}
}

// --- Tests: Idempotency — Existing Temp, Error Paths ---

func TestPutChunkHandler_ExistingTemp_VolumeNotFound(t *testing.T) {
	// Chunk on volume 99 which is not registered
	existingTemp := &domain.TempChunk{
		ID:        domain.NewChunkID(time.Now().UnixMilli(), 1, 99, 0),
		WriterKey: []byte("wk"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	setup := newPutTestHandler(putWithRepo(repoReturningChunk(existingTemp)))

	_, err := setup.handler.HandlePutChunk(context.Background(), &PutChunkInput{
		WriteKey: []byte("wk"),
		Body:     bytes.NewReader(nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(err, volumeerrors.ErrNotFound) {
		t.Errorf("expected volume ErrNotFound, got %v", err)
	}
}

func TestPutChunkHandler_ExistingTemp_VolumeDegraded(t *testing.T) {
	chunkID := domain.NewChunkID(time.Now().UnixMilli(), 1, 1, 0)
	existingTemp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: []byte("wk"),
		CreatedAt: time.Now().Add(-time.Hour),
	}

	setup := newPutTestHandler(
		putWithRepo(repoReturningChunk(existingTemp)),
		putWithHealthChecker(&mockVolumeHealthChecker{
			checkVolumeHealthFunc: func(v domain.VolumeID) *domain.VolumeHealth {
				return &domain.VolumeHealth{State: domain.VolumeStateDegraded}
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

	if !errors.Is(err, volumeerrors.ErrDegraded) {
		t.Errorf("expected ErrDegraded, got %v", err)
	}

	// Admission errors on existing temp should be wrapped with ChunkError
	var chunkErr *chunkerrors.ChunkError
	if !errors.As(err, &chunkErr) {
		t.Fatal("expected ChunkError context on degraded volume for existing temp")
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
			return true, nil // chunk exists on disk, promotes to available
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
