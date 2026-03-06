package adaptersfilesystem

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/rs/zerolog"
)

type WallClockFenceFile struct {
	path     string
	interval time.Duration
	logger   *zerolog.Logger

	mu     sync.RWMutex
	lastMs int64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ applicationservices.ClockFence = (*WallClockFenceFile)(nil)

func NewWallClockFenceFile(logger *zerolog.Logger, path string, interval time.Duration) *WallClockFenceFile {
	return &WallClockFenceFile{
		logger:   logger,
		path:     path,
		interval: interval,
	}
}

func (w *WallClockFenceFile) UnixMilli(ctx context.Context) (int64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.lastMs == 0 {
		return 0, errors.New("clock fence not initialized")
	}
	return w.lastMs, nil
}

type state struct {
	LastMs int64 `json:"last_ms"`
}

func (w *WallClockFenceFile) load() (int64, error) {
	data, err := os.ReadFile(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, err
	}
	return s.LastMs, nil
}

func (w *WallClockFenceFile) store(v int64) error {
	tmp := w.path + ".tmp"

	data, err := json.Marshal(state{LastMs: v})
	if err != nil {
		return err
	}

	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, w.path)
}

// --- lifecycle ---

func (w *WallClockFenceFile) start(ctx context.Context) error {
	// Load last persisted value
	last, err := w.load()
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()

	// Strict regression check
	if last != 0 && now < last {
		return errors.New("clock moved backwards at startup")
	}

	w.mu.Lock()
	w.lastMs = now
	w.mu.Unlock()

	if err := w.store(now); err != nil {
		return err
	}

	// Start heartbeat loop
	runCtx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	w.wg.Add(1)
	go w.loop(runCtx)

	w.logger.Info().Msg("clock fence: " + w.path)

	return nil
}

func (w *WallClockFenceFile) loop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick()
		}
	}
}

func (w *WallClockFenceFile) tick() {
	now := time.Now().UnixMilli()

	w.mu.Lock()
	defer w.mu.Unlock()

	if now < w.lastMs {
		panic("clock moved backwards during runtime")
	}

	w.lastMs = now
	_ = w.store(now)
}

func (w *WallClockFenceFile) stop(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()

	w.mu.RLock()
	last := w.lastMs
	w.mu.RUnlock()

	if last == 0 {
		return nil
	}
	return w.store(last)
}
