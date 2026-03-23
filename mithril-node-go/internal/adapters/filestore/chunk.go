package adaptersfilestore

import (
	"context"
	"io"

	adaptersfilesystem "github.com/amari/mithril/mithril-node-go/internal/adapters/filesystem"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type ChunkStorage struct {
	*SysChunkStorage
}

var _ domain.ChunkStorage = (*SysChunkStorage)(nil)

func NewChunkStorage(root *adaptersfilesystem.Root, bufferSize int) (*ChunkStorage, error) {
	sys, err := NewSysChunkStorage(root, bufferSize)
	if err != nil {
		return nil, err
	}

	return &ChunkStorage{
		SysChunkStorage: sys,
	}, nil
}

func (s *ChunkStorage) Open(ctx context.Context, id domain.ChunkID) (domain.ChunkHandle, error) {
	doneCh := make(chan struct {
		handle domain.ChunkHandle
		err    error
	}, 1)

	go func() {
		defer close(doneCh)

		h, err := s.SysChunkStorage.Open(ctx, id)

		doneCh <- struct {
			handle domain.ChunkHandle
			err    error
		}{h, err}
	}()

	select {
	case res := <-doneCh:
		return res.handle, res.err
	case <-ctx.Done():
		go func() {
			res, ok := <-doneCh
			if ok && res.handle != nil {
				res.handle.Close()
			}
		}()

		return nil, ctx.Err()
	}
}

func (s *ChunkStorage) Create(ctx context.Context, id domain.ChunkID, opts domain.CreateChunkOptions) error {
	doneCh := make(chan error, 1)

	go func() {
		defer close(doneCh)

		doneCh <- s.SysChunkStorage.Create(ctx, id, opts)
	}()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChunkStorage) Put(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, opts domain.PutChunkOptions) error {
	doneCh := make(chan error, 1)

	go func() {
		defer close(doneCh)

		doneCh <- s.SysChunkStorage.Put(ctx, id, r, n, opts)
	}()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChunkStorage) Append(ctx context.Context, id domain.ChunkID, knownSize int64, r io.Reader, n int64, opts domain.AppendChunkOptions) error {
	doneCh := make(chan error, 1)

	go func() {
		defer close(doneCh)

		doneCh <- s.SysChunkStorage.Append(ctx, id, knownSize, r, n, opts)
	}()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChunkStorage) Delete(ctx context.Context, id domain.ChunkID) error {
	doneCh := make(chan error, 1)

	go func() {
		defer close(doneCh)

		doneCh <- s.SysChunkStorage.Delete(ctx, id)
	}()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChunkStorage) ShrinkToFit(ctx context.Context, id domain.ChunkID, knownSize int64, opts domain.ShrinkChunkToFitOptions) error {
	doneCh := make(chan error, 1)

	go func() {
		defer close(doneCh)

		doneCh <- s.SysChunkStorage.ShrinkToFit(ctx, id, knownSize, opts)
	}()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChunkStorage) Exists(ctx context.Context, id domain.ChunkID) (bool, error) {
	doneCh := make(chan struct {
		ok  bool
		err error
	}, 1)

	go func() {
		defer close(doneCh)

		ok, err := s.SysChunkStorage.Exists(ctx, id)

		doneCh <- struct {
			ok  bool
			err error
		}{ok, err}
	}()

	select {
	case res := <-doneCh:
		return res.ok, res.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
