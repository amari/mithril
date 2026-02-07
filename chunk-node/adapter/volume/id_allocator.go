package volume

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/volume"
	"github.com/cockroachdb/pebble/v2"
)

// pebble/badger/bbolt or file with atomic rename

type pebbleVolumeIDAllocator struct {
	db *pebble.DB

	mu           sync.Mutex
	nextVolumeID uint32
}

var _ volume.VolumeIDAllocator = (*pebbleVolumeIDAllocator)(nil)

func NewPebbleVolumeIDAllocator(db *pebble.DB) (*pebbleVolumeIDAllocator, error) {
	var nextVolumeID uint32 = 1

	data, closer, err := db.Get([]byte("next_volume_id"))

	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
	}

	if err == nil {
		defer closer.Close()

		nextVolumeID = bytesToUint32(data)
	}

	return &pebbleVolumeIDAllocator{
		db:           db,
		nextVolumeID: nextVolumeID,
	}, nil
}

func (a *pebbleVolumeIDAllocator) AllocateVolumeID(_ context.Context) (domain.VolumeID, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	volumeID := domain.VolumeID(a.nextVolumeID)
	nextVolumeID := a.nextVolumeID + 1

	if nextVolumeID > 0xFFFF {
		return 0, chunkstoreerrors.ErrVolumeIDExhausted
	}

	err := a.db.Set([]byte("next_volume_id"), uint32ToBytes(nextVolumeID), pebble.Sync)
	if err != nil {
		return 0, err
	}

	a.nextVolumeID = nextVolumeID

	return volumeID, nil
}

func uint32ToBytes(n uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, n)
}

func bytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
