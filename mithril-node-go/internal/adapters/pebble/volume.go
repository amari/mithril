package adapterspebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/amari/mithril/mithril-node-go/internal/adapters/fdbtuple"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/cockroachdb/pebble/v2"
)

type VolumeIDCounter struct {
	db *pebble.DB

	mu    sync.RWMutex
	value uint32
}

var _ domain.VolumeIDCounter = (*VolumeIDCounter)(nil)

func NewVolumeIDCounter(db *pebble.DB) *VolumeIDCounter {
	return &VolumeIDCounter{
		db: db,
	}
}

func (c *VolumeIDCounter) Start() error {
	value, err := c.getCurrentValue()
	if err != nil {
		return err
	}

	c.value = value

	return nil
}

func (c *VolumeIDCounter) Next() (domain.VolumeID, error) {
	value, err := c.incrementValue()
	if err != nil {
		return 0, err
	}

	return domain.VolumeID(value), nil
}

func (c *VolumeIDCounter) getCurrentValue() (uint32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	raw, closer, err := c.db.Get(fdbtuple.Tuple{"counter", "volume", "id"}.Pack())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 1, nil
		}

		return 0, fmt.Errorf("%w: %v", ErrPebbleOperationFailed, err)
	}
	defer closer.Close()

	value := binary.LittleEndian.Uint32(raw)

	return value, nil
}

func (c *VolumeIDCounter) incrementValue() (uint32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	value := c.value
	newValue := value + 1

	if newValue > 0xffff {
		return 0, ErrPebbleCounterOverflow
	}

	err := c.db.Set(fdbtuple.Tuple{"counter", "volume", "id"}.Pack(), binary.LittleEndian.AppendUint32(nil, newValue), nil)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrPebbleOperationFailed, err)
	}

	c.value = newValue

	return value, nil
}
