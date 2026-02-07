package domain

import (
	"encoding/base64"
	"encoding/binary"
	"time"
)

type ChunkID [16]byte

func NewChunkID(unixMilli int64, nodeID uint32, volumeID uint16, sequence uint16) ChunkID {
	var id ChunkID

	binary.BigEndian.PutUint64(id[0:], uint64(unixMilli))
	binary.BigEndian.PutUint32(id[8:], nodeID)
	binary.BigEndian.PutUint16(id[12:], volumeID)
	binary.BigEndian.PutUint16(id[14:], sequence)

	return id
}

func (id ChunkID) UnixMilli() int64 {
	return int64(binary.BigEndian.Uint64(id[0:]))
}

func (id ChunkID) Time() time.Time {
	return time.UnixMilli(id.UnixMilli())
}

func (id ChunkID) NodeID() NodeID {
	return NodeID(binary.BigEndian.Uint32(id[8:]))
}

func (id ChunkID) VolumeID() VolumeID {
	return VolumeID(binary.BigEndian.Uint16(id[12:]))
}

func (id ChunkID) Sequence() uint16 {
	return binary.BigEndian.Uint16(id[14:])
}

func ParseID(s string) (ChunkID, error) {
	data, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ChunkID{}, err
	}

	var id ChunkID

	copy(id[:], data)

	return id, nil
}

func (id ChunkID) String() string {
	return base64.RawURLEncoding.EncodeToString(id[:])
}
