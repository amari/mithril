package domain

type ChunkState int

const (
	ChunkStateTemp ChunkState = iota
	ChunkStateAvailable
	ChunkStateDeleted
)

func (cs ChunkState) String() string {
	switch cs {
	case ChunkStateTemp:
		return "pending"
	case ChunkStateAvailable:
		return "stored"
	case ChunkStateDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}
