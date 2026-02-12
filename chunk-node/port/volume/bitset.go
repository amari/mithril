package volume

type VolumeBitset interface {
	Add(id ID) bool
	Remove(id ID) bool
	Contains(id ID) bool
	IsEmpty() bool
}
