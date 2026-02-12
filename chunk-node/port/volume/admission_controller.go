package volume

// VolumeAdmissionController gates volume operations.
//
// Implementations determine whether read, write, or stat operations should
// proceed on a given volume.
type VolumeAdmissionController interface {
	// AdmitWrite determines if a write operation should proceed.
	// Returns nil to allow the operation, or an error to reject it.
	AdmitWrite(id ID) error

	// AdmitRead determines if a read operation should proceed.
	// Returns nil to allow the operation, or an error to reject it.
	AdmitRead(id ID) error

	// AdmitStat determines if a stat operation should proceed.
	// Returns nil to allow the operation, or an error to reject it.
	AdmitStat(id ID) error
}
