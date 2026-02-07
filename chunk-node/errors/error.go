package errors

type ChunkError struct {
	Cause error

	Chunk *Chunk
}

func (e *ChunkError) Error() string {
	return e.Cause.Error()
}

func (e *ChunkError) Unwrap() error {
	return e.Cause
}

type VolumeError struct {
	Cause error

	Volume *Volume
}

func (e *VolumeError) Error() string {
	return e.Cause.Error()
}

func (e *VolumeError) Unwrap() error {
	return e.Cause
}
