package volume

type VolumeLabelCollector interface {
	CollectVolumeLabels() (map[string]string, error)
	Close() error
}
