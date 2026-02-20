package volume

import "context"

type VolumeLabelCollector interface {
	CollectVolumeLabels() (map[string]string, error)

	Watch(watchCtx context.Context) <-chan struct{}
}
