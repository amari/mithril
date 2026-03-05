package applicationservices

import "github.com/amari/mithril/mithril-node-go/internal/domain"

type VolumeMetricsExporter interface {
	Export(volumes []domain.VolumeHandle)
}
