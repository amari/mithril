package adaptersotel

import (
	"context"
	"sync"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type VolumeMetricType string

const (
	Counter       VolumeMetricType = "counter"
	Gauge         VolumeMetricType = "gauge"
	UpDownCounter VolumeMetricType = "updowncounter"
)

type VolumeMetric struct {
	Name            string
	Description     string
	Unit            string
	Type            VolumeMetricType
	OS              []string
	Int64Callback   func(ctx context.Context, volume domain.VolumeHandle) (int64, bool)
	Float64Callback func(ctx context.Context, volume domain.VolumeHandle) (float64, bool)
}

type VolumeMetricsExporter struct {
	mu              sync.RWMutex
	exportedVolumes []domain.VolumeHandle

	meter metric.Meter
}

var _ applicationservices.VolumeMetricsExporter = (*VolumeMetricsExporter)(nil)

func NewVolumeMetricsExporter(mp metric.MeterProvider) *VolumeMetricsExporter {
	meter := mp.Meter("")

	return &VolumeMetricsExporter{
		meter: meter,
	}
}

func (e *VolumeMetricsExporter) Export(volumes []domain.VolumeHandle) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.exportedVolumes = volumes
}

func (e *VolumeMetricsExporter) start() error {
	metrics := []VolumeMetric{
		{
			Name: "storage.volume.free",
			Type: Gauge,
			Unit: "bytes",
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetSpaceUtilizationStatisticsProvider().Get().Value.FreeBytes, true
			},
		},
		{
			Name: "storage.volume.total",
			Type: Gauge,
			Unit: "bytes",
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetSpaceUtilizationStatisticsProvider().Get().Value.TotalBytes, true
			},
		},
		{
			Name: "storage.volume.used",
			Type: Gauge,
			Unit: "bytes",
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetSpaceUtilizationStatisticsProvider().Get().Value.UsedBytes, true
			},
		},
		{
			Name: "storage.volume.read_bytes",
			Type: Counter,
			OS:   []string{"darwin"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetIOKitIOBlockStorageDriverStatisticsProvider().Get().Value.BytesRead, true
			},
		},
		{
			Name: "storage.volume.write_bytes",
			Type: Counter,
			OS:   []string{"darwin"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetIOKitIOBlockStorageDriverStatisticsProvider().Get().Value.BytesWritten, true
			},
		},

		// Darwin-only
		{
			Name: "storage.volume.read_ops",
			Type: Counter,
			OS:   []string{"darwin"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetIOKitIOBlockStorageDriverStatisticsProvider().Get().Value.Reads, true
			},
		},
		{
			Name: "storage.volume.write_ops",
			Type: Counter,
			OS:   []string{"darwin"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetIOKitIOBlockStorageDriverStatisticsProvider().Get().Value.Writes, true
			},
		},
		{
			Name:        "storage.volume.read_latency",
			Type:        Counter,
			Description: "The number of nanoseconds of latency during reads since the block storage driver was instantiated.",
			Unit:        "ns",
			OS:          []string{"darwin"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.
					GetIOKitIOBlockStorageDriverStatisticsProvider().
					Get().
					Value.
					LatentReadTime, true
			},
		},
		{
			Name:        "storage.volume.write_latency",
			Type:        Counter,
			Description: "The number of nanoseconds of latency during writes since the block storage driver was instantiated.",
			Unit:        "ns",
			OS:          []string{"darwin"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.
					GetIOKitIOBlockStorageDriverStatisticsProvider().
					Get().
					Value.
					LatentWriteTime, true
			},
		},

		// Linux-only
		{
			Name: "storage.volume.read_ops",
			Type: Counter,
			OS:   []string{"linux"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetLinuxBlockLayerStatisticsProvider().Get().Value.Reads, true
			},
		},
		{
			Name: "storage.volume.write_ops",
			Type: Counter,
			OS:   []string{"linux"},
			Int64Callback: func(ctx context.Context, volume domain.VolumeHandle) (int64, bool) {
				return volume.GetLinuxBlockLayerStatisticsProvider().Get().Value.Writes, true
			},
		},
	}

	for _, m := range metrics {
		switch m.Type {
		case Counter:
			if m.Int64Callback != nil {
				_, err := e.meter.Int64ObservableCounter(
					m.Name,
					metric.WithDescription(m.Description),
					metric.WithUnit(m.Unit),
					metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
						e.mu.RLock()
						exportedVolumes := e.exportedVolumes
						e.mu.RUnlock()

						for _, volume := range exportedVolumes {
							value, ok := m.Int64Callback(ctx, volume)
							if ok {
								obs.Observe(
									value,
									metric.WithAttributes(
										attribute.Int64("node_id", int64(volume.Info().Node)),
										attribute.Int64("volume_id", int64(volume.Info().ID)),
									),
									metric.WithAttributes(volume.GetOTelAttributeProvider().Get()...),
								)
							}
						}

						return nil
					}),
				)
				if err != nil {
					return err
				}
			} else if m.Float64Callback != nil {
				_, err := e.meter.Float64ObservableCounter(
					m.Name,
					metric.WithDescription(m.Description),
					metric.WithUnit(m.Unit),
					metric.WithFloat64Callback(func(ctx context.Context, obs metric.Float64Observer) error {
						e.mu.RLock()
						defer e.mu.RUnlock()

						for _, volume := range e.exportedVolumes {
							value, ok := m.Float64Callback(ctx, volume)
							if ok {
								obs.Observe(
									value,
									metric.WithAttributes(
										attribute.Int64("node_id", int64(volume.Info().Node)),
										attribute.Int64("volume_id", int64(volume.Info().ID)),
									),
									metric.WithAttributes(volume.GetOTelAttributeProvider().Get()...),
								)
							}
						}

						return nil
					}),
				)
				if err != nil {
					return err
				}
			} else {
				return nil
			}
		case Gauge:
			if m.Int64Callback != nil {
				_, err := e.meter.Int64ObservableGauge(
					m.Name,
					metric.WithDescription(m.Description),
					metric.WithUnit(m.Unit),
					metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
						e.mu.RLock()
						defer e.mu.RUnlock()

						for _, volume := range e.exportedVolumes {
							value, ok := m.Int64Callback(ctx, volume)
							if ok {
								obs.Observe(
									value,
									metric.WithAttributes(
										attribute.Int64("node_id", int64(volume.Info().Node)),
										attribute.Int64("volume_id", int64(volume.Info().ID)),
									),
									metric.WithAttributes(volume.GetOTelAttributeProvider().Get()...),
								)
							}
						}

						return nil
					}),
				)
				if err != nil {
					return err
				}
			} else if m.Float64Callback != nil {
				_, err := e.meter.Float64ObservableGauge(
					m.Name,
					metric.WithDescription(m.Description),
					metric.WithUnit(m.Unit),
					metric.WithFloat64Callback(func(ctx context.Context, obs metric.Float64Observer) error {
						e.mu.RLock()
						defer e.mu.RUnlock()

						for _, volume := range e.exportedVolumes {
							value, ok := m.Float64Callback(ctx, volume)
							if ok {
								obs.Observe(
									value,
									metric.WithAttributes(
										attribute.Int64("node_id", int64(volume.Info().Node)),
										attribute.Int64("volume_id", int64(volume.Info().ID)),
									),
									metric.WithAttributes(volume.GetOTelAttributeProvider().Get()...),
								)
							}
						}

						return nil
					}),
				)
				if err != nil {
					return err
				}
			} else {
				return nil
			}
		case UpDownCounter:
			if m.Int64Callback != nil {
				_, err := e.meter.Int64ObservableUpDownCounter(
					m.Name,
					metric.WithDescription(m.Description),
					metric.WithUnit(m.Unit),
					metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
						e.mu.RLock()
						defer e.mu.RUnlock()

						for _, volume := range e.exportedVolumes {
							value, ok := m.Int64Callback(ctx, volume)
							if ok {
								obs.Observe(
									value,
									metric.WithAttributes(
										attribute.Int64("node_id", int64(volume.Info().Node)),
										attribute.Int64("volume_id", int64(volume.Info().ID)),
									),
									metric.WithAttributes(volume.GetOTelAttributeProvider().Get()...),
								)
							}
						}

						return nil
					}),
				)
				if err != nil {
					return err
				}
			} else if m.Float64Callback != nil {
				_, err := e.meter.Float64ObservableUpDownCounter(
					m.Name,
					metric.WithDescription(m.Description),
					metric.WithUnit(m.Unit),
					metric.WithFloat64Callback(func(ctx context.Context, obs metric.Float64Observer) error {
						e.mu.RLock()
						defer e.mu.RUnlock()

						for _, volume := range e.exportedVolumes {
							value, ok := m.Float64Callback(ctx, volume)
							if ok {
								obs.Observe(
									value,
									metric.WithAttributes(
										attribute.Int64("node_id", int64(volume.Info().Node)),
										attribute.Int64("volume_id", int64(volume.Info().ID)),
									),
									metric.WithAttributes(volume.GetOTelAttributeProvider().Get()...),
								)
							}
						}

						return nil
					}),
				)
				if err != nil {
					return err
				}
			} else {
				return nil
			}
		}
	}

	return nil
}

func (e *VolumeMetricsExporter) stop() error {
	return nil
}
