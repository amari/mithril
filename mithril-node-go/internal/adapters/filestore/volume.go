package adaptersfilestore

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	adaptersfilesystem "github.com/amari/mithril/mithril-node-go/internal/adapters/filesystem"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"go.opentelemetry.io/otel/attribute"
)

type VolumeHandle struct {
	node   domain.NodeID
	volume domain.VolumeID
	path   string

	root *adaptersfilesystem.Root

	chunkStorage                                *ChunkStorage
	characteristicsProvider                     *VolumeCharacteristicsProvider
	labelSources                                []domain.VolumeLabelSource
	otelAttributeProvider                       *VolumeOTelAttributeProvider
	structuredLoggingFieldsProvider             *VolumeStructuredLoggingFieldsProvider
	spaceUtilizationStatisticsProvider          *SpaceUtilizationVolumeStatisticsProvider
	iokitIOBlockStorageDriverStatisticsProvider *IOKitIOBlockStorageDriverVolumeStatisticsProvider
	linuxBlockLayerStatisticsProvider           *LinuxBlockLayerVolumeStatisticsProvider
	statusProvider                              *VolumeStatusProvider

	started atomic.Bool
}

var _ domain.VolumeHandle = (*VolumeHandle)(nil)

func NewVolumeHandle(node domain.NodeID, volume domain.VolumeID, path string) *VolumeHandle {
	return &VolumeHandle{
		node:   node,
		volume: volume,
		path:   path,
	}
}

func (h *VolumeHandle) Close() error {
	_ = h.stop()

	return nil
}

func (h *VolumeHandle) Info() domain.VolumeInfo {
	return domain.VolumeInfo{
		ID:   h.volume,
		Node: h.node,
	}
}

func (h *VolumeHandle) GetChunkStorage() domain.ChunkStorage { return h.chunkStorage }

func (h *VolumeHandle) GetCharacteristicsProvider() domain.VolumeCharacteristicsProvider {
	return h.characteristicsProvider
}

func (h *VolumeHandle) GetLabelSources() []domain.VolumeLabelSource { return h.labelSources }

func (h *VolumeHandle) GetStatusProvider() domain.VolumeStatusProvider {
	return h.statusProvider
}

func (h *VolumeHandle) GetStructuredLoggingFieldsProvider() domain.VolumeStructuredLoggingFieldsProvider {
	return h.structuredLoggingFieldsProvider
}

func (h *VolumeHandle) GetOTelAttributeProvider() domain.VolumeOTelAttributeProvider {
	return h.otelAttributeProvider
}

func (h *VolumeHandle) GetIOKitIOBlockStorageDriverStatisticsProvider() domain.VolumeStatisticsProvider[domain.IOKitIOBlockStorageDriverStatistics] {
	return h.iokitIOBlockStorageDriverStatisticsProvider
}

func (h *VolumeHandle) GetLinuxBlockLayerStatisticsProvider() domain.VolumeStatisticsProvider[domain.LinuxBlockLayerStatistics] {
	return h.linuxBlockLayerStatisticsProvider
}

func (h *VolumeHandle) GetSpaceUtilizationStatisticsProvider() domain.VolumeStatisticsProvider[domain.SpaceUtilizationStatistics] {
	return h.spaceUtilizationStatisticsProvider
}

func (h *VolumeHandle) start() error {
	if h.started.Load() {
		return nil
	}

	undoFuncs := []func(){}
	defer func() {
		for _, undoFunc := range slices.Backward(undoFuncs) {
			undoFunc()
		}
	}()

	root, err := adaptersfilesystem.OpenRoot(h.path)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	undoFuncs = append(undoFuncs, func() { _ = root.Close() })

	chunkStorage, err := NewChunkStorage(root, 0)
	if err != nil {
		return err
	}
	if err = chunkStorage.start(); err != nil {
		return err
	}
	undoFuncs = append(undoFuncs, func() { _ = chunkStorage.stop() })

	characteristicsProvider := NewVolumeCharacteristicsProvider(h.path)
	if err = characteristicsProvider.start(); err != nil {
		return err
	}
	undoFuncs = append(undoFuncs, func() { _ = characteristicsProvider.stop() })

	characteristicsLabelSource := NewVolumeCharacteristicsLabelSource(characteristicsProvider)
	if err = characteristicsLabelSource.start(); err != nil {
		return err
	}
	undoFuncs = append(undoFuncs, func() { _ = characteristicsLabelSource.stop() })

	otelAttributeProvider := NewVolumeOTelAttributeProvider(characteristicsProvider, h.path)
	if err = otelAttributeProvider.start(); err != nil {
		return err
	}
	undoFuncs = append(undoFuncs, func() { _ = otelAttributeProvider.stop() })

	structuredLoggingFieldsProvider := NewVolumeStructuredLoggingFieldsProvider(characteristicsProvider, h.path)
	if err = structuredLoggingFieldsProvider.start(); err != nil {
		return err
	}
	undoFuncs = append(undoFuncs, func() { _ = structuredLoggingFieldsProvider.stop() })

	spaceUtilizationStatisticsProvider, err := NewSpaceUtilizationVolumeStatisticsProvider(h.path, 250*time.Millisecond)
	if err != nil {
		return err
	}
	if err = spaceUtilizationStatisticsProvider.start(); err != nil {
		return err
	}

	var iokitIOBlockStorageDriverStatisticsProvider *IOKitIOBlockStorageDriverVolumeStatisticsProvider
	if runtime.GOOS == "darwin" {
		iokitIOBlockStorageDriverStatisticsProvider, err = NewIOKitIOBlockStorageDriverVolumeStatisticsProvider(h.path, 250*time.Millisecond)
		if err != nil {
			return err
		}
		if err = iokitIOBlockStorageDriverStatisticsProvider.start(); err != nil {
			return err
		}
		undoFuncs = append(undoFuncs, func() { _ = iokitIOBlockStorageDriverStatisticsProvider.stop() })
		h.iokitIOBlockStorageDriverStatisticsProvider = iokitIOBlockStorageDriverStatisticsProvider
	}

	var linuxBlockLayerStatisticsProvider *LinuxBlockLayerVolumeStatisticsProvider
	if runtime.GOOS == "linux" {
		linuxBlockLayerStatisticsProvider, err = NewLinuxBlockLayerVolumeStatisticsProvider(h.path, 250*time.Millisecond)
		if err != nil {
			return err
		}
		if err = linuxBlockLayerStatisticsProvider.start(); err != nil {
			return err
		}
		undoFuncs = append(undoFuncs, func() { _ = linuxBlockLayerStatisticsProvider.stop() })
		h.linuxBlockLayerStatisticsProvider = linuxBlockLayerStatisticsProvider
	}

	statusProvider := &VolumeStatusProvider{
		chunkStorageHealthController:             chunkStorage.healthController,
		spaceUtilizationSampleVolumeHealthSource: NewSpaceUtilizationSampleVolumeHealthSource(spaceUtilizationStatisticsProvider),
	}
	if runtime.GOOS == "darwin" {
		statusProvider.iokitIOBlockStorageDriverStatisticsProvider = NewIOKitIOBlockStorageDriverStatisticsVolumeHealthSource(iokitIOBlockStorageDriverStatisticsProvider)
	}
	if runtime.GOOS == "linux" {
		statusProvider.linuxBlockLayerStatisticsProvider = NewLinuxBlockLayerStatisticsVolumeHealthSource(linuxBlockLayerStatisticsProvider)
	}
	if err = statusProvider.start(); err != nil {
		return err
	}

	h.root = root
	h.chunkStorage = chunkStorage
	h.characteristicsProvider = characteristicsProvider
	h.labelSources = []domain.VolumeLabelSource{characteristicsLabelSource}
	h.otelAttributeProvider = otelAttributeProvider
	h.structuredLoggingFieldsProvider = structuredLoggingFieldsProvider
	h.spaceUtilizationStatisticsProvider = spaceUtilizationStatisticsProvider
	if runtime.GOOS == "darwin" {
		h.iokitIOBlockStorageDriverStatisticsProvider = iokitIOBlockStorageDriverStatisticsProvider
	}
	if runtime.GOOS == "linux" {
		h.linuxBlockLayerStatisticsProvider = linuxBlockLayerStatisticsProvider
	}
	h.statusProvider = statusProvider
	h.started.Store(true)

	undoFuncs = nil

	return nil
}

func (h *VolumeHandle) stop() error {
	if !h.started.Load() {
		return nil
	}

	_ = h.root.Close()
	_ = h.chunkStorage.stop()
	_ = h.characteristicsProvider.stop()
	_ = h.otelAttributeProvider.stop()
	_ = h.structuredLoggingFieldsProvider.stop()
	_ = h.spaceUtilizationStatisticsProvider.stop()
	if h.iokitIOBlockStorageDriverStatisticsProvider != nil {
		h.iokitIOBlockStorageDriverStatisticsProvider.stop()
	}
	if h.linuxBlockLayerStatisticsProvider != nil {
		h.linuxBlockLayerStatisticsProvider.stop()
	}
	_ = h.statusProvider.stop()
	h.started.Store(false)

	return nil
}

type VolumeCharacteristicsLabelSource struct {
	CharacteristicsProvider domain.VolumeCharacteristicsProvider
	labels                  map[string]string
}

var _ domain.VolumeLabelSource = (*VolumeCharacteristicsLabelSource)(nil)

func NewVolumeCharacteristicsLabelSource(characteristicsProvider domain.VolumeCharacteristicsProvider) *VolumeCharacteristicsLabelSource {
	return &VolumeCharacteristicsLabelSource{
		CharacteristicsProvider: characteristicsProvider,
		labels:                  make(map[string]string),
	}
}

func (s *VolumeCharacteristicsLabelSource) Read() map[string]string {
	return s.labels
}

func (s *VolumeCharacteristicsLabelSource) Watch(watchCtx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}

func (s *VolumeCharacteristicsLabelSource) start() error {
	characteristics := s.CharacteristicsProvider.Get()
	if characteristics == nil {
		return nil
	}

	s.labels = s.buildLabels(characteristics)

	return nil
}

func (s *VolumeCharacteristicsLabelSource) stop() error {
	return nil
}

func (s *VolumeCharacteristicsLabelSource) buildLabels(characteristics *domain.VolumeCharacteristics) map[string]string {
	if characteristics == nil {
		return nil
	}

	labels := make(map[string]string)

	// Medium labels
	switch characteristics.Medium {
	case domain.MediumTypeRotational:
		labels["hdd"] = "true"
	case domain.MediumTypeSolidState:
		labels["ssd"] = "true"
	}

	// Protocol labels
	switch characteristics.Protocol {
	case domain.ProtocolTypeNVMe:
		labels["nvme"] = "true"
	case domain.ProtocolTypeSCSI:
		labels["scsi"] = "true"
	case domain.ProtocolTypeATA:
		labels["ata"] = "true"
	}

	// Interconnect labels
	switch characteristics.Interconnect {
	case domain.InterconnectTypeFibreChannel:
		labels["fibre-channel"] = "true"
	case domain.InterconnectTypeFireWire:
		labels["firewire"] = "true"
	case domain.InterconnectTypeInfiniBand:
		labels["infiniband"] = "true"
	case domain.InterconnectTypePATA:
		labels["pata"] = "true"
	case domain.InterconnectTypePCIExpress:
		labels["pcie"] = "true"
	case domain.InterconnectTypeRDMA:
		labels["rdma"] = "true"
	case domain.InterconnectTypeSAS:
		labels["sas"] = "true"
	case domain.InterconnectTypeSATA:
		labels["sata"] = "true"
	case domain.InterconnectTypeTCP:
		labels["tcp"] = "true"
	case domain.InterconnectTypeUSB:
		labels["usb"] = "true"
	case domain.InterconnectTypeVirtIO:
		labels["virtio"] = "true"
	}

	// Filesystem labels
	if characteristics.Filesystem != "" {
		labels[strings.ToLower(characteristics.Filesystem.String())] = "true"
	}

	// Zoning labels
	switch characteristics.Zoning {
	case domain.ZoningTypeUnknown:
	default:
		labels["zoned"] = "true"

		switch characteristics.Protocol {
		case domain.ProtocolTypeNVMe:
		default:
			labels["smr"] = "true"
		}
	}

	return labels
}

type VolumeStructuredLoggingFieldsProvider struct {
	characteristicsProvider domain.VolumeCharacteristicsProvider
	path                    string

	mu                 sync.RWMutex
	wg                 sync.WaitGroup
	fields             []any
	watchCtx           context.Context
	watchCtxCancelFunc context.CancelFunc
}

var _ domain.VolumeStructuredLoggingFieldsProvider = (*VolumeStructuredLoggingFieldsProvider)(nil)

func NewVolumeStructuredLoggingFieldsProvider(characteristicsProvider domain.VolumeCharacteristicsProvider, path string) *VolumeStructuredLoggingFieldsProvider {
	p := &VolumeStructuredLoggingFieldsProvider{
		characteristicsProvider: characteristicsProvider,
		path:                    path,
	}

	return p
}

func (p *VolumeStructuredLoggingFieldsProvider) Get() []any {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.fields
}

func (p *VolumeStructuredLoggingFieldsProvider) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.watchCtx, p.watchCtxCancelFunc = context.WithCancel(context.Background())

	p.wg.Go(func() { p.updateFields() })

	return nil
}

func (p *VolumeStructuredLoggingFieldsProvider) stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.watchCtxCancelFunc()

	p.wg.Wait()

	return nil
}

func (p *VolumeStructuredLoggingFieldsProvider) updateFields() {
	ch := p.characteristicsProvider.Watch(p.watchCtx)

	for {
		select {
		case <-p.watchCtx.Done():
			return
		case <-ch:
			p.mu.Lock()
			p.fields = p.getFields()
			p.mu.Unlock()
		}
	}
}

func (p *VolumeStructuredLoggingFieldsProvider) getFields() []any {
	characteristics := p.characteristicsProvider.Get()
	if characteristics == nil {
		return nil
	}

	fields := []any{
		"volume.filestore.path", p.path,
	}

	if characteristics.DeviceName != "" {
		fields = append(fields, "volume.device.name", characteristics.DeviceName)
	}

	if characteristics.SerialNumber != "" {
		fields = append(fields, "volume.device.serial", characteristics.SerialNumber)
	}

	if characteristics.Vendor != "" {
		fields = append(fields, "volume.device.vendor", characteristics.Vendor)
	}

	if characteristics.Model != "" {
		fields = append(fields, "volume.device.model", characteristics.Model)
	}

	if characteristics.Medium != "" {
		var medium string
		switch characteristics.Medium {
		case domain.MediumTypeRotational:
			medium = "hdd"
		case domain.MediumTypeSolidState:
			medium = "ssd"
		default:
			medium = ""
		}

		if medium != "" {
			fields = append(fields, "volume.device.medium", medium)
		}
	}

	if characteristics.Interconnect != "" {
		fields = append(fields, "volume.device.interconnect", string(characteristics.Interconnect))
	}

	if characteristics.Protocol != "" {
		fields = append(fields, "volume.device.protocol", string(characteristics.Protocol))
	}

	if characteristics.Filesystem != "" {
		fields = append(fields, "volume.filesystem", strings.ToLower(characteristics.Filesystem.String()))
	}

	if characteristics.Zoning != "" {
		fields = append(fields, "volume.device.zoning", strings.ToLower(string(characteristics.Zoning)))
	}

	return fields
}

type VolumeOTelAttributeProvider struct {
	characteristicsProvider domain.VolumeCharacteristicsProvider
	path                    string

	mu                 sync.RWMutex
	wg                 sync.WaitGroup
	attrs              []attribute.KeyValue
	watchCtx           context.Context
	watchCtxCancelFunc context.CancelFunc
}

var _ domain.VolumeOTelAttributeProvider = (*VolumeOTelAttributeProvider)(nil)

func NewVolumeOTelAttributeProvider(characteristicsProvider domain.VolumeCharacteristicsProvider, path string) *VolumeOTelAttributeProvider {
	p := &VolumeOTelAttributeProvider{
		characteristicsProvider: characteristicsProvider,
		path:                    path,
	}

	return p
}

func (p *VolumeOTelAttributeProvider) Get() []attribute.KeyValue {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.attrs
}

func (p *VolumeOTelAttributeProvider) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.watchCtx, p.watchCtxCancelFunc = context.WithCancel(context.Background())

	p.wg.Go(func() { p.updateAttrs() })

	return nil
}

func (p *VolumeOTelAttributeProvider) stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.watchCtxCancelFunc()

	p.wg.Wait()

	return nil
}

func (p *VolumeOTelAttributeProvider) updateAttrs() {
	ch := p.characteristicsProvider.Watch(p.watchCtx)

	for {
		select {
		case <-p.watchCtx.Done():
			return
		case <-ch:
			p.mu.Lock()
			p.attrs = p.getAttrs()
			p.mu.Unlock()
		}
	}
}

func (p *VolumeOTelAttributeProvider) getAttrs() []attribute.KeyValue {
	characteristics := p.characteristicsProvider.Get()
	if characteristics == nil {
		return nil
	}

	attrs := []attribute.KeyValue{
		attribute.String("volume.filestore.path", p.path),
	}

	if characteristics.DeviceName != "" {
		attrs = append(attrs, attribute.String("volume.device.name", characteristics.DeviceName))
	}

	if characteristics.SerialNumber != "" {
		attrs = append(attrs, attribute.String("volume.device.serial", characteristics.SerialNumber))
	}

	if characteristics.Vendor != "" {
		attrs = append(attrs, attribute.String("volume.device.vendor", characteristics.Vendor))
	}

	if characteristics.Model != "" {
		attrs = append(attrs, attribute.String("volume.device.model", characteristics.Model))
	}

	if characteristics.Medium != "" {
		var medium string
		switch characteristics.Medium {
		case domain.MediumTypeRotational:
			medium = "hdd"
		case domain.MediumTypeSolidState:
			medium = "ssd"
		default:
			medium = ""
		}

		if medium != "" {
			attrs = append(attrs, attribute.String("volume.device.medium", medium))
		}
	}

	if characteristics.Interconnect != "" {
		attrs = append(attrs, attribute.String("volume.device.interconnect", string(characteristics.Interconnect)))
	}

	if characteristics.Protocol != "" {
		attrs = append(attrs, attribute.String("volume.device.protocol", string(characteristics.Protocol)))
	}

	if characteristics.Filesystem != "" {
		attrs = append(attrs, attribute.String("volume.filesystem", strings.ToLower(characteristics.Filesystem.String())))
	}

	if characteristics.Zoning != "" {
		attrs = append(attrs, attribute.String("volume.device.zoning", strings.ToLower(string(characteristics.Zoning))))
	}

	return attrs
}

type VolumeStatusProvider struct {
	chunkStorageHealthController                *ChunkStorageHealthController
	spaceUtilizationSampleVolumeHealthSource    *SpaceUtilizationSampleVolumeHealthSource
	iokitIOBlockStorageDriverStatisticsProvider *IOKitIOBlockStorageDriverStatisticsVolumeHealthSource
	linuxBlockLayerStatisticsProvider           *LinuxBlockLayerStatisticsVolumeHealthSource
}

var _ domain.VolumeStatusProvider = (*VolumeStatusProvider)(nil)

func (p *VolumeStatusProvider) start() error {
	if p.spaceUtilizationSampleVolumeHealthSource != nil {
		if err := p.spaceUtilizationSampleVolumeHealthSource.start(); err != nil {
			return err
		}
	}

	if p.iokitIOBlockStorageDriverStatisticsProvider != nil {
		if err := p.iokitIOBlockStorageDriverStatisticsProvider.start(); err != nil {
			return err
		}
	}

	if p.linuxBlockLayerStatisticsProvider != nil {
		if err := p.linuxBlockLayerStatisticsProvider.start(); err != nil {
			return err
		}
	}

	return nil
}

func (p *VolumeStatusProvider) stop() error {
	if p.spaceUtilizationSampleVolumeHealthSource != nil {
		if err := p.spaceUtilizationSampleVolumeHealthSource.stop(); err != nil {
			return err
		}
	}

	if p.iokitIOBlockStorageDriverStatisticsProvider != nil {
		if err := p.iokitIOBlockStorageDriverStatisticsProvider.stop(); err != nil {
			return err
		}
	}

	if p.linuxBlockLayerStatisticsProvider != nil {
		if err := p.linuxBlockLayerStatisticsProvider.stop(); err != nil {
			return err
		}
	}

	return nil
}

func (p *VolumeStatusProvider) Get() domain.VolumeStatus {
	health := domain.VolumeOK

	if p.chunkStorageHealthController != nil {
		otherHealth := p.chunkStorageHealthController.Get()
		health = max(health, otherHealth)
	}

	if p.spaceUtilizationSampleVolumeHealthSource != nil {
		otherHealth := p.spaceUtilizationSampleVolumeHealthSource.Get()
		health = max(health, otherHealth)
	}

	if p.iokitIOBlockStorageDriverStatisticsProvider != nil {
		otherHealth := p.iokitIOBlockStorageDriverStatisticsProvider.Get()
		health = max(health, otherHealth)
	}

	if p.linuxBlockLayerStatisticsProvider != nil {
		otherHealth := p.linuxBlockLayerStatisticsProvider.Get()
		health = max(health, otherHealth)
	}

	return domain.VolumeStatus{
		Health: health,
	}
}
