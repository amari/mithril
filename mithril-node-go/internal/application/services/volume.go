package applicationservices

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type VolumeService interface {
	Start() error
	Stop(ctx context.Context) error
	AddFileStoreVolume(path string) (domain.VolumeHandle, error)
	RemoveVolume(volume domain.VolumeID) error
	GetVolume(volume domain.VolumeID) (domain.VolumeHandle, error)
}

type volumeService struct {
	nodeIDProvider        domain.NodeIDProvider
	volumeIDCounter       domain.VolumeIDCounter
	fileStoreFormat       domain.FileStoreFormat
	volumeChooser         domain.VolumeChooser
	volumeLabelPublisher  domain.VolumeLabelPublisher
	volumeMetricsExporter VolumeMetricsExporter

	mu                 sync.RWMutex
	wg                 sync.WaitGroup
	watchCtx           context.Context
	watchCtxCancelFunc context.CancelFunc
	handleMap          map[domain.VolumeID]domain.VolumeHandle
	idSlice            []domain.VolumeID
	handleSlice        []domain.VolumeHandle
}

func NewVolumeService(nodeIDProvider domain.NodeIDProvider, volumeIDCounter domain.VolumeIDCounter, fileStoreFormat domain.FileStoreFormat, volumeChooser domain.VolumeChooser, volumeLabelPublisher domain.VolumeLabelPublisher, volumeMetricsExporter VolumeMetricsExporter) VolumeService {
	return &volumeService{
		nodeIDProvider:        nodeIDProvider,
		volumeIDCounter:       volumeIDCounter,
		fileStoreFormat:       fileStoreFormat,
		volumeChooser:         volumeChooser,
		volumeLabelPublisher:  volumeLabelPublisher,
		volumeMetricsExporter: volumeMetricsExporter,
		handleMap:             map[domain.VolumeID]domain.VolumeHandle{},
	}
}

func (s *volumeService) Start() error {
	s.watchCtx, s.watchCtxCancelFunc = context.WithCancel(context.Background())

	return nil
}

func (s *volumeService) Stop(ctx context.Context) error {
	stopCh := make(chan struct{})

	go func() {
		defer close(stopCh)

		func() {
			s.mu.Lock()
			defer s.mu.Unlock()

			s.watchCtxCancelFunc()

			s.volumeChooser.Reset(nil)
			s.volumeMetricsExporter.Export(nil)

			wg := &sync.WaitGroup{}

			for _, h := range s.handleMap {
				wg.Go(func() {
					_ = h.Close()
				})
			}

			s.handleMap = nil
			s.handleSlice = nil

			s.wg.Wait()
		}()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stopCh:
	}

	return nil
}

func (s *volumeService) AddFileStoreVolume(path string) (domain.VolumeHandle, error) {
	st, err := s.fileStoreFormat.Stat(path)
	if err != nil {
		if !errors.Is(err, domain.ErrFileStoreVolumeNotInitialized) {
			return nil, fmt.Errorf("%w: %w", ErrVolumeStatFailed, err)
		}

		node := s.nodeIDProvider.GetNodeID()

		volume, err := s.volumeIDCounter.Next()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrVolumeIDSequenceOverflow, err)
		}

		if err := s.fileStoreFormat.Initialize(node, volume, path); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrVolumeInitializationFailed, err)
		}

		st, err = s.fileStoreFormat.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrVolumeStatFailed, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.handleMap[st.ID]; ok {
		return nil, ErrVolumeAlreadyOpen
	}

	h, err := s.fileStoreFormat.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrVolumeOpenFailed, err)
	}

	s.handleMap[st.ID] = h

	i, _ := slices.BinarySearch(s.idSlice, st.ID)
	s.idSlice = slices.Insert(s.idSlice, i, st.ID)
	s.handleSlice = slices.Insert(s.handleSlice, i, h)

	for _, source := range h.GetLabelSources() {
		ch := source.Watch(s.watchCtx)

		go func() {
			for _ = range ch {
				s.collectAndPublishLabels()
			}

			s.collectAndPublishLabels()
		}()
	}

	s.volumeChooser.Reset(s.idSlice)
	s.volumeMetricsExporter.Export(s.handleSlice)

	return h, nil
}

func (s *volumeService) collectAndPublishLabels() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	allLabels := make(map[domain.VolumeID]map[string]string, len(s.handleMap))

	for id, handle := range s.handleMap {
		for _, source := range handle.GetLabelSources() {
			labels, ok := allLabels[id]
			if !ok {
				labels = map[string]string{}
				allLabels[id] = labels
			}

			maps.Copy(labels, source.Read())
		}
	}

	s.volumeLabelPublisher.Publish(s.nodeIDProvider.GetNodeID(), allLabels)
}

func (s *volumeService) RemoveVolume(volume domain.VolumeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.handleMap[volume]
	if !ok {
		return ErrUnknownVolume
	}

	if err := h.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCloseFailed, err)
	}

	delete(s.handleMap, volume)

	if i, ok := slices.BinarySearch(s.idSlice, volume); ok {
		s.idSlice = slices.Delete(s.idSlice, i, i+1)
		s.handleSlice = slices.Delete(s.handleSlice, i, i+1)
	}

	s.volumeChooser.Reset(s.idSlice)
	s.volumeMetricsExporter.Export(s.handleSlice)

	return nil
}

func (s *volumeService) GetVolume(volume domain.VolumeID) (domain.VolumeHandle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.handleMap[volume]
	if !ok {
		return nil, ErrUnknownVolume
	}

	return h, nil
}
