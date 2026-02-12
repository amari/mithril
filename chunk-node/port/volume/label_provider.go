package volume

type VolumeLabelProvider interface {
	GetVolumeLabels(id ID) map[string]string
}

type VolumeLabelIndexProvider interface {
	GetVolumesWithLabel(label string) []ID
}

type VolumeBitsetLabelIndexProvider interface {
	GetVolumeBitsetWithLabel(label string) VolumeBitset
}

type VolumeBitsetLabelIndexesProvider interface {
	GetAllBitsetLabelIndexes() map[string]VolumeBitset
}
