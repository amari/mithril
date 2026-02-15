package node

type NodeLabelProvider interface {
	GetNodeLabels() map[string]string
}
