package node

import "github.com/amari/mithril/chunk-node/domain"

type NodeIDProvider interface {
	GetNodeID() (domain.NodeID, error)
}
