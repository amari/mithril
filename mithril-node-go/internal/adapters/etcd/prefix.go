package adaptersetcd

import (
	"fmt"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type Prefix string

func NewPrefix(prefix string) Prefix {
	return Prefix(prefix)
}

func (p Prefix) ClusterPrefix() string {
	return string(p) + "/mithril/v1/cluster"
}

func (p Prefix) DiscoveryPrefix() string {
	return string(p.ClusterPrefix() + "/discovery")
}

func (p Prefix) DiscoveryNodePrefix() string {
	return string(p.DiscoveryPrefix() + "/nodes")
}

func (p Prefix) DiscoveryNodeKey(nodeID domain.NodeID) string {
	return fmt.Sprintf("%s/%10d", p.DiscoveryNodePrefix(), nodeID)
}

func (p Prefix) RegistryPrefix() string {
	return string(p.ClusterPrefix() + "/registry")
}

func (p Prefix) RegistryNodeClaimPrefix() string {
	return string(p.RegistryPrefix() + "/claims/nodes")
}

func (p Prefix) RegistryNodeClaimKey(nodeID domain.NodeID) string {
	return fmt.Sprintf("%s/%10d", p.RegistryNodeClaimPrefix(), nodeID)
}

func (p Prefix) RegistryNodeLabelsPrefix() string {
	return string(p.RegistryPrefix() + "/labels/nodes")
}

func (p Prefix) RegistryNodeLabelsKey(nodeID domain.NodeID) string {
	return fmt.Sprintf("%s/%10d", p.RegistryNodeLabelsPrefix(), nodeID)
}

func (p Prefix) RegistryNodeTaintsPrefix() string {
	return string(p.RegistryPrefix() + "/taints/nodes")
}

func (p Prefix) RegistryNodeTaintsKey(nodeID domain.NodeID) string {
	return fmt.Sprintf("%s/%10d", p.RegistryNodeTaintsPrefix(), nodeID)
}

func (p Prefix) RegistryNodeVolumesPrefix() string {
	return string(p.RegistryPrefix() + "/volumes/nodes")
}

func (p Prefix) RegistryNodeVolumesKey(nodeID domain.NodeID) string {
	return fmt.Sprintf("%s/%10d", p.RegistryNodeVolumesPrefix(), nodeID)
}

func (p Prefix) SchedulerPrefix() string {
	return string(p.ClusterPrefix() + "/scheduler")
}

func (p Prefix) SchedulerCardShuffleElectionPrefix() string {
	return string(p.SchedulerPrefix() + "/card-shuffle/leader")
}

func (p Prefix) SchedulerCardShuffleGenerationKey() string {
	return string(p.SchedulerPrefix() + "/card-shuffle/generation")
}

func (p Prefix) SchedulerCardShuffleDeckPrefix() string {
	return string(p.SchedulerPrefix() + "/card-shuffle/decks")
}

func (p Prefix) SchedulerCardShuffleDeckKey(deckID int) string {
	return fmt.Sprintf("%s/%03d", p.SchedulerCardShuffleDeckPrefix(), deckID)
}
