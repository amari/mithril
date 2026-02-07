package domain

type NodeID uint32

type NodeSeed []byte

type NodeIdentity struct {
	NodeID NodeID
	Proof  []byte
}

type NodeAnnouncement struct {
	StartupNonce uint64
	GRPCURLs     []string
}
