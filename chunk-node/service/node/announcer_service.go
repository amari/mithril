package node

import (
	"context"
	"crypto/rand"
	"encoding/binary"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/rs/zerolog"
)

type NodeAnnouncerService struct {
	announcer    port.NodeAnnouncer
	announcement *domain.NodeAnnouncement
	log          *zerolog.Logger
}

func NewNodeAnnouncerService(
	announcer port.NodeAnnouncer,
	log *zerolog.Logger,
	grpcURLs []string,
) *NodeAnnouncerService {
	// Generate a random startup nonce
	var nonce uint64
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback to 0 if we can't generate random
		nonce = 0
	} else {
		nonce = binary.BigEndian.Uint64(b)
	}

	return &NodeAnnouncerService{
		announcer: announcer,
		announcement: &domain.NodeAnnouncement{
			StartupNonce: nonce,
			GRPCURLs:     grpcURLs,
		},
		log: log,
	}
}

func (nas *NodeAnnouncerService) AnnounceNode(ctx context.Context) error {
	nas.log.Info().
		Uint64("startupNonce", nas.announcement.StartupNonce).
		Msg("Announcing node presence")

	return nas.announcer.SetAnnouncement(ctx, nas.announcement)
}

func (nas *NodeAnnouncerService) ClearAnnouncement(ctx context.Context) error {
	nas.log.Info().
		Msg("Clearing node announcement")

	return nas.announcer.ClearAnnouncement(ctx)
}
