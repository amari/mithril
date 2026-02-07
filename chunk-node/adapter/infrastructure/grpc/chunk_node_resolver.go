package grpc

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const chunkNodeScheme = "chunk-node"

// chunkNodeResolverBuilder builds resolvers for the chunk-node:/// URI scheme.
//
// The chunk-node resolver enables gRPC clients to connect to chunk nodes by their
// NodeID rather than by explicit addresses. This provides several benefits:
//
// 1. Location transparency: Clients specify which node they need (by ID), not where it is
// 2. Automatic failover: Node announcements include priority-ordered connection URLs
// 3. Restart detection: When a node restarts (StartupNonce changes), connections are refreshed
// 4. Flexible addressing: Nodes can announce multiple URLs (pod IP, service DNS, external ingress)
//
// Target format: chunk-node:///{nodeID}
// Example: chunk-node:///00000001 (connects to node with ID 0x00000001)
//
// Resolution flow:
// - Looks up node in cluster via MemberResolver (reads from etcd)
// - Tries announced GRPCURLs in priority order (e.g., pod IP, then service DNS)
// - Each URL may itself resolve to multiple addresses (e.g., DNS load balancing)
// - Watches for node changes via MemberWatchManager (etcd watch)
// - Re-resolves automatically when node restarts (StartupNonce change detected)
//
// Race condition handling:
// Resolution races (stale connection info) are handled by ChunkID validation.
// If we connect to the wrong node due to a resolution race, RPCs fail because
// the ChunkID's embedded NodeID won't match the remote node's actual ID.
type chunkNodeResolverBuilder struct {
	memberResolver portnode.MemberResolver
	watchManager   portnode.MemberWatchManager
}

// NewChunkNodeResolverBuilder creates and registers a chunk-node resolver builder.
// Must be called during application initialization to enable chunk-node:/// URLs.
func NewChunkNodeResolverBuilder(
	memberResolver portnode.MemberResolver,
	watchManager portnode.MemberWatchManager,
) *chunkNodeResolverBuilder {
	builder := &chunkNodeResolverBuilder{
		memberResolver: memberResolver,
		watchManager:   watchManager,
	}

	resolver.Register(builder)

	return builder
}

func (b *chunkNodeResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	// Parse NodeID from target endpoint (hex format: 00000001)
	var nodeID domain.NodeID
	if _, err := fmt.Sscanf(target.Endpoint(), "%08x", &nodeID); err != nil {
		return nil, fmt.Errorf("invalid chunk-node target, expected node ID in hex format: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &chunkNodeResolver{
		nodeID:         nodeID,
		memberResolver: b.memberResolver,
		watchManager:   b.watchManager,
		cc:             cc,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Start watching for cluster events
	go r.watchMember()

	// Do initial resolution
	go r.resolve()

	return r, nil
}

func (b *chunkNodeResolverBuilder) Scheme() string {
	return chunkNodeScheme
}

// chunkNodeResolver resolves a specific NodeID to connection endpoints.
//
// Behavior:
// - Caches the most recent Member info (NodeID + StartupNonce + GRPCURLs)
// - Watches for cluster events via MemberWatchManager
// - Re-resolves when StartupNonce changes (indicates node restart)
// - Reports error if node leaves the cluster
// - Tries GRPCURLs in priority order, delegating to their respective resolvers
//
// Lifecycle:
// - Created per grpc.ClientConn (one resolver per connection)
// - Automatically cleaned up when connection is closed (via Close())
// - Underlying watch may be shared with other resolvers for the same NodeID
type chunkNodeResolver struct {
	nodeID         domain.NodeID
	memberResolver portnode.MemberResolver
	watchManager   portnode.MemberWatchManager
	cc             resolver.ClientConn
	ctx            context.Context
	cancel         context.CancelFunc

	mu           sync.RWMutex
	cachedMember *portnode.Member
}

// ResolveNow triggers an immediate re-resolution of the node's connection info.
func (r *chunkNodeResolver) ResolveNow(resolver.ResolveNowOptions) {
	go r.resolve()
}

// Close stops watching for cluster events and cleans up resources.
func (r *chunkNodeResolver) Close() {
	r.cancel()
}

func (r *chunkNodeResolver) resolve() {
	// Resolve member info
	member, err := r.memberResolver.ResolveMember(r.ctx, r.nodeID)
	if err != nil {
		r.cc.ReportError(fmt.Errorf("failed to resolve node %08x: %w", r.nodeID, err))
		return
	}

	// Update cache
	r.mu.Lock()
	r.cachedMember = member
	r.mu.Unlock()

	// Try URLs in priority order
	for _, url := range member.GRPCURLs {
		addresses, err := r.resolveURL(url)
		if err != nil {
			// This URL failed, try next
			continue
		}

		if len(addresses) > 0 {
			// Success! Update connection with these addresses
			r.cc.UpdateState(resolver.State{
				Addresses: addresses,
			})
			return
		}
	}

	// All URLs failed
	r.cc.ReportError(fmt.Errorf("all %d gRPC URLs failed for node %08x", len(member.GRPCURLs), r.nodeID))
}

// resolveURL delegates resolution to the appropriate resolver based on the URL's scheme.
// For example:
// - "10.0.1.5:9000" → passthrough resolver (direct address)
// - "dns:///chunk-nodes.default.svc.cluster.local:9000" → DNS resolver (A/AAAA records)
// - Custom schemes can be supported by registering additional resolvers
func (r *chunkNodeResolver) resolveURL(url string) ([]resolver.Address, error) {
	target := parseTarget(url)

	// Get the builder for this scheme
	builder := resolver.Get(target.URL.Scheme)
	if builder == nil {
		// No resolver for this scheme, treat as passthrough (direct address)
		return []resolver.Address{{Addr: target.Endpoint()}}, nil
	}

	// Create a temporary ClientConn to capture the resolution result
	captureCC := &captureClientConn{}

	// Build the resolver
	res, err := builder.Build(target, captureCC, resolver.BuildOptions{})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	// Trigger resolution
	res.ResolveNow(resolver.ResolveNowOptions{})

	// Return captured addresses
	if captureCC.state.Addresses != nil {
		return captureCC.state.Addresses, nil
	}

	if captureCC.err != nil {
		return nil, captureCC.err
	}

	return nil, fmt.Errorf("no addresses resolved for %s", url)
}

// watchMember observes cluster events and triggers re-resolution when needed.
func (r *chunkNodeResolver) watchMember() {
	events, err := r.watchManager.Watch(r.ctx, r.nodeID)
	if err != nil {
		r.cc.ReportError(fmt.Errorf("failed to watch node %08x: %w", r.nodeID, err))
		return
	}

	for event := range events {
		switch event.Type {
		case portnode.ClusterMemberEventTypeUpdated:
			// Node restarted or announcement changed - check StartupNonce
			r.mu.RLock()
			cached := r.cachedMember
			r.mu.RUnlock()

			if cached != nil && event.Node != nil && cached.StartupNonce != event.Node.StartupNonce {
				// StartupNonce changed - node restarted, trigger re-resolution
				r.ResolveNow(resolver.ResolveNowOptions{})
			}

		case portnode.ClusterMemberEventTypeLeft:
			// Node left cluster - report error
			r.cc.ReportError(fmt.Errorf("node %08x left the cluster", r.nodeID))
		}
	}
}

// parseTarget parses a URL string into a resolver.Target
func parseTarget(url string) resolver.Target {
	parsed := resolver.Target{}

	// Check if URL has a scheme
	if idx := strings.Index(url, "://"); idx > 0 {
		parsed.URL.Scheme = url[:idx]
		url = url[idx+3:]
	} else {
		// No scheme means passthrough (direct address)
		parsed.URL.Scheme = "passthrough"
	}

	// Remove leading slashes
	url = strings.TrimPrefix(url, "/")

	parsed.URL.Path = "/" + url

	return parsed
}

// captureClientConn captures the resolution result from delegated resolvers
type captureClientConn struct {
	state resolver.State
	err   error
}

func (c *captureClientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *captureClientConn) ReportError(err error) {
	c.err = err
}

func (c *captureClientConn) NewAddress(addresses []resolver.Address) {
	c.state.Addresses = addresses
}

func (c *captureClientConn) NewServiceConfig(serviceConfig string) {
	// Deprecated - using ParseServiceConfig instead
}

func (c *captureClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	// For our use case, we don't need to parse service config from delegated resolvers
	return &serviceconfig.ParseResult{}
}
