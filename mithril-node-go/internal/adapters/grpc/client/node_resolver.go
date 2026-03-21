package adaptersgrpcclient

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const schemeMithrilNode = "mithrilnode"

type MithrilNodeResolver struct {
	nodeID           domain.NodeID
	nodePeerResolver domain.NodePeerResolver
	clientConn       resolver.ClientConn
	buildOpts        resolver.BuildOptions

	mu        sync.Mutex
	wg        *sync.WaitGroup
	ctx       context.Context
	cancelCtx context.CancelFunc
}

var _ resolver.Resolver = (*MithrilNodeResolver)(nil)

func (r *MithrilNodeResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	r.wg.Go(func() {
		r.doResolve()
	})
}

func (r *MithrilNodeResolver) applyPresence(presence *domain.NodePresence) {
	r.applyGRPCURLs(presence.GRPC.URLs)
}

func (r *MithrilNodeResolver) applyPeer(peer *domain.NodePeer) {
	r.applyGRPCURLs(peer.GRPC.URLs)
}

func (r *MithrilNodeResolver) applyGRPCURLs(urls []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, url := range urls {
		state, err := r.resolveURL(url)
		if err != nil {
			// This URL failed, try the next one (if any)
			continue
		}

		if len(state.Addresses) > 0 || len(state.Endpoints) > 0 {
			r.clientConn.UpdateState(state)

			return
		}
	}
}

func (r *MithrilNodeResolver) doResolve() {
	peer, err := r.nodePeerResolver.Resolve(r.ctx, r.nodeID)
	if err != nil {
		r.clientConn.ReportError(fmt.Errorf("failed to resolve mithril node with id %10d: %w", r.nodeID, err))
		return
	}

	r.applyPeer(peer)

	// All URLs failed
	r.clientConn.ReportError(fmt.Errorf("all gRPC URLs failed for node %010d", r.nodeID))
}

func (r *MithrilNodeResolver) parseTarget(target string) resolver.Target {
	parsed := resolver.Target{}

	// Check if URL has a scheme
	if idx := strings.Index(target, "://"); idx > 0 {
		parsed.URL.Scheme = target[:idx]
		target = target[idx+3:]
	} else {
		// No scheme means passthrough (direct address)
		parsed.URL.Scheme = "passthrough"
	}

	// Remove leading slashes
	target = strings.TrimPrefix(target, "/")

	parsed.URL.Path = "/" + target

	return parsed
}

func (r *MithrilNodeResolver) resolveURL(url string) (resolver.State, error) {
	// We're going to delegate to other resolvers.
	target := r.parseTarget(url)

	// Prevent infinite recursion
	if target.URL.Scheme == schemeMithrilNode {
		// FIXME: make the error message less cryptic and more user-friendly
		return resolver.State{}, fmt.Errorf("cannot defer to %q resolver", schemeMithrilNode)
	}

	// Get the builder for this scheme
	builder := resolver.Get(target.URL.Scheme)
	if builder == nil {
		// No resolver for this scheme, treat as passthrough (direct address)
		return resolver.State{
			Endpoints: []resolver.Endpoint{
				{
					Addresses: []resolver.Address{
						{
							Addr: target.Endpoint(),
						},
					},
				},
			},
		}, nil
	}

	// Create a temporary ClientConn to capture the resolution result
	captureCC := &clientConn{}

	// Build the resolver
	res, err := builder.Build(target, captureCC, resolver.BuildOptions{})
	if err != nil {
		return resolver.State{}, err
	}
	defer res.Close()

	// Trigger resolution
	res.ResolveNow(resolver.ResolveNowOptions{})

	// Return captured addresses
	if captureCC.State.Addresses != nil {
		return captureCC.State, nil
	}

	// Return captured error
	if captureCC.Err != nil {
		return resolver.State{}, captureCC.Err
	}

	// No addresses resolved, return an error
	return resolver.State{}, fmt.Errorf("no addresses resolved for %s", url)
}

func (r *MithrilNodeResolver) Close() {
	r.mu.Lock()
	r.cancelCtx()
	r.mu.Unlock()

	r.wg.Wait()
}

type MithrilNodeResolverBuilder struct {
	nodePeerResolver domain.NodePeerResolver
	wg               *sync.WaitGroup
	ctx              context.Context
	cancelCtx        context.CancelFunc
}

var _ resolver.Builder = (*MithrilNodeResolverBuilder)(nil)

func NewMithrilNodeResolverBuilder(nodePeerResolver domain.NodePeerResolver) *MithrilNodeResolverBuilder {
	return &MithrilNodeResolverBuilder{nodePeerResolver: nodePeerResolver, wg: &sync.WaitGroup{}}
}

func (b *MithrilNodeResolverBuilder) Start() {
	b.ctx, b.cancelCtx = context.WithCancel(context.Background())
}

func (b *MithrilNodeResolverBuilder) Stop() {
	b.cancelCtx()

	b.wg.Wait()
}

func (b *MithrilNodeResolverBuilder) Scheme() string { return schemeMithrilNode }

func (b *MithrilNodeResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	spew.Dump(target)
	// Parse NodeID from target endpoint
	var nodeID domain.NodeID
	if _, err := fmt.Sscanf(target.URL.Host, "%010d", &nodeID); err != nil {
		return nil, fmt.Errorf("invalid chunk-node target, expected node ID in decimal format: %w", err)
	}

	resolverCtx, resolverCancel := context.WithCancel(b.ctx)

	r := &MithrilNodeResolver{nodeID: nodeID, nodePeerResolver: b.nodePeerResolver, clientConn: cc, buildOpts: opts, wg: b.wg, ctx: resolverCtx, cancelCtx: resolverCancel}

	w, err := b.nodePeerResolver.Watch(b.ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to watch node: %w", err)
	}

	go func() {
		for {
			select {
			case <-resolverCtx.Done():
				return
			case _, ok := <-w:
				if !ok {
					return
				}

				r.wg.Go(func() {
					r.doResolve()
				})
			}
		}
	}()

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

type clientConn struct {
	State resolver.State
	Err   error
}

var _ resolver.ClientConn = (*clientConn)(nil)

func (c *clientConn) UpdateState(state resolver.State) error {
	c.State = state
	return nil
}

func (c *clientConn) ReportError(err error) {
	c.Err = err
}

func (c *clientConn) NewAddress(addresses []resolver.Address) {
	c.State.Addresses = addresses
}

func (c *clientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	// For our use case, we don't need to parse service config from delegated resolvers
	return &serviceconfig.ParseResult{}
}
