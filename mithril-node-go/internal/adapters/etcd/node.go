package adaptersetcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	nodev1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/node/v1"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

type NodeClaimModel struct {
	ID    domain.NodeID `json:"nodeId"`
	Proof []byte        `json:"proof"`
}

type NodeClaimRegistry struct {
	client *clientv3.Client
}

var _ domain.NodeClaimRegistry = (*NodeClaimRegistry)(nil)

func NewNodeClaimRegistry(client *clientv3.Client) *NodeClaimRegistry {
	return &NodeClaimRegistry{
		client: client,
	}
}

func (r *NodeClaimRegistry) Register(ctx context.Context, claim *domain.NodeClaim) error {
	key := fmt.Sprintf("/claims/%010d", uint32(claim.ID))

	bo := backoff.NewExponentialBackOff()
	bo.Reset()

	model := NodeClaimModel{
		ID:    claim.ID,
		Proof: claim.Proof[:],
	}

	value, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNodeClaimEncodingFailed, err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := r.client.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, string(value))).
			Else(clientv3.OpGet(key)).
			Commit()

		if err != nil {
			// Retry network errors with backoff.
			var netErr net.Error
			if errors.As(err, &netErr) {
				d := bo.NextBackOff()

				if d == backoff.Stop {
					return fmt.Errorf("%w: %w", ErrRetryExhausted, err)
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(d):
				}
				continue
			}

			return err
		}

		// Successful claim
		if resp.Succeeded {
			bo.Reset()
			return nil
		}

		// Unexpected response shape
		if len(resp.Responses) != 1 {
			return fmt.Errorf("%w: %w", domain.ErrNodeClaimConflict, ErrUnexpectedResponse)
		}

		getResp := resp.Responses[0].GetResponseRange()
		if getResp == nil || len(getResp.Kvs) == 0 {
			// Race: deleted between compare/get — retry immediately.
			continue
		}

		var otherModel NodeClaimModel

		if err := json.Unmarshal(getResp.Kvs[0].Value, &otherModel); err != nil {
			return fmt.Errorf("%w: %w", ErrNodeClaimDecodingFailed, err)
		}

		if !bytes.Equal(otherModel.Proof[:], model.Proof[:]) {
			return fmt.Errorf("%w", domain.ErrNodeClaimConflict)
		}

		// Idempotent success
		bo.Reset()
		return nil
	}
}

func (r *NodeClaimRegistry) Check(ctx context.Context, claim *domain.NodeClaim) error {
	key := fmt.Sprintf("/claims/%010d", uint32(claim.ID))

	bo := backoff.NewExponentialBackOff()
	bo.Reset()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		getResp, err := r.client.KV.Get(ctx, key)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) {
				d := bo.NextBackOff()
				if d == backoff.Stop {
					return fmt.Errorf("%w: %w", ErrRetryExhausted, err)
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(d):
				}

				continue
			}

			return err
		}

		if len(getResp.Kvs) == 0 {
			return fmt.Errorf("%w", ErrNodeClaimNotFound)
		}

		var otherModel NodeClaimModel

		if err := json.Unmarshal(getResp.Kvs[0].Value, &otherModel); err != nil {
			return fmt.Errorf("%w: %w", ErrNodeClaimDecodingFailed, err)
		}

		if !bytes.Equal(otherModel.Proof[:], claim.Proof[:]) {
			return fmt.Errorf("%w", domain.ErrNodeClaimInvalid)
		}

		return nil
	}
}

type NodeLabelPublisher struct {
	client *clientv3.Client
	logger *zerolog.Logger

	mu    sync.Mutex
	value *EventualValue
}

var _ domain.NodeLabelPublisher = (*NodeLabelPublisher)(nil)

func NewNodeLabelPublisher(client *clientv3.Client, logger *zerolog.Logger) *NodeLabelPublisher {
	return &NodeLabelPublisher{
		client: client,
		logger: logger,
	}
}

func (p *NodeLabelPublisher) Publish(node domain.NodeID, labels map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.value == nil {
		key := fmt.Sprintf("/registry/nodes/%010d/labels", uint32(node))

		p.value = NewEventualValue(p.client, key)
	}

	msg := nodev1.Labels{
		NodeId: uint32(node),
	}
	for key, value := range labels {
		msg.Labels = append(msg.Labels, &nodev1.Labels_Label{
			Key:   key,
			Value: value,
		})
	}

	value, err := protojson.Marshal(&msg)
	if err != nil {
		// TODO: log the error
	}

	p.value.Set(string(value))
}

// /alive/nodes/:nodeID

type NodePresencePublisher struct {
	client *clientv3.Client
	logger *zerolog.Logger

	mu    sync.Mutex
	value *LeasedValue
}

var _ domain.NodePresencePublisher = (*NodePresencePublisher)(nil)

func NewNodePresencePublisher(client *clientv3.Client, logger *zerolog.Logger) *NodePresencePublisher {
	return &NodePresencePublisher{
		client: client,
		logger: logger,
	}
}

func (p *NodePresencePublisher) Publish(presence *domain.NodePresence) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.value == nil {
		key := fmt.Sprintf("/alive/nodes/%010d", uint32(presence.ID))

		p.value = NewLeasedValue(p.client, key, 30)
	}

	msg := nodev1.Presence{
		NodeId: uint32(presence.ID),
		Nonce:  presence.Nonce[:],
		Grpc: &nodev1.Presence_GRPCInfo{
			Urls: presence.GRPC.URLs,
		},
	}

	value, err := protojson.Marshal(&msg)
	if err != nil {
		// TODO: log the error
	}

	p.value.Set(string(value))
}
