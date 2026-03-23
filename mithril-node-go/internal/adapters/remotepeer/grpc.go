package adaptersremotepeer

import (
	"context"
	"fmt"
	"io"
	"sync"

	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	applicationerrors "github.com/amari/mithril/mithril-node-go/internal/application/errors"
	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type GRPCRemotePeerChunkServiceClientProvider struct {
	peerResolver domain.NodePeerResolver

	mu        sync.Mutex
	connCache map[domain.NodeID]*grpcRemotePeerChunkServiceClientProviderCacheEntry
}

var _ applicationservices.RemotePeerChunkServiceClientProvider = (*GRPCRemotePeerChunkServiceClientProvider)(nil)

func NewGRPCRemotePeerChunkServiceClientProvider(peerResolver domain.NodePeerResolver) *GRPCRemotePeerChunkServiceClientProvider {
	return &GRPCRemotePeerChunkServiceClientProvider{
		peerResolver: peerResolver,
		connCache:    make(map[domain.NodeID]*grpcRemotePeerChunkServiceClientProviderCacheEntry),
	}
}

func (p *GRPCRemotePeerChunkServiceClientProvider) GetRemotePeerChunkServiceClient(nodeID domain.NodeID) (applicationservices.RemotePeerChunkServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.connCache[nodeID]; ok {
		return conn.Client, nil
	}

	// FIXME: add TLS support!
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	cc, err := grpc.NewClient(fmt.Sprintf("mithrilnode://%010d", nodeID), dialOpts...)
	if err != nil {
		// FIXME
		return nil, err
	}

	client := chunkv1.NewChunkServiceClient(cc)

	conn := &grpcRemotePeerChunkServiceClientProviderCacheEntry{
		Client: &GRPCRemotePeerChunkServiceClient{
			client: client,
		},
	}

	p.connCache[nodeID] = conn

	return conn.Client, nil
}

type grpcRemotePeerChunkServiceClientProviderCacheEntry struct {
	Client *GRPCRemotePeerChunkServiceClient
}

type GRPCRemotePeerChunkServiceClient struct {
	client chunkv1.ChunkServiceClient
}

var _ applicationservices.RemotePeerChunkServiceClient = (*GRPCRemotePeerChunkServiceClient)(nil)

func (c *GRPCRemotePeerChunkServiceClient) Stat(ctx context.Context, chunkID domain.ChunkID) (*applicationservices.RemotePeerChunkServiceStatResponse, error) {
	resp, err := c.client.StatChunk(ctx, &chunkv1.StatChunkRequest{ChunkId: chunkID.Bytes()})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			// unreachable
			return nil, err
		}

		return nil, StatusToError(st)
	}

	volumeStatus := VolumeStatusFromProto(resp.Volume)

	chunk, err := ChunkFromProto(resp.Chunk)
	if err != nil {
		return nil, applicationerrors.ContextErrorWithRemotePeerVolumeStatus(err, volumeStatus)
	}

	return &applicationservices.RemotePeerChunkServiceStatResponse{
		Chunk:        chunk,
		VolumeStatus: volumeStatus,
	}, nil
}

func (c *GRPCRemotePeerChunkServiceClient) OpenRangeReader(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (applicationservices.RemotePeerChunkRangeReader, error) {
	resp, err := c.client.ReadChunk(ctx, &chunkv1.ReadChunkRequest{
		ChunkId: chunkID.Bytes(),
		Offset:  offset,
		Length:  length,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			// unreachable
			return nil, err
		}

		return nil, StatusToError(st)
	}

	// receive the first message, which should always be the header
	msg, err := resp.Recv()
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			// unreachable
			return nil, err
		}

		return nil, StatusToError(st)
	}

	header := msg.GetHeader()
	if header == nil {
		return nil, fmt.Errorf("expected header message: %w", domain.ErrInternal)
	}

	headerVolumeStatus := VolumeStatusFromProto(header.Volume)

	headerChunk, err := ChunkFromProto(header.Chunk)
	if err != nil {
		return nil, fmt.Errorf("failed to parse header: %w", applicationerrors.NewContextError(err).WithRemotePeerVolumeStatus(headerVolumeStatus))
	}

	// return the reader
	r := &GRPCRemotePeerChunkRangeReader{
		chunk:         headerChunk,
		volumeStatus:  headerVolumeStatus,
		bytesExpected: int(header.TotalDataLength),
	}

	r.recv = func() ([]byte, error) {
		msg, err := resp.Recv()
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				return nil, err
			}

			return nil, StatusToError(st)
		}

		dataMsg := msg.GetData()
		if dataMsg == nil {
			return nil, fmt.Errorf("expected data message: %w", domain.ErrInternal)
		}

		r.volumeStatus = VolumeStatusFromProto(dataMsg.GetVolume())

		return dataMsg.Data, nil
	}

	return r, nil
}

type GRPCRemotePeerChunkRangeReader struct {
	chunk        *applicationservices.RemotePeerChunk
	volumeStatus *domain.VolumeStatus

	recv          func() ([]byte, error)
	bytesExpected int
	bytesRead     int
	buffer        []byte
}

var _ applicationservices.RemotePeerChunkRangeReader = (*GRPCRemotePeerChunkRangeReader)(nil)

func (r *GRPCRemotePeerChunkRangeReader) Chunk() *applicationservices.RemotePeerChunk {
	return r.chunk
}

func (r *GRPCRemotePeerChunkRangeReader) VolumeStatus() *domain.VolumeStatus {
	return r.volumeStatus
}

func (r *GRPCRemotePeerChunkRangeReader) Read(p []byte) (n int, err error) {
	if r.bytesRead >= r.bytesExpected {
		return 0, io.EOF
	}

	expected := r.bytesExpected - r.bytesRead
	if len(p) > expected {
		p = p[:expected]
	}

	buf := r.buffer
	read := r.bytesRead

	if len(r.buffer) > 0 {
		n = copy(p, buf)
		buf = buf[n:]
		p = p[n:]

		read += n

		if len(p) == 0 {
			r.buffer = buf
			r.bytesRead = read

			return n, nil
		}
	}

	data, err := r.recv()
	if err != nil {
		if n > 0 {
			r.buffer = buf
			r.bytesRead = read

			return n, nil
		}

		return 0, err
	}

	if len(data) == 0 {
		r.buffer = buf
		r.bytesRead = read

		return n, nil
	}

	m := copy(p, data)
	data = data[m:]
	p = p[m:]

	n += m
	read += m

	if len(data) > 0 {
		buf = data
	} else {
		buf = nil
	}

	r.buffer = buf
	r.bytesRead = read

	if r.bytesRead >= r.bytesExpected {
		return n, io.EOF
	}

	return n, nil
}

func (r *GRPCRemotePeerChunkRangeReader) Close() error {
	// no-op
	return nil
}

func ChunkFromProto(chunk *chunkv1.Chunk) (*applicationservices.RemotePeerChunk, error) {
	if chunk == nil {
		return nil, nil
	}

	var id domain.ChunkID
	if err := id.UnmarshalBinary(chunk.Id); err != nil {
		return nil, err
	}

	return applicationservices.NewRemotePeerChunk(
		id,
		chunk.GetSize(),
		chunk.GetVersion(),
	), nil
}

func VolumeStatusFromProto(vol *chunkv1.Volume) *domain.VolumeStatus {
	if vol == nil {
		return nil
	}

	health := domain.VolumeUnknown
	switch vol.State {
	case chunkv1.Volume_STATE_OK:
		health = domain.VolumeOK
	case chunkv1.Volume_STATE_DEGRADED:
		health = domain.VolumeDegraded
	case chunkv1.Volume_STATE_FAILED:
		health = domain.VolumeFailed
	case chunkv1.Volume_STATE_UNSPECIFIED:
		health = domain.VolumeUnknown
	}

	return &domain.VolumeStatus{Health: health}
}
