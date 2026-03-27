# Etcd Cluster Design

## Overview

Mithril utilizes etcd as the canonical data store for cluster coordination, scheduling, and distributed state management.

**Operator-First Design**: A core design constraint of the etcd keyspace is human readability to support manual emergency debugging and remediation. Instead of opaque binary formats, **all etcd values are encoded as JSON**, and all keys are strictly ASCII strings. This guarantees that operators can use standard tools like `etcdctl` to reliably inspect, patch, or repair cluster state during outages without requiring custom decoding tools.

The keyspace uses a unified hierarchical layout under `:prefix/mithril/v1/cluster/` to group operational roles:

- **Discovery (`/discovery/`)**: Tracks the set of currently available nodes actively participating in the cluster via lease-bound keys. Centralized routing is intentionally avoided in favor of direct RPC.
- **Registry (`/registry/`)**: Stores static identity claims and self-published node metadata, such as labels, taints, and attached volume bitsets.
- **Metadata (`/metadata/`)**: Tracks operator-applied intrinsic overrides (labels, taints, cordons) using a completely flattened, disparate keyspace for safe manual patching via `etcdctl`.
- **Scheduler (`/scheduler/`)**: Coordinates the operation of the Card Shuffle Scheduler, managing leader election, generation transitions, and deck distributions.

## Etcd Key Design

### Discovery

#### `:prefix/mithril/v1/cluster/discovery/nodes/:node_id`

- Lease-bound
- Indicates a node is active and available for direct RPC connections

```proto
package mithril.cluster.discovery.v1;

message NodeRecord {
  uint32 node_id = 1;
  bytes nonce = 2;

  Endpoints endpoints = 3;

  message Endpoints {
    // Active gRPC targets for fast-path cluster operations and client routing.
    repeated string grpc = 1;

    // Future expansion examples:
    // repeated string http = 2;
    // repeated string metrics = 3;
  }
}
```

### Registry (Node Metadata)

#### `:prefix/mithril/v1/cluster/registry/claims/nodes/:node_id`

- Deterministic node ID allocator claim

```proto
package mithril.cluster.registry.node.v1;

message ClaimRecord {
  uint32 node_id = 1;

  bytes proof = 2;
}
```

#### `:prefix/mithril/v1/cluster/registry/labels/nodes/:node_id`

- Self-published node labels

```proto
package mithril.cluster.registry.node.v1;

import "mithril/cluster/registry/node/v1/label.proto";

message LabelRecord {
  uint32 node_id = 1;
  repeated Label labels = 2;
}
```

#### `:prefix/mithril/v1/cluster/registry/taints/nodes/:node_id`

- Self-published node taints for scheduling control

```proto
package mithril.cluster.registry.node.v1;

message TaintRecord {
  uint32 node_id = 1;
  repeated Taint taints = 2;

  message Taint {
    string key = 1;
    string value = 2;
    Effect effect = 3;

    enum Effect {
      EFFECT_UNSPECIFIED = 0;
      EFFECT_NO_SCHEDULE = 1;
      EFFECT_PREFER_NO_SCHEDULE = 2;
      EFFECT_NO_EXECUTE = 3;
    }
  }
}
```

#### `:prefix/mithril/v1/cluster/registry/volumes/nodes/:node_id`

- Labeled volume bitsets for fast set operations on volumes by label

```proto
package mithril.cluster.registry.node.v1;

import "mithril/cluster/registry/node/v1/label.proto";

message VolumeRecord {
  uint32 node_id = 1;
  repeated LabeledVolumeSet labeled_volume_sets = 2;

  message LabeledVolumeSet {
    Label label = 1;

    // Volume ID bitset as little-endian uint64 slice.
    bytes volume_set = 2;
  }
}
```

### Scheduler (Card Shuffle)

#### `:prefix/mithril/v1/cluster/scheduler/card-shuffle/leader/`

- Key prefix for etcd leader election (e.g., using `concurrency.Election`)
- No value stored; the election mechanism uses subkeys

#### `:prefix/mithril/v1/cluster/scheduler/card-shuffle/generation`

- Leader bumps generation after regenerating all decks
- Clients watch/poll this key to invalidate their cached decks

```proto
package mithril.cluster.scheduler.cardshuffle.v1;

message GenerationRecord {
  uint64 generation = 1;
}
```

#### `:prefix/mithril/v1/cluster/scheduler/card-shuffle/decks/:deck_id`

- `:deck_id` is sequential: 0, 1, 2, ..., min(N!, 100) - 1
- In practice: 100 decks when N >= 5 nodes, N! decks when N <= 4 nodes
- Leader regenerates decks on a timer, not on node join/leave (too flappy)
- Clients pick a random deck, pick a random starting index, iterate locally

```proto
package mithril.cluster.scheduler.cardshuffle.v1;

message DeckRecord {
  repeated uint32 node_ids = 1;
}
```

### Metadata (Operator Overrides)

#### `:prefix/mithril/v1/cluster/metadata/cordon/nodes/:node_id`

- Operator-applied cordon to gracefully drain or halt scheduling

```proto
package mithril.cluster.metadata.node.v1;

message CordonRecord {
  uint32 node_id = 1;
}
```

#### `:prefix/mithril/v1/cluster/metadata/labels/:label_key/nodes/:node_id`

- Operator-applied label overrides
- Flattened for independent, per-key `etcdctl` writes

```proto
package mithril.cluster.metadata.node.v1;

message LabelRecord {
  uint32 node_id = 1;
  string key = 2;
  string value = 3;
}
```

#### `:prefix/mithril/v1/cluster/metadata/taints/:taint_key/:taint_effect/nodes/:node_id`

- Operator-applied taint overrides
- Flattened for independent, per-taint `etcdctl` writes

```proto
package mithril.cluster.metadata.node.v1;

message TaintRecord {
  uint32 node_id = 1;
  string key = 2;
  string value = 3;
  Effect effect = 4;

  enum Effect {
    EFFECT_UNSPECIFIED = 0;
    EFFECT_NO_SCHEDULE = 1;
    EFFECT_PREFER_NO_SCHEDULE = 2;
    EFFECT_NO_EXECUTE = 3;
  }
}
```
