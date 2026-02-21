# Etcd Cluster Design

## Overview

TODO

## Etcd Key Design

### Node Identity

#### `/claims/:nodeID`

```proto
package mithril.cluster.node.v1;

message Claim {
  uint32 node_id = 1;
  bytes proof = 2;
}
```

### Node Liveness

#### `/alive/nodes/:nodeID`

- Lease-bound

```proto
package mithril.cluster.node.v1;

message Presence {
  uint32 node_id = 1;
  bytes nonce = 2;
  GRPCInfo grpc = 3;

  message GRPCInfo {
    repeated string urls = 1;
  }
}
```

### Node Registry

#### `/registry/nodes/:nodeID/labels`

```proto
package mithril.cluster.node.v1;

message Labels {
  uint32 node_id = 1;

  repeated Label labels = 2;

  message Label {
    string key = 1;
    string value = 2;
  }
}
```

#### `/registry/nodes/:nodeID/labeled_volume_bitsets`

```proto
package mithril.cluster.node.v1;

// LabeledVolumeBitsets holds bitsets for set operations on volumes by label.
message LabeledVolumeBitsets {
  // Denormalized from the etcd key for convenience.
  uint32 node_id = 1;

  repeated LabeledVolumeBitset bitsets = 2;

  message LabeledVolumeBitset {
    string key = 1;
    string value = 2;

    // Volume ID bitset as little-endian uint64 slice.
    bytes volume_ids = 3;
  }
}
```

#### `/registry/nodes/:nodeID/taints`

```proto
package mithril.cluster.node.v1;

message Taints {
  // Denormalized from the etcd key for convenience.
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

### Card Shuffle Scheduler

#### `/schedulers/card-shuffle/leader/`

- Key prefix for etcd leader election (e.g., using `concurrency.Election`)
- No value stored; the election mechanism uses subkeys

#### `/schedulers/card-shuffle/generation`

- Leader bumps generation after regenerating all decks
- Clients watch/poll this key to invalidate their cached decks

```proto
package mithril.cluster.schedulers.card_shuffle.v1;

message Generation {
  uint64 generation = 1;
}
```

#### `/schedulers/card-shuffle/decks/:deckID`

- `:deckID` is sequential: 0, 1, 2, ..., min(N!, 100) - 1
- In practice: 100 decks when N >= 5 nodes, N! decks when N <= 4 nodes
- Leader regenerates decks on a timer, not on node join/leave (too flappy)
- Clients pick a random deck, pick a random starting index, iterate locally

```proto
package mithril.cluster.schedulers.card_shuffle.v1;

message Deck {
  repeated uint32 node_ids = 1;
}
```
