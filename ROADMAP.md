# Mithril Roadmap & Architecture Vision

**Author:** Amari
**Status:** Draft / Active Roadmap
**Last Updated:** March 2026

## Objective
Mithril is a pragmatic, software-defined storage platform inspired by Facebook's Tectonic, Ceph, and MinIO. The project aims to provide a modern, highly portable FOSS (Free Open-Source Software) alternative that addresses common engineering bottlenecks natively without sacrificing performance.

## Background & Motivation
While enterprise-only systems like Google Colossus and Facebook's Tectonic have proven the multi-modal architecture at scale, Ceph remains practically the only viable FOSS product in the space. Mithril is built to provide a modern, highly portable FOSS alternative that addresses common engineering bottlenecks natively without sacrificing performance.

- **Cross-Platform Portability:** While Ceph is deeply tied to Linux kernel internals, a primary goal of Mithril is to operate seamlessly across Linux, Windows, FreeBSD, and macOS.
- **Host VFS Bypass:** The project aims to offer native SDKs so data-intensive applications (like AI/ML training loops in Python) can stream data directly from chunk nodes entirely in userspace, bypassing kernel and Virtual File System (VFS) bottlenecks.
- **Direct Application Embedding:** Rather than forcing operators to rely on remote filesystem mounts or intermediate gateways, the goal is to provide native libraries (for Go, Node.js, etc.) that allow developers to embed the storage client directly into their software (e.g., functioning as a backend for decentralized projects like IPFS).

## Goals
- **Strict Separation:** Deeply decouple raw bulk data chunk storage from cluster metadata.
- **Shared Abstraction Layer:** Translate diverse access patterns (Block, POSIX File, Object, File Sync) into a common chunk and KV metadata backbone.
- **Pluggable Security:** Empower users to "Bring Your Own IdP" (Identity Provider) and authorization engine to seamlessly secure Mithril across all storage protocols.
- **Asynchronous Maintenance:** Decouple repair and garbage collection entirely from hot I/O paths using dynamically scaled background workers.

## Non-Goals
- **Single Protocol Hegemony:** The system will not force all workloads into a single protocol.
- **Strict Linearizable Consistency:** The architecture explicitly rejects extreme global locking in the metadata index, preferring eventual MVCC consistency over strict linearizable or serializable consistency.
- **Native Erasure-Coded WALs:** Packed Blocks (Write-Ahead Logs) are explicitly replicated, never erasure-coded, to preserve sub-stripe commit speeds.

## Detailed Architecture Design

### 1. Data & Metadata Layer Separation
At its core, the architecture aims to strictly separate raw bulk data from cluster metadata.
- **Chunk Storage Nodes:** The goal is to provide a low-level append-only blob store exclusively for bulk data. *(Currently implemented in Go; planned to be rewritten in Rust to support `DIRECT_IO` and advanced backends).*
- **Metadata K/V Databases:** The architecture intends to map cluster-wide metadata (block maps, layer projections, quotas) onto external K/V databases (FoundationDB, TiKV, ScyllaDB, Postgres). To prevent locking bottlenecks, the design demands eventual consistency via MVCC (Multi-Version Concurrency Control); metadata keys embed an epoch/timestamp suffix where the most recent timestamp wins, bypassing strict linearizable or serializable transactions.

### 2. Logical Block Abstractions
- **Striped Blocks:** Aimed at high-throughput streaming by buffering writes into deterministic stripes for efficient Erasure Coding or Replication.
- **Packed Blocks:** Designed to target low-latency, small random I/O (e.g., 4 KiB file updates). The goal is to act as a strictly replicated Write-Ahead Log (WAL) capable of committing sub-stripe I/O instantly, with background workers later compacting them into Striped Blocks.

### 3. High-Performance Mechanics
Mithril is built with mechanical sympathy in mind, aiming to efficiently utilize modern hardware capabilities.
- **Rust Core & FFI Bindings:** A core goal is to build a single authoritative Rust core to guarantee algorithmic consistency (erasure coding, node selection, retries). The roadmap targets robust `io_uring` support via runtime adapters (`compio`) and ergonomic FFI bindings to serve as Tiered SDKs for Go, Python, Node.js, Swift, and JVM.
- **Custom Binary Protocol (CSTP) (Draft):** An early draft of a custom binary protocol aiming to overcome gRPC/Protobuf serialization overhead. It proposes 128-byte aligned frames to theoretically enable true zero-copy networking.
- **Userspace NIC Direct Access:** Long-term plans include a privileged lifecycle-managing daemon intended to securely assign raw NIC hardware parcels directly to trusted protocols/servers, targeting DPDK-level latency.
- **Decentralized Placement & Topology:** The "Card-Shuffle" design aims to provide decentralized, pre-generated subsets of node IDs. Clients fetch these subsets by reading and watching the cluster prefix in etcd. The Rust SDK will then apply Kubernetes-inspired abstractions (using node labels) to filter that subset to securely choose a target Node—enforcing affinity, anti-affinity, and failure domains without a central coordinator. Once the RPC reaches the chosen Node, the node utilizes homomorphic volume label indexes to determine which internal volume the chunk is placed onto.

### 4. Security (Enterprise IAM & Zero-Trust)
Because the same underlying data layers should be securely accessible via multiple protocols, a core objective is to centralize security to act as an Identity-Aware Storage Gateway.
- **Authentication (Authn) Review (Target):** Modeled after Kubernetes `TokenReview`, the goal is to decouple protocol cryptography from credential storage using pluggable gRPC review services. Frontends would handle native protocol handshakes, while the backend validates credentials to return a normalized `UserInfo` identity.
- **Authorization (Authz) Review (Target):** Modeled after Kubernetes `SubjectAccessReview`. The design aims for a shared, protocol-agnostic backend that consumes the normalized identity to execute cross-protocol access decisions using pluggable engines like SpiceDB, OpenFGA, OPA, or Casbin.
- **Identity Mapping:** The architecture intends to use CEL (Common Expression Language) programs to dynamically map external identity attributes into deterministic, Google Cloud-style Principal Identifiers.
- **Protocol-Specific ACL Translation:** Enterprise filesystems rely heavily on rich Access Control Lists. The design aims to explicitly translate the normalized `UserInfo` identity into native file-operation identities, providing POSIX numeric `uid/gid` maps for NFSv3, fully-qualified `<NAME>@<DOMAIN>` principals for NFSv4, and Windows Security Identifiers (SIDs) for SMB DACLs/SACLs, alongside operator-configured squash mapping.

### 5. Multi-Modal Storage Protocols
Storage access generally maps to distinct protocol families: Block, POSIX File, File Sync, and Object. Rather than maintaining siloed backend systems for each format, Mithril aims to leverage a shared abstraction layer. By translating these diverse access patterns into a common chunk and KV metadata backbone, the goal is to make multi-protocol support highly pragmatic to implement and maintain.
- **Block Storage Target:** NBD, NVMe-oF/TCP, iSCSI, Native QEMU block driver.
- **POSIX-like Filesystems**: NFS, SMB, AFP, FUSE, SFTP, WebDAV, Rsync.
- **File Sync:** Syncthing or Dropbox-style sync.
- **Object & Cloud Storage:** AWS S3, OpenStack Swift, Azure Blob Storage, Google Cloud Storage, COSI.

### 6. Distributed Background Workers
The system is designed to operate asynchronously. A major operational goal is to deeply decouple repair and garbage collection from the hot I/O paths, handling them entirely via distributed background workers. To manage "compaction debt" from Packed Blocks without throttling frontend client I/O, the goal is to leverage external auto-scaling engines (like KEDA) to dynamically scale out the worker pools based on backlog depth.
- `mithril-worker-chunk-gc`
- `mithril-worker-block-gc`
- `mithril-worker-block-repair` *(responsible for background parity reconstruction during quorum failures)*
- `mithril-worker-block-scrub`
- `mithril-worker-object-lifecycle`

## Implementation Milestones (Roadmap)
1. **Current Status**: Foundational Chunk Storage built (`mithril-node-go`).
2. **Phase 1**: Initial Metadata K/V Integration and Striped/Packed Block abstractions.
3. **Phase 2**: Delivery of the core Rust SDK (`mithril-sdk-rs`) handling placement and IO tracking.
4. **Phase 3**: Initial stand-up of Multi-Modal Protocol Servers (Block and S3 targets first).
5. **Phase 4**: Security normalization via the IAM/Review services.
6. **Future Vision**: Rewrite of the chunk nodes to Rust for `DIRECT_IO` bare-namespace access.
