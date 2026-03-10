# mithril

mithril is a software-defined storage platform for the real world. From a single Raspberry Pi to a multi-node cloud cluster, it runs on what you have, speaks the protocols you need, and gets out of your way. Built in Go and Rust. Inspired by Ceph, MinIO, and Facebook's Tectonic.

Early-stage and experimental.

## Design Principles

**Mechanical sympathy.** mithril understands what it runs on, and stays out of the way.

**Versatile storage backends.** Filesystems and raw disks. CMR, SMR and ZNS. No hostile disk takeover.

**Append-only.** No read-modify-write.

**Fail fast and early.** Errors are typed, precise, and surfaced immediately.

**Idempotency.** All mutating operations are idempotent, making retries and recovery safe by design.

**Use commodity open-source software. Don't reinvent the wheel.**

**Cloud-native.** gRPC, OpenTelemetry. Kubernetes (planned).

**Cross-platform.** Linux and macOS today. Windows and FreeBSD on the roadmap.

## Components

- [`mithril-node-go`](./mithril-node-go) chunk storage node (Go)
- [`mithril-sdk-rs`](./mithril-sdk-rs) SDK core (Rust)
- [`proto/mithril`](./proto/mithril) protobuf definitions

## Roadmap

- Platform support: Windows, FreeBSD
- Storage backends: ZoneFS (SMR HDDs, NVMe ZNS), Direct I/O (raw block devices), SPDK (bare NVMe namespaces)
- Multi-modal storage.
  - Disk: iSCSI, NBD, NVMe-oF
  - File: NFS, SMB, AFP, 9P, SFTP, TFTP, WebDAV, FUSE
  - Object: S3, Swift
  - HDFS
- Event-driven architecture: Kafka, NATS Streaming
- Background workers: chunk GC, block repair, scrubbing
- Kubernetes: Helm chart, CSI driver
- SDKs: Go, Python, Node.js, Swift, .NET, Kotlin, Deno

## License

AGPL-3.0
