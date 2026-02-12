# Volume Label Design

Volume labels are key-value string pairs that describe properties of a storage volume. Both keys and values are strings.

---

## Labels Derived from Characteristics

These labels are automatically derived from `VolumeCharacteristics` detected at runtime.

When a characteristic field is empty (zero value), no label is emitted for that category.

### Medium Labels

Derived from `VolumeCharacteristics.Medium`.

| Medium       | Label       | Characteristic                    |
|--------------|-------------|--------------------------|
| Rotational   | `hdd=true`  | `MediumTypeRotational`   |
| Solid State  | `ssd=true`  | `MediumTypeSolidState`   |

Note: Linux reports VirtIO volumes as rotational by default because the virtio-blk driver does not propagate the rotational hint from the hypervisor.

### Protocol Labels

Derived from `VolumeCharacteristics.Protocol`.

The protocol is the storage command set used to communicate with the device. It determines command semantics, queuing behavior, and feature support. A protocol is transported over an interconnect, and the same protocol can run over multiple interconnects:

| Protocol | Label        | Characteristic             | Typical Interconnects                                              |
|----------|--------------|-------------------|--------------------------------------------------------------------|
| NVMe     | `nvme=true`  | `ProtocolTypeNVMe` | PCI Express, TCP (NVMe-oF), RDMA (NVMe-oF), Fibre Channel (FC-NVMe) |
| SCSI     | `scsi=true`  | `ProtocolTypeSCSI` | SAS, USB, FireWire, TCP (iSCSI), Fibre Channel, VirtIO-SCSI        |
| ATA      | `ata=true`   | `ProtocolTypeATA`  | SATA, PATA                                                         |

#### Queue Depth

Queue depth determines how many I/O commands can be in flight simultaneously. Deeper queues improve both throughput and latency: the controller sees requests sooner and can optimize scheduling (reordering to minimize seek time on HDDs, or parallelizing across flash channels on SSDs). Shallow queues starve the controller of visibility into upcoming work.

| Protocol | Queues | Depth per Queue |
|----------|--------|-----------------|
| NVMe     | Up to 64K | Up to 64K |
| SCSI     | 1 | 64-256 |
| ATA      | 1 | Up to 32 (with NCQ) |

Note: VirtIO has two storage device types. **VirtIO-SCSI** exposes a virtual SCSI HBA and uses the SCSI protocol. **VirtIO-Blk** has its own protocol, but since it's tightly bound to the VirtIO interconnect, exposing both `virtio=true` and `virtio-blk=true` would be redundant. In practice, operators working with VMs and containers rarely care which VirtIO device type is in use, so VirtIO-Blk devices will have an empty `Protocol` in their characteristics.

Note: iSER (iSCSI Extensions for RDMA) is not currently distinguished from iSCSI. iSCSI volumes will show `tcp=true`, even when using RDMA transport.

### Interconnect Labels

Derived from `VolumeCharacteristics.Interconnect`.

| Label                | Characteristic                          |
|----------------------|--------------------------------|
| `fibre-channel=true` | `InterconnectTypeFibreChannel` |
| `firewire=true`      | `InterconnectTypeFireWire`     |
| `infiniband=true`    | `InterconnectTypeInfiniBand`   |
| `pata=true`          | `InterconnectTypePATA`         |
| `pcie=true`          | `InterconnectTypePCIExpress`   |
| `rdma=true`          | `InterconnectTypeRDMA`         |
| `sas=true`           | `InterconnectTypeSAS`          |
| `sata=true`          | `InterconnectTypeSATA`         |
| `tcp=true`           | `InterconnectTypeTCP`          |
| `usb=true`           | `InterconnectTypeUSB`          |
| `virtio=true`        | `InterconnectTypeVirtIO`       |

---

## Examples

**NVMe SSD:**
```
ssd=true nvme=true pcie=true
```

**NVMe-oF/TCP (NVMe over TCP):**
```
nvme=true tcp=true
```

**SATA HDD:**
```
hdd=true ata=true sata=true
```

**iSCSI LUN (over TCP):**
```
scsi=true tcp=true
```

**iSCSI LUN (over RDMA/iSER):**
```
scsi=true tcp=true
```
(Note: iSER is not currently distinguished; appears identical to iSCSI+TCP)

**USB External Drive:**
```
scsi=true usb=true
```

**VirtIO-Blk Disk:**
```
hdd=true virtio=true
```
(Note: Linux reports VirtIO as rotational by default, even when backed by an SSD)

**VirtIO-SCSI Disk:**
```
hdd=true scsi=true virtio=true
```
(Note: Linux reports VirtIO as rotational by default, even when backed by an SSD)
