package domain

type SpaceUtilizationStatistics struct {
	// TotalBytes is the total space of the volume in bytes.
	TotalBytes int64
	// UsedBytes is the used space of the volume in bytes.
	UsedBytes int64
	// FreeBytes is the free space of the volume in bytes.
	FreeBytes int64
}

// Raw values from /sys/block/<device>/stat
type LinuxBlockLayerStatistics struct {
	// Reads is the number of read operations completed.
	Reads int64
	// MergedReads is the number of read operations merged.
	MergedReads int64
	// SectorsRead is the number of sectors read.
	SectorsRead int64
	// ReadTicks is the total number of milliseconds that read I/O requests have waited on this block device.
	// If multiple requests are waiting, this value increases faster than real time.
	ReadTicks int64
	// Writes is the number of write operations completed.
	Writes int64
	// MergedWrites is the number of write operations merged.
	MergedWrites int64
	// SectorsWritten is the number of sectors written.
	SectorsWritten int64
	// WriteTicks is the total number of milliseconds that write I/O requests have waited on this block device.
	// If multiple requests are waiting, this value increases faster than real time.
	WriteTicks int64
	// InFlight is the number of I/O operations currently in progress.
	InFlight int64
	// IOTicks is the total number of milliseconds during which the device has had I/O requests queued.
	IOTicks int64
	// TimeInQueue is the total wait time for all I/O requests, in milliseconds.
	TimeInQueue int64
	// Discards is the number of discard operations completed.
	Discards int64
	// MergedDiscards is the number of discard operations merged.
	MergedDiscards int64
	// SectorsDiscarded is the number of sectors discarded.
	SectorsDiscarded int64
	// DiscardTicks is the total number of milliseconds that discard I/O requests have waited on this block device.
	// If multiple requests are waiting, this value increases faster than real time.
	DiscardTicks int64
	// Flushes is the number of flush operations completed.
	Flushes int64
	// FlushTicks is the total number of milliseconds that flush I/O requests have waited on this block device.
	// If multiple requests are waiting, this value increases faster than real time.
	FlushTicks int64
}

// Statistics from IOKit block storage drivers on Apple platforms.
type IOKitIOBlockStorageDriverStatistics struct {
	// Describes the number of bytes read since the block storage driver was instantiated.
	//
	// This property describes the number of bytes read since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	BytesRead int64 `plist:"Bytes (Read)"`
	// Describes the number of bytes written since the block storage driver was instantiated.
	//
	// This property describes the number of bytes written since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	BytesWritten int64 `plist:"Bytes (Write)"`
	// Describes the number of nanoseconds of latency during reads since the block storage driver was instantiated.
	//
	// This property describes the number of nanoseconds of latency during reads since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	LatentReadTime int64 `plist:"Latency Time (Read)"`
	// Describes the number of nanoseconds of latency during writes since the block storage driver was instantiated.
	//
	// This property describes the number of nanoseconds of latency during writes since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	LatentWriteTime int64 `plist:"Latency Time (Write)"`
	// Describes the number of read errors encountered since the block storage driver was instantiated.
	//
	// This property describes the number of read errors encountered since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	ReadErrors int64 `plist:"Errors (Read)"`
	// Describes the number of read retries required since the block storage driver was instantiated.
	//
	// This property describes the number of read retries required since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	ReadRetries int64 `plist:"Retries (Read)"`
	// Describes the number of read operations processed since the block storage driver was instantiated.
	//
	// This property describes the number of read operations processed since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	Reads int64 `plist:"Operations (Read)"`
	// Describes the number of nanoseconds spent performing reads since the block storage driver was instantiated.
	//
	// This property describes the number of nanoseconds spent performing reads since the block storage driver was instantiated. It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table. It has an OSNumber value.
	TotalReadTime int64 `plist:"Total Time (Read)"`
	// Describes the number of nanoseconds spent performing writes since the block storage driver was instantiated.
	//
	// This property describes the number of nanoseconds spent performing writes since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	TotalWriteTime int64 `plist:"Total Time (Write)"`
	// Describes the number of write errors encountered since the block storage driver was instantiated.
	//
	// This property describes the number of write errors encountered since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	WriteErrors int64 `plist:"Errors (Write)"`
	// Describes the number of write retries required since the block storage driver was instantiated.
	//
	// This property describes the number of write retries required since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	WriteRetries int64 `plist:"Retries (Write)"`
	// Describes the number of write operations processed since the block storage driver was instantiated.
	//
	// This property describes the number of write operations processed since the block storage driver was instantiated.
	// It is one of the statistic entries listed under the top-level kIOBlockStorageDriverStatisticsKey property table.
	// It has an OSNumber value.
	Writes int64 `plist:"Operations (Write)"`
}
