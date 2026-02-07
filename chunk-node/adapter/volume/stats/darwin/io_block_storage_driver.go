//go:build darwin
// +build darwin

package darwin

/*
#cgo darwin LDFLAGS: -framework CoreFoundation -framework IOKit
#include <stdlib.h>

#include <sys/mount.h>

#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/storage/IOMedia.h>
#include <IOKit/storage/IOStorageDeviceCharacteristics.h>
#include <IOKit/storage/IOBlockStorageDriver.h>

CFDictionaryRef ReadIOBlockStorageDriverStatistics(io_service_t driver) {
	if (driver == IO_OBJECT_NULL) {
	    return NULL;
	}

	CFDictionaryRef stats = (CFDictionaryRef)
        IORegistryEntryCreateCFProperty(driver,
                                        CFSTR("Statistics"),
                                        kCFAllocatorDefault,
                                        0);

    // Caller owns the returned CFDictionaryRef (if non-NULL)
    return stats;
}
*/
import "C"
import "errors"

type IOBlockStorageDriver struct {
	className string
	service   C.io_service_t
}

func NewIOBlockStorageDriver(service C.io_service_t) (*IOBlockStorageDriver, error) {
	className, err := getIOServiceClassName(service)
	if err != nil {
		return nil, err
	}

	return &IOBlockStorageDriver{
		className: className,
		service:   service,
	}, nil
}

func (d *IOBlockStorageDriver) Close() error {
	return closeIOService(d.service)
}

func (d *IOBlockStorageDriver) ClassName() string {
	return d.className
}

func (d *IOBlockStorageDriver) ReadRawStatistics() (map[string]any, error) {
	statsDict := C.ReadIOBlockStorageDriverStatistics(d.service)
	if uintptr(statsDict) == 0 {
		return nil, errors.New("failed to read IOBlockStorageDriver statistics")
	}
	defer C.CFRelease(C.CFTypeRef(statsDict))

	return CFDictionary(statsDict)
}
