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

io_service_t GetIOBlockStorageDriverFromMedia(io_service_t media) {
	io_service_t parent = IO_OBJECT_NULL;
	io_service_t current = media;
	io_service_t ret = IO_OBJECT_NULL;

	while (IORegistryEntryGetParentEntry(current, kIOServicePlane, &parent) == KERN_SUCCESS) {
		if (IOObjectConformsTo(parent, "IOBlockStorageDriver")) {
			ret = parent;
			IOObjectRetain(ret);   // retain the driver before returning
			break;
		}

		// move up one level, but never release media
		if (current != media) {
			IOObjectRelease(current);
		}
		current = parent;
	}

	// If we didn't find a driver, release the last node (but never release media)
    if (ret == IO_OBJECT_NULL) {
		if (current != media) {
			IOObjectRelease(current);
		}
    }

	return ret;
}

kern_return_t GetIOBlockStorageDriverFromMedia2(io_service_t media, io_service_t *driverOut) {
	io_service_t parent = IO_OBJECT_NULL;
	io_service_t current = media;
	io_service_t driver = IO_OBJECT_NULL;

	kern_return_t ret = IORegistryEntryGetParentEntry(current, kIOServicePlane, &parent);

	for (; ret == KERN_SUCCESS; ret = IORegistryEntryGetParentEntry(current, kIOServicePlane, &parent)) {
		if (IOObjectConformsTo(parent, "IOBlockStorageDriver")) {
			driver = parent;
			IOObjectRetain(driver);   // retain the driver before returning
			break;
		}

		// move up one level, but never release media
		if (current != media) {
			IOObjectRelease(current);
		}
		current = parent;
	}

	// If we didn't find a driver, release the last node (but never release media)
    if (driver == IO_OBJECT_NULL) {
		if (current != media) {
			IOObjectRelease(current);
		}
    }

    *driverOut = driver;

	return ret;
}
*/
import "C"
import (
	"errors"
)

// IOBlockStorageDriver wraps an IOBlockStorageDriver IOService for reading statistics.
type IOBlockStorageDriver struct {
	className string
	service   IOService
}

// NewIOBlockStorageDriver creates a new IOBlockStorageDriver from an IOService handle.
func NewIOBlockStorageDriver(service IOService) (*IOBlockStorageDriver, error) {
	className, err := GetIOServiceClassName(service)
	if err != nil {
		return nil, err
	}

	return &IOBlockStorageDriver{
		className: className,
		service:   service,
	}, nil
}

// Close releases the underlying IOService handle.
func (d *IOBlockStorageDriver) Close() error {
	return CloseIOService(d.service)
}

// ClassName returns the IOKit class name of the driver.
func (d *IOBlockStorageDriver) ClassName() string {
	return d.className
}

// ReadRawStatistics reads the Statistics property from the IOBlockStorageDriver.
func (d *IOBlockStorageDriver) ReadRawStatistics() (map[string]any, error) {
	statsDict := C.ReadIOBlockStorageDriverStatistics(d.service)
	if uintptr(statsDict) == 0 {
		return nil, errors.New("failed to read IOBlockStorageDriver statistics")
	}
	defer C.CFRelease(C.CFTypeRef(statsDict))

	return CFDictionary(C.CFDictionaryRef(statsDict))
}

// GetBlockStorageDriverFromMedia walks up the IOService tree from an IOMedia
// to find the associated IOBlockStorageDriver.
func GetBlockStorageDriverFromMedia(media *IOMedia) (*IOBlockStorageDriver, error) {
	var service C.io_service_t
	kr := C.GetIOBlockStorageDriverFromMedia2(media.service, &service)

	if err := ErrorFromKernReturn(KernReturn(kr)); err != nil {
		return nil, err
	}

	if service == C.IO_OBJECT_NULL {
		return nil, ErrIOReturnNoDevice
	}

	return NewIOBlockStorageDriver(IOService(service))
}
