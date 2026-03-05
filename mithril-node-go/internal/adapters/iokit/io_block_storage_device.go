//go:build darwin
// +build darwin

package iokit

/*
#cgo darwin LDFLAGS: -framework CoreFoundation -framework IOKit
#include <stdlib.h>

#include <sys/mount.h>

#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/storage/IOMedia.h>
#include <IOKit/storage/IOStorageDeviceCharacteristics.h>
#include <IOKit/storage/IOBlockStorageDriver.h>

CFDictionaryRef ReadIOBlockStorageDeviceProtocolCharacteristics(io_service_t device) {
	if (device == IO_OBJECT_NULL) {
	    return NULL;
	}

	CFDictionaryRef protocolCharacteristics = (CFDictionaryRef)
        IORegistryEntryCreateCFProperty(device,
                                        CFSTR("Protocol Characteristics"),
                                        kCFAllocatorDefault,
                                        0);

    // Caller owns the returned CFDictionaryRef (if non-NULL)
    return protocolCharacteristics;
}

CFDictionaryRef ReadIOBlockStorageDeviceDeviceCharacteristics(io_service_t device) {
	if (device == IO_OBJECT_NULL) {
	    return NULL;
	}

	CFDictionaryRef deviceCharacteristics = (CFDictionaryRef)
        IORegistryEntryCreateCFProperty(device,
                                        CFSTR("Device Characteristics"),
                                        kCFAllocatorDefault,
                                        0);

    // Caller owns the returned CFDictionaryRef (if non-NULL)
    return deviceCharacteristics;
}

kern_return_t GetIOBlockStorageDeviceFromDriver(io_service_t driver, io_service_t *deviceOut) {
	io_service_t parent = IO_OBJECT_NULL;
	io_service_t current = driver;
	io_service_t device = IO_OBJECT_NULL;

	kern_return_t ret = IORegistryEntryGetParentEntry(current, kIOServicePlane, &parent);

	for (; ret == KERN_SUCCESS; ret = IORegistryEntryGetParentEntry(current, kIOServicePlane, &parent)) {
		if (IOObjectConformsTo(parent, "IOBlockStorageDevice")) {
			device = parent;
			IOObjectRetain(device);   // retain the device before returning
			break;
		}

		// move up one level, but never release driver
		if (current != driver) {
			IOObjectRelease(current);
		}
		current = parent;
	}

	// If we didn't find a device, release the last node (but never release driver)
    if (device == IO_OBJECT_NULL) {
		if (current != driver) {
			IOObjectRelease(current);
		}
    }

    *deviceOut = device;

	return ret;
}
*/
import "C"
import (
	"errors"
)

type IOBlockStorageDevice struct {
	className string
	service   IOService
}

// NewIOBlockStorageDevice creates a new IOBlockStorageDevice from an IOService handle.
func NewIOBlockStorageDevice(service IOService) (*IOBlockStorageDevice, error) {
	className, err := GetIOServiceClassName(service)
	if err != nil {
		return nil, err
	}

	return &IOBlockStorageDevice{
		className: className,
		service:   service,
	}, nil
}

// Close releases the underlying IOService handle.
func (d *IOBlockStorageDevice) Close() error {
	return CloseIOService(d.service)
}

// ClassName returns the IOKit class name of the device.
func (d *IOBlockStorageDevice) ClassName() string {
	return d.className
}

// ReadRawProtocolCharacteristics reads the Protocol Characteristics property from the IOBlockStorageDevice.
func (d *IOBlockStorageDevice) ReadRawProtocolCharacteristics() (map[string]any, error) {
	statsDict := C.ReadIOBlockStorageDeviceProtocolCharacteristics(d.service)
	if uintptr(statsDict) == 0 {
		return nil, errors.New("failed to read IOBlockStorageDevice protocol characteristics")
	}
	defer C.CFRelease(C.CFTypeRef(statsDict))

	return CFDictionary(C.CFDictionaryRef(statsDict))
}

// ReadRawDeviceCharacteristics reads the Device Characteristics property from the IOBlockStorageDevice.
func (d *IOBlockStorageDevice) ReadRawDeviceCharacteristics() (map[string]any, error) {
	deviceDict := C.ReadIOBlockStorageDeviceDeviceCharacteristics(d.service)
	if uintptr(deviceDict) == 0 {
		return nil, errors.New("failed to read IOBlockStorageDevice device characteristics")
	}
	defer C.CFRelease(C.CFTypeRef(deviceDict))

	return CFDictionary(C.CFDictionaryRef(deviceDict))
}

// GetBlockStorageDeviceFromDriver walks up the IOService tree from an IODriver
// to find the associated IOBlockStorageDevice.
func GetBlockStorageDeviceFromDriver(driver *IOBlockStorageDriver) (*IOBlockStorageDevice, error) {
	var service C.io_service_t
	kr := C.GetIOBlockStorageDeviceFromDriver(driver.service, &service)

	if err := ErrorFromKernReturn(KernReturn(kr)); err != nil {
		return nil, err
	}

	if service == C.IO_OBJECT_NULL {
		return nil, ErrIOReturnNoDevice
	}

	return NewIOBlockStorageDevice(IOService(service))
}
