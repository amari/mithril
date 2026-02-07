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

// Turn a BSD device name into an IOMedia io_service_t
io_service_t GetIOMediaFromBSDName(const char* bsdName) {
	CFStringRef cfBSDName = CFStringCreateWithCString(kCFAllocatorDefault, bsdName, kCFStringEncodingUTF8);
	if (cfBSDName == NULL) {
		return IO_OBJECT_NULL;
	}

	CFMutableDictionaryRef matchingDict = IOServiceMatching("IOMedia");
	if (matchingDict == NULL) {
		CFRelease(cfBSDName);
		return IO_OBJECT_NULL;
	}

	CFDictionarySetValue(matchingDict, CFSTR("BSD Name"), (CFTypeRef)cfBSDName);
	CFRelease(cfBSDName);

	io_service_t service = IOServiceGetMatchingService(0, matchingDict);

	return service;
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

	// If we didn’t find a driver, release the last node (but never release media)
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

	// If we didn’t find a driver, release the last node (but never release media)
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
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/amari/mithril/chunk-node/unix"
)

type IOMedia struct {
	bsdName string
	service C.io_service_t
}

func IOMediaFromPath(path string) (*IOMedia, error) {
	buf, err := unix.Statfs(path)
	if err != nil {
		return nil, err
	}

	// TODO: validate buf.Mntfromname is a valid UTF-8 string

	rawBSDName := strings.Trim(string(buf.Mntfromname[:]), "\x00")
	bsdName := strings.TrimPrefix(filepath.Base(rawBSDName), "r")

	return IOMediaFromBSDName(bsdName)
}

func getIOMediaFromBSDName(bsdName string) (C.io_service_t, error) {
	bsdNameCString := C.CString(bsdName)
	defer C.free(unsafe.Pointer(bsdNameCString))

	media := C.GetIOMediaFromBSDName(bsdNameCString)
	if media == C.IO_OBJECT_NULL {
		return C.IO_OBJECT_NULL, ErrIOReturnNoDevice
	}

	return media, nil
}

func IOMediaFromBSDName(bsdName string) (*IOMedia, error) {
	bsdNameCString := C.CString(bsdName)
	defer C.free(unsafe.Pointer(bsdNameCString))

	media, err := getIOMediaFromBSDName(bsdName)
	if err != nil {
		return nil, err
	}

	return &IOMedia{
		bsdName: bsdName,
		service: media,
	}, nil
}

func (m *IOMedia) Close() error {
	return closeIOService(m.service)
}

func (m *IOMedia) BSDName() string {
	return m.bsdName
}

func (m *IOMedia) GetBlockStorageDriver() (*IOBlockStorageDriver, error) {
	var service C.io_service_t
	kr := C.GetIOBlockStorageDriverFromMedia2(m.service, &service)

	if err := errorFromKernReturn(kr); err != nil {
		return nil, err
	}

	if service == C.IO_OBJECT_NULL {
		return nil, ErrIOReturnNoDevice
	}

	return NewIOBlockStorageDriver(service)
}
