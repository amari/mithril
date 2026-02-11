//go:build darwin
// +build darwin

package darwin

/*
#cgo darwin LDFLAGS: -framework CoreFoundation -framework IOKit
#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>

bool IOServiceAncestorConformsToClassName(io_service_t start, const io_name_t className) {
	io_service_t parent = IO_OBJECT_NULL;
	io_service_t current = start;

	while (IORegistryEntryGetParentEntry(current, kIOServicePlane, &parent) == KERN_SUCCESS) {
		if (IOObjectConformsTo(parent, className)) {
			if (current != start) {
				IOObjectRelease(current);
			}

			return true;
		}

		// move up one level, but never release start
		if (current != start) {
			IOObjectRelease(current);
		}
		current = parent;
	}

	// Cleanup but never release start
	if (current != start) {
		IOObjectRelease(current);
	}

	return false;
}

bool IOServiceAncestorHasPhysicalInterconnectEqualToSAS(io_service_t start) {
	io_service_t parent = IO_OBJECT_NULL;
	io_service_t current = start;

	while (IORegistryEntryGetParentEntry(current, kIOServicePlane, &parent) == KERN_SUCCESS) {
	CFTypeRef value =
            IORegistryEntryCreateCFProperty(parent,
                                            CFSTR("Physical Interconnect"),
                                            kCFAllocatorDefault,
                                            0);

        if (value) {
            if (CFGetTypeID(value) == CFStringGetTypeID()) {
                if (CFStringCompare(value, CFSTR("SAS"), 0) == kCFCompareEqualTo) {
	                if (current != start) {
						IOObjectRelease(current);
					}

					CFRelease(value);

					return true;
                }
            }
            CFRelease(value);
        }

		// move up one level, but never release start
		if (current != start) {
			IOObjectRelease(current);
		}

		current = parent;
	}

	// Cleanup but never release start
	if (current != start) {
		IOObjectRelease(current);
	}

	return false;
}
*/
import "C"
import (
	"errors"
	"unsafe"
)

// IOService is a wrapper around io_service_t for use in Go code.
type IOService = C.io_service_t

// IOObjectNull represents a null IOService handle.
const IOObjectNull = C.IO_OBJECT_NULL

// CloseIOService releases an IOService handle.
func CloseIOService(service IOService) error {
	if service != IOObjectNull {
		C.IOObjectRelease(service)
	}

	return nil
}

// GetIOServiceClassName returns the class name of an IOService.
func GetIOServiceClassName(svc IOService) (string, error) {
	var cname C.io_name_t

	kr := C.IOObjectGetClass(C.io_object_t(svc), &cname[0])
	if err := ErrorFromKernReturn(kr); err != nil {
		return "", err
	}

	return C.GoString(&cname[0]), nil
}

func CheckIOServiceTreeConformsToClassName(svc IOService, className string) (bool, error) {
	if len(className) >= 128 { // IORegistryEntryGetParentEntry uses io_name_t which is 128 bytes
		return false, errors.New("class name exceeds maximum length of 127 characters")
	}

	cClassName := C.CString(className)
	defer C.free(unsafe.Pointer(cClassName))

	return bool(C.IOServiceAncestorConformsToClassName(svc, (*C.char)(unsafe.Pointer(cClassName)))), nil
}

func CheckIOServiceTreeHasPhysicalInterconnectEqualToSAS(svc IOService) (bool, error) {
	return bool(C.IOServiceAncestorHasPhysicalInterconnectEqualToSAS(svc)), nil
}
