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
*/
import "C"
import (
	"path/filepath"
	"strings"
	"unsafe"

	adaptersunix "github.com/amari/mithril/mithril-node-go/internal/adapters/unix"
)

// IOMedia represents an IOMedia service in the IOKit registry.
type IOMedia struct {
	bsdName string
	service IOService
}

// BSDNameFromPath returns the BSD device name for the filesystem containing the given path.
func BSDNameFromPath(path string) (string, error) {
	buf, err := adaptersunix.Statfs(path)
	if err != nil {
		return "", err
	}

	// TODO: validate buf.Mntfromname is a valid UTF-8 string

	rawBSDName := strings.Trim(string(buf.Mntfromname[:]), "\x00")
	bsdName := strings.TrimPrefix(filepath.Base(rawBSDName), "r")

	return bsdName, nil
}

// IOMediaFromPath creates an IOMedia from a filesystem path.
func IOMediaFromPath(path string) (*IOMedia, error) {
	bsdName, err := BSDNameFromPath(path)
	if err != nil {
		return nil, err
	}

	return IOMediaFromBSDName(bsdName)
}

// GetIOMediaFromBSDName returns the raw IOService for a BSD device name.
// The caller is responsible for releasing the service with CloseIOService.
func GetIOMediaFromBSDName(bsdName string) (IOService, error) {
	bsdNameCString := C.CString(bsdName)
	defer C.free(unsafe.Pointer(bsdNameCString))

	media := C.GetIOMediaFromBSDName(bsdNameCString)
	if media == IOObjectNull {
		return IOObjectNull, ErrIOReturnNoDevice
	}

	return media, nil
}

// IOMediaFromBSDName creates an IOMedia from a BSD device name (e.g., "disk0").
func IOMediaFromBSDName(bsdName string) (*IOMedia, error) {
	media, err := GetIOMediaFromBSDName(bsdName)
	if err != nil {
		return nil, err
	}

	return &IOMedia{
		bsdName: bsdName,
		service: media,
	}, nil
}

// Close releases the IOMedia's underlying IOService handle.
func (m *IOMedia) Close() error {
	return CloseIOService(m.service)
}

// BSDName returns the BSD device name for this IOMedia.
func (m *IOMedia) BSDName() string {
	return m.bsdName
}

// Service returns the underlying IOService handle.
func (m *IOMedia) Service() IOService {
	return m.service
}
