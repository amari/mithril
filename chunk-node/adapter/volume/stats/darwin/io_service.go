//go:build darwin
// +build darwin

package darwin

/*
#cgo darwin LDFLAGS: -framework IOKit
#include <IOKit/IOKitLib.h>
*/
import "C"

func closeIOService(service C.io_service_t) error {
	if service != C.IO_OBJECT_NULL {
		C.IOObjectRelease(service)
		service = C.IO_OBJECT_NULL
	}

	return nil
}

func getIOServiceClassName(svc C.io_service_t) (string, error) {
	var cname C.io_name_t

	kr := C.IOObjectGetClass(C.io_object_t(svc), &cname[0])
	if err := errorFromKernReturn(kr); err != nil {
		return "", err
	}

	return C.GoString(&cname[0]), nil
}
