//go:build darwin
// +build darwin

package darwin

/*
#cgo darwin LDFLAGS: -framework CoreFoundation
#include <CoreFoundation/CoreFoundation.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// CFNumber converts a CFNumberRef to a Go numeric type.
func CFNumber(number C.CFNumberRef) (any, error) {
	switch C.CFNumberGetType(number) {
	case C.kCFNumberSInt8Type:
		var num C.int8_t
		if C.CFNumberGetValue(number, C.kCFNumberSInt8Type, unsafe.Pointer(&num)) != 0 {
			return int8(num), nil
		}
	case C.kCFNumberSInt16Type:
		var num C.int16_t
		if C.CFNumberGetValue(number, C.kCFNumberSInt16Type, unsafe.Pointer(&num)) != 0 {
			return int16(num), nil
		}
	case C.kCFNumberSInt32Type:
		var num C.int32_t
		if C.CFNumberGetValue(number, C.kCFNumberSInt32Type, unsafe.Pointer(&num)) != 0 {
			return int32(num), nil
		}
	case C.kCFNumberSInt64Type:
		var num C.int64_t
		if C.CFNumberGetValue(number, C.kCFNumberSInt64Type, unsafe.Pointer(&num)) != 0 {
			return int64(num), nil
		}
	default:
		return nil, fmt.Errorf("unsupported CFNumber type: %d", C.CFNumberGetType(number))
	}

	return nil, fmt.Errorf("failed to convert CFNumber to Go number")
}

// CFString converts a CFStringRef to a Go string.
func CFString(str C.CFStringRef) (string, error) {
	if uintptr(str) == 0 {
		return "", nil
	}

	length := C.CFStringGetLength(str)
	cap := C.CFStringGetMaximumSizeForEncoding(length, C.kCFStringEncodingUTF8) + 1

	buf := make([]C.char, cap)
	if C.CFStringGetCString(str, &buf[0], C.CFIndex(cap), C.kCFStringEncodingUTF8) == 0 {
		return "", fmt.Errorf("failed to convert CFString to Go string")
	}

	// Safe to convert now b/c buf is null-terminated
	return C.GoString(&buf[0]), nil
}

// CFType converts a CFTypeRef to a Go type.
func CFType(v C.CFTypeRef) (any, error) {
	if uintptr(v) == 0 {
		return nil, nil
	}

	switch C.CFGetTypeID(v) {
	case C.CFArrayGetTypeID():
		return CFArray((C.CFArrayRef)(v))
	case C.CFDataGetTypeID():
		return CFData((C.CFDataRef)(v))
	case C.CFDictionaryGetTypeID():
		return CFDictionary((C.CFDictionaryRef)(v))
	case C.CFNumberGetTypeID():
		return CFNumber((C.CFNumberRef)(v))
	case C.CFStringGetTypeID():
		return CFString((C.CFStringRef)(v))
	default:
		return nil, fmt.Errorf("unsupported CFTypeRef type ID: %d", C.CFGetTypeID(v))
	}
}

// CFData converts a CFDataRef to a Go byte slice.
func CFData(data C.CFDataRef) ([]byte, error) {
	if uintptr(data) == 0 {
		return nil, nil
	}

	length := C.CFDataGetLength(data)
	if length == 0 {
		return []byte{}, nil
	}

	buf := make([]byte, length)
	C.CFDataGetBytes(data, C.CFRange{0, length}, (*C.UInt8)(unsafe.Pointer(&buf[0])))

	return buf, nil
}

// CFArray converts a CFArrayRef to a Go slice.
func CFArray(array C.CFArrayRef) ([]any, error) {
	count := C.CFArrayGetCount(array)
	result := make([]any, count)

	for i := C.CFIndex(0); i < count; i++ {
		item := C.CFArrayGetValueAtIndex(array, i)
		value, err := CFType((C.CFTypeRef)(item))
		if err != nil {
			return nil, fmt.Errorf("failed to convert CFArray item to Go type: %w", err)
		}
		result[i] = value
	}

	return result, nil
}

// CFDictionary converts a CFDictionaryRef to a Go map.
func CFDictionary(dict C.CFDictionaryRef) (map[string]any, error) {
	result := make(map[string]any)

	count := C.CFDictionaryGetCount(dict)
	if count == 0 {
		return result, nil
	}

	keys := make([]C.CFStringRef, count)
	values := make([]C.CFTypeRef, count)

	C.CFDictionaryGetKeysAndValues(dict,
		(*unsafe.Pointer)(unsafe.Pointer(&keys[0])),
		(*unsafe.Pointer)(unsafe.Pointer(&values[0])),
	)

	for i := range count {
		key, err := CFString(keys[i])
		if err != nil {
			return nil, fmt.Errorf("failed to convert CFDictionary key to string: %w", err)
		}

		value, err := CFType(values[i])
		if err != nil {
			return nil, fmt.Errorf("failed to convert CFDictionary value to Go type: %w", err)
		}

		result[key] = value
	}

	return result, nil
}
