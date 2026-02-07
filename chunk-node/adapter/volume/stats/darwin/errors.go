//go:build darwin
// +build darwin

package darwin

/*
#cgo darwin LDFLAGS: -framework IOKit
#include <IOKit/IOKitLib.h>
#include <IOKit/IOReturn.h>
*/
import "C"

import "errors"

// Sentinel Go errors corresponding 1:1 with IOKit error constants.
var (
	ErrIOReturnError            = errors.New("kIOReturnError")
	ErrIOReturnNoMemory         = errors.New("kIOReturnNoMemory")
	ErrIOReturnNoResources      = errors.New("kIOReturnNoResources")
	ErrIOReturnIPCError         = errors.New("kIOReturnIPCError")
	ErrIOReturnNoDevice         = errors.New("kIOReturnNoDevice")
	ErrIOReturnNotPrivileged    = errors.New("kIOReturnNotPrivileged")
	ErrIOReturnBadArgument      = errors.New("kIOReturnBadArgument")
	ErrIOReturnLockedRead       = errors.New("kIOReturnLockedRead")
	ErrIOReturnLockedWrite      = errors.New("kIOReturnLockedWrite")
	ErrIOReturnExclusiveAccess  = errors.New("kIOReturnExclusiveAccess")
	ErrIOReturnBadMessageID     = errors.New("kIOReturnBadMessageID")
	ErrIOReturnUnsupported      = errors.New("kIOReturnUnsupported")
	ErrIOReturnVMError          = errors.New("kIOReturnVMError")
	ErrIOReturnInternalError    = errors.New("kIOReturnInternalError")
	ErrIOReturnIOError          = errors.New("kIOReturnIOError")
	ErrIOReturnCannotLock       = errors.New("kIOReturnCannotLock")
	ErrIOReturnNotOpen          = errors.New("kIOReturnNotOpen")
	ErrIOReturnNotReadable      = errors.New("kIOReturnNotReadable")
	ErrIOReturnNotWritable      = errors.New("kIOReturnNotWritable")
	ErrIOReturnNotAligned       = errors.New("kIOReturnNotAligned")
	ErrIOReturnBadMedia         = errors.New("kIOReturnBadMedia")
	ErrIOReturnStillOpen        = errors.New("kIOReturnStillOpen")
	ErrIOReturnRLDError         = errors.New("kIOReturnRLDError")
	ErrIOReturnDMAError         = errors.New("kIOReturnDMAError")
	ErrIOReturnBusy             = errors.New("kIOReturnBusy")
	ErrIOReturnTimeout          = errors.New("kIOReturnTimeout")
	ErrIOReturnOffline          = errors.New("kIOReturnOffline")
	ErrIOReturnNotReady         = errors.New("kIOReturnNotReady")
	ErrIOReturnNotAttached      = errors.New("kIOReturnNotAttached")
	ErrIOReturnNoChannels       = errors.New("kIOReturnNoChannels")
	ErrIOReturnNoSpace          = errors.New("kIOReturnNoSpace")
	ErrIOReturnPortExists       = errors.New("kIOReturnPortExists")
	ErrIOReturnCannotWire       = errors.New("kIOReturnCannotWire")
	ErrIOReturnNoInterrupt      = errors.New("kIOReturnNoInterrupt")
	ErrIOReturnNoFrames         = errors.New("kIOReturnNoFrames")
	ErrIOReturnMessageTooLarge  = errors.New("kIOReturnMessageTooLarge")
	ErrIOReturnNotPermitted     = errors.New("kIOReturnNotPermitted")
	ErrIOReturnNoPower          = errors.New("kIOReturnNoPower")
	ErrIOReturnNoMedia          = errors.New("kIOReturnNoMedia")
	ErrIOReturnUnformattedMedia = errors.New("kIOReturnUnformattedMedia")
	ErrIOReturnUnsupportedMode  = errors.New("kIOReturnUnsupportedMode")
	ErrIOReturnUnderrun         = errors.New("kIOReturnUnderrun")
	ErrIOReturnOverrun          = errors.New("kIOReturnOverrun")
	ErrIOReturnDeviceError      = errors.New("kIOReturnDeviceError")
	ErrIOReturnNoCompletion     = errors.New("kIOReturnNoCompletion")
)

// Map of kern_return_t → Go error.
var iokitErrors = map[C.kern_return_t]error{
	C.kIOReturnError:            ErrIOReturnError,
	C.kIOReturnNoMemory:         ErrIOReturnNoMemory,
	C.kIOReturnNoResources:      ErrIOReturnNoResources,
	C.kIOReturnIPCError:         ErrIOReturnIPCError,
	C.kIOReturnNoDevice:         ErrIOReturnNoDevice,
	C.kIOReturnNotPrivileged:    ErrIOReturnNotPrivileged,
	C.kIOReturnBadArgument:      ErrIOReturnBadArgument,
	C.kIOReturnLockedRead:       ErrIOReturnLockedRead,
	C.kIOReturnLockedWrite:      ErrIOReturnLockedWrite,
	C.kIOReturnExclusiveAccess:  ErrIOReturnExclusiveAccess,
	C.kIOReturnBadMessageID:     ErrIOReturnBadMessageID,
	C.kIOReturnUnsupported:      ErrIOReturnUnsupported,
	C.kIOReturnVMError:          ErrIOReturnVMError,
	C.kIOReturnInternalError:    ErrIOReturnInternalError,
	C.kIOReturnIOError:          ErrIOReturnIOError,
	C.kIOReturnCannotLock:       ErrIOReturnCannotLock,
	C.kIOReturnNotOpen:          ErrIOReturnNotOpen,
	C.kIOReturnNotReadable:      ErrIOReturnNotReadable,
	C.kIOReturnNotWritable:      ErrIOReturnNotWritable,
	C.kIOReturnNotAligned:       ErrIOReturnNotAligned,
	C.kIOReturnBadMedia:         ErrIOReturnBadMedia,
	C.kIOReturnStillOpen:        ErrIOReturnStillOpen,
	C.kIOReturnRLDError:         ErrIOReturnRLDError,
	C.kIOReturnDMAError:         ErrIOReturnDMAError,
	C.kIOReturnBusy:             ErrIOReturnBusy,
	C.kIOReturnTimeout:          ErrIOReturnTimeout,
	C.kIOReturnOffline:          ErrIOReturnOffline,
	C.kIOReturnNotReady:         ErrIOReturnNotReady,
	C.kIOReturnNotAttached:      ErrIOReturnNotAttached,
	C.kIOReturnNoChannels:       ErrIOReturnNoChannels,
	C.kIOReturnNoSpace:          ErrIOReturnNoSpace,
	C.kIOReturnPortExists:       ErrIOReturnPortExists,
	C.kIOReturnCannotWire:       ErrIOReturnCannotWire,
	C.kIOReturnNoInterrupt:      ErrIOReturnNoInterrupt,
	C.kIOReturnNoFrames:         ErrIOReturnNoFrames,
	C.kIOReturnMessageTooLarge:  ErrIOReturnMessageTooLarge,
	C.kIOReturnNotPermitted:     ErrIOReturnNotPermitted,
	C.kIOReturnNoPower:          ErrIOReturnNoPower,
	C.kIOReturnNoMedia:          ErrIOReturnNoMedia,
	C.kIOReturnUnformattedMedia: ErrIOReturnUnformattedMedia,
	C.kIOReturnUnsupportedMode:  ErrIOReturnUnsupportedMode,
	C.kIOReturnUnderrun:         ErrIOReturnUnderrun,
	C.kIOReturnOverrun:          ErrIOReturnOverrun,
	C.kIOReturnDeviceError:      ErrIOReturnDeviceError,
	C.kIOReturnNoCompletion:     ErrIOReturnNoCompletion,
}

// errorFromKernReturn converts a kern_return_t into a Go error.
func errorFromKernReturn(kr C.kern_return_t) error {
	if kr == C.KERN_SUCCESS {
		return nil
	}
	if err, ok := iokitErrors[kr]; ok {
		return err
	}
	return errors.New("unknown IOKit error")
}
