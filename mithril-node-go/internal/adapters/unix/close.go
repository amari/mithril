package adaptersunix

import "golang.org/x/sys/unix"

func Close(fd int) error {
	return ignoringEINTR(func() error { return unix.Close(fd) })
}
