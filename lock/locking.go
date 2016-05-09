// Package locking provides simple Flock based file locking utilities
// designed for synchronization around files on a single system.
package lock

import (
	"os"
	"syscall"
)

// Exclusive attempts to obtain an exclusive lock on the open file
// descriptor.  This will block until the lock can be obtained.
func Exclusive(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	return nil
}

// Share attempts to obtain a shared or read-only lock on the given open
// file descriptor.  Multiple processes or file descriptors may hold
// shared locks on the same file.  This will block until the lock can be
// obtained.
func Share(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH); err != nil {
		return err
	}
	return nil
}

// TryExclusive is the non-blocking form of Exclusive and will return an
// error if the lock could not be obtained immediately.
func TryExclusive(file *os.File) error {
	lock := syscall.LOCK_EX | syscall.LOCK_NB
	if err := syscall.Flock(int(file.Fd()), lock); err != nil {
		return err
	}
	return nil
}

// TryShare is the non-blocking form of Share and will return an error if the
// lock could not be obtained immediately.
func TryShare(file *os.File) error {
	lock := syscall.LOCK_SH | syscall.LOCK_NB
	if err := syscall.Flock(int(file.Fd()), lock); err != nil {
		return err
	}
	return nil
}

// Release will release the currently held exclusive or shared lock on the
// given open file descriptor.  Note that closing the file descriptor also
// releases locks currently held on it.
func Release(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return err
	}
	return nil
}

// IsResourceUnavailable is used on the errors returned by TryExclusive and
// TryShare to determine if the error means the lock could not be obtained.
// The above functions may return other errors, of course.
func IsResourceUnavailable(err error) bool {
	if errno, ok := err.(syscall.Errno); ok {
		return errno == syscall.EAGAIN
	}

	return false
}
