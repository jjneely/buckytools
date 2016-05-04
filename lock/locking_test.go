package lock

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestExclusive(t *testing.T) {
	file, err := ioutil.TempFile("/tmp", "locking_test.go")
	if err != nil {
		t.Fatal(err)
	}

	// Grab an exclusive lock
	err = Exclusive(file)
	if err != nil {
		t.Fatal(err)
	}

	file2, err := os.Open(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Try to lock the second fd exclusively
	err = TryExclusive(file2)
	if err == nil {
		t.Fatalf("Attempt to acquire second lock on the same file succeeded?!")
	}
	if !IsResourceUnavailable(err) {
		t.Fatalf("Exclusive lock returned unexpected error: %s", err)
	}

	file2.Close()
	file.Close()

	os.Remove(file.Name())
}

func TestShare(t *testing.T) {
	file, err := ioutil.TempFile("/tmp", "locking_test.go")
	if err != nil {
		t.Fatal(err)
	}

	err = Share(file)
	if err != nil {
		t.Fatal(err)
	}

	// New file handle for same file
	file2, err := os.Open(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	err = TryExclusive(file2)
	if err == nil {
		t.Fatalf("Exclusive lock succeded on file already locked by another descriptor")
	}
	if !IsResourceUnavailable(err) {
		t.Fatalf("Exclusive lock returned unexpected error: %s", err)
	}

	file2.Close()
	file.Close()
}

func TestRelease(t *testing.T) {
	file, err := ioutil.TempFile("/tmp", "locking_test.go")
	if err != nil {
		t.Fatal(err)
	}

	// Grab an exclusive lock
	err = Exclusive(file)
	if err != nil {
		t.Fatal(err)
	}

	// New file handle for same file
	file2, err := os.Open(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Release first lock
	err = Release(file)
	if err != nil {
		t.Fatal(err)
	}

	err = TryExclusive(file2)
	if err != nil {
		t.Fatalf("Lock attempt failed after released! %s", err)
	}

	file2.Close()
	file.Close()
}
