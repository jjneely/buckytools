package main

import (
	"crypto/sha1"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randomFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buff := make([]byte, 7*1024)
	_, err = rand.Read(buff)
	if err != nil {
		return err
	}

	_, err = file.Write(buff)
	if err != nil {
		return err
	}

	zeros := make([]byte, 18*1024)
	_, err = file.Write(zeros)
	if err != nil {
		return err
	}
	_, err = file.Write(buff)
	if err != nil {
		return err
	}

	// a 32KiB file that can be made sparse
	return nil
}

func hash(path string) (string, error) {
	buff := make([]byte, 32*1024) // We only use the first 32KiB of the file
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Read(buff)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("% x", sha1.Sum(buff)), nil
}

func TestCopySparse(t *testing.T) {
	src := "sourcefile"
	dst := "sourcefile.sparse"

	err := randomFile(src)
	if err != nil {
		t.Fatalf("%s", err)
	}
	srcHash, err := hash(src)
	if err != nil {
		t.Fatalf("%s", err)
	}

	srcFile, err := os.Open(src)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer srcFile.Close()
	dstFile, err := os.Create(dst)
	if err != nil {
		t.Fatalf("%s", err)
	}

	n, err := copySparse(dstFile, srcFile)
	dstFile.Close()
	if err != nil {
		t.Fatalf("%s", err)
	}
	dstHash, err := hash(dst)
	if err != nil {
		t.Fatalf("%s", err)
	}

	fmt.Printf("%d bytes copied.\n", n)
	fmt.Printf("source hash: %s\n", srcHash)
	fmt.Printf("dest hash  : %s\n", dstHash)

	if srcHash != dstHash {
		t.Errorf("Sparse file does not contain the same content as source!")
	} else {
		fmt.Printf("Files are equal.\n")
	}
}
