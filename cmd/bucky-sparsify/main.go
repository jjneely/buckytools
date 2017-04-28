package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

import (
	"github.com/jjneely/buckytools"
	"github.com/jjneely/buckytools/lock"
)

// Command Line Flags
var debug bool

// count is our running total of encountered files
var count int

func copySparse(dst *os.File, src io.Reader) (written int64, err error) {
	buf := make([]byte, 4*1024)   // work in 4k chunks
	zeros := make([]byte, 4*1024) // reference slice of zeros
	size := int64(0)
	for {
		nr, er := src.Read(buf)
		size += int64(nr)
		if bytes.Equal(zeros[:nr], buf[:nr]) {
			// a block of zeros, seek
			_, err = dst.Seek(int64(nr), 1)
			if err != nil {
				break
			}
			written += int64(nr)
		} else if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			// Success, set the file size
			dst.Truncate(size)
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	return written, err
}

func usage() {
	fmt.Printf("%s [options] <path>\n", os.Args[0])
	fmt.Printf("Version: %s\n\n", buckytools.Version)
	fmt.Printf(`Finds .wsp Whisper DBs by walking the given path, create an exclusive
lock on each file and re-write it as a sparse file.  Can reclaim a significant
amount of space on a Graphite storage node. All errors and logs are reported
to STDERR.`)
	fmt.Printf("\n\n")
	flag.PrintDefaults()
}

// examine implements the WalkFunc type for our file system walk
func examine(path string, info os.FileInfo, err error) error {
	// Did the Walk function hit an error on this file?
	if err != nil {
		log.Printf("%s\n", err)
		return nil
	}

	// Sanity check our file
	if info.IsDir() {
		if strings.HasPrefix(path, ".") {
			return filepath.SkipDir
		}
		return nil
	}
	if !info.Mode().IsRegular() {
		// Not a regular file
		return nil
	}
	if !strings.HasSuffix(path, ".wsp") {
		// Not a Whisper Database
		return nil
	}

	if debug {
		log.Printf("Rewriting: %s", path)
	}
	return rewrite(path)
}

// Do the actual rewrite of the file -- path must be a Whisper DB
func rewrite(path string) error {
	// Open the existing file
	file, err := os.Open(path)
	if err != nil {
		log.Printf("%s", err)
		return err
	}
	defer file.Close()

	// Exclusive lock the file -- may block -- released when file closed
	err = lock.Exclusive(file)
	if err != nil {
		log.Printf("%s", err)
		return err
	}

	// Create .sparse tmp file
	tmpPath := path + ".sparse"
	sparse, err := os.Create(tmpPath)
	if err != nil {
		log.Printf("%s", err)
		return err
	}
	_, err = copySparse(sparse, file)
	sparse.Close()
	if err != nil {
		log.Printf("%s", err)
		return err
	}

	// Rename the file
	err = os.Rename(tmpPath, path)
	if err != nil {
		log.Printf("%s", err)
		return err
	}

	// No error, lock will now be released and old file removed
	count++
	return nil
}

func main() {
	var version bool
	flag.Usage = usage
	flag.BoolVar(&version, "version", false, "Display version information.")
	flag.BoolVar(&debug, "debug", false, "Verbose output.")
	flag.BoolVar(&debug, "d", false, "Verbose output.")
	flag.Parse()

	if version {
		fmt.Printf("Buckytools version: %s\n", buckytools.Version)
		os.Exit(0)
	}

	// Walk each path/file given as an argument
	for i := 0; i < flag.NArg(); i++ {
		err := filepath.Walk(flag.Arg(i), examine)
		if err != nil {
			log.Printf("%s", err)
		}
	}

	if debug {
		log.Printf("Re-wrote %d Whisper files", count)
	}
}
