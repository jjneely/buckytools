package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

import "github.com/jjneely/buckytools"
import "github.com/jjneely/buckytools/whisper"

// Command Line Flags
var debug bool
var prefix string

func usage() {
	fmt.Printf("%s [options]\n", os.Args[0])
	fmt.Printf("Version: %s\n", buckytools.Version)
	fmt.Printf("\tWalks the PREFIX path searching for files that end in .wsp\n")
	fmt.Printf("\tand will print the file name or metric key if that Whisper\n")
	fmt.Printf("\tdatabase contains all null data points (is empty). Errors\n")
	fmt.Printf("\tgo to STDERR and filename output to STDOUT.\n\n")
	flag.PrintDefaults()
}

// examine implements the WalkFunc type for our file system walk
func examine(path string, info os.FileInfo, err error) error {
	// Did the Walk function hit an error on this file?
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
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

	wsp, err := whisper.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return err
	}
	defer wsp.Close()

	ts, count, err := buckytools.FindValidDataPoints(wsp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return err
	}

	if debug {
		fmt.Printf("%s: %d data points used out of %d in %s\n",
			path, len(ts), count, flag.Arg(0))
	} else if len(ts) == 0 {
		fmt.Println(path)
	}

	return nil
}

func main() {
	var version bool
	flag.Usage = usage
	flag.BoolVar(&version, "version", false, "Display version information.")
	flag.BoolVar(&debug, "debug", false, "Verbose output.")
	flag.BoolVar(&debug, "d", false, "Verbose output.")
	flag.StringVar(&prefix, "prefix", "/opt/graphite/storage/whisper",
		"Root of Whisper database store.")
	flag.StringVar(&prefix, "p", "/opt/graphite/storage/whisper",
		"Root of Whisper database store.")
	flag.Parse()

	if version {
		fmt.Printf("Buckytools version: %s\n", buckytools.Version)
		os.Exit(0)
	}

	prefix, err := filepath.Abs(prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	// Start our walk
	err = filepath.Walk(prefix, examine)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
