package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

import (
	"github.com/jjneely/buckytools"
	"github.com/jjneely/buckytools/metrics"
	"github.com/jjneely/buckytools/whisper"
)

// Command Line Flags
var debug bool
var metricName bool
var lastDP bool

func usage() {
	fmt.Printf("%s [options]\n", os.Args[0])
	fmt.Printf("Version: %s\n", buckytools.Version)
	fmt.Printf("Walks the PREFIX path searching for files that end in .wsp\n")
	fmt.Printf("and will print the file name or metric key if that Whisper\n")
	fmt.Printf("database contains all null data points (is empty). Errors\n")
	fmt.Printf("go to STDERR and filename output to STDOUT.\n\n")
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

	wsp, err := whisper.Open(path)
	if err != nil {
		log.Printf("%s\n", err)
		return err
	}
	defer wsp.Close()

	ts, count, err := buckytools.FindValidDataPoints(wsp)
	if err != nil {
		log.Printf("%s\n", err)
		return err
	}

	if lastDP {
		if len(ts) > 0 {
			fmt.Printf("%s: Most recent DP: %s\t%.2f\n",
				path, ts[0].Time, ts[0].Value)
		} else {
			fmt.Printf("%s: No valid data points\n", path)
		}
	}
	if debug {
		fmt.Printf("%s: %d data points used out of %d in %s\n",
			path, len(ts), count, flag.Arg(0))
	}
	if !debug && !lastDP && len(ts) == 0 {
		if metricName {
			fmt.Println(metrics.PathToMetric(path))
		} else {
			fmt.Println(path)
		}
	}

	return nil
}

func main() {
	var version bool
	flag.Usage = usage
	flag.BoolVar(&version, "version", false, "Display version information.")
	flag.BoolVar(&debug, "debug", false, "Verbose output.")
	flag.BoolVar(&debug, "d", false, "Verbose output.")
	flag.BoolVar(&metricName, "m", false,
		"Output metric names rather than paths.")
	flag.BoolVar(&metricName, "metricname", false,
		"Output metric names rather than paths.")
	flag.BoolVar(&lastDP, "l", false,
		"Show the most recent valid data point.")
	flag.BoolVar(&lastDP, "last", false,
		"Show the most recent valid data point.")
	flag.Parse()

	if version {
		fmt.Printf("Buckytools version: %s\n", buckytools.Version)
		os.Exit(0)
	}

	if flag.NArg() > 0 {
		// Handle command line given WSP files
		for _, p := range flag.Args() {
			if !strings.HasSuffix(p, ".wsp") {
				log.Fatalf("%s: Not a .wsp file", p)
			}
			stat, err := os.Stat(p)
			if err != nil {
				log.Fatalf("%s\n", err)
			}
			examine(p, stat, nil)
		}
	} else {
		// Start our walk
		err := filepath.Walk(metrics.Prefix, examine)
		if err != nil {
			log.Fatalf("%s\n", err)
		}
	}
}
