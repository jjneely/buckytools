package main

import (
	"flag"
	"fmt"
	"os"
)

import "github.com/jjneely/buckytools"
import "github.com/jjneely/buckytools/whisper"

func usage() {
	fmt.Printf("%s <wsp_file>\n", os.Args[0])
	fmt.Printf("Version: %s\n", buckytools.Version)
	fmt.Printf("\tReturns successfully if there are no non-null data points in\n")
	fmt.Printf("\tthe given Whsiper DB file.\n\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	version := flag.Bool("version", false, "Display version information.")
	quiet := flag.Bool("quiet", false, "Silence output.")
	flag.BoolVar(quiet, "q", false, "Silence output.")
	flag.Parse()

	if *version {
		fmt.Printf("Buckytools version: %s\n", buckytools.Version)
		os.Exit(0)
	}
	if flag.NArg() != 1 {
		usage()
		os.Exit(1)
	}

	wsp, err := whisper.Open(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	defer wsp.Close()

	ts, count, err := buckytools.FindValidDataPoints(wsp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if !*quiet {
		fmt.Printf("%d data points used out of %d in %s\n",
			len(ts), count, flag.Arg(0))
	}

	if len(ts) == 0 {
		os.Exit(0)
	} else {
		os.Exit(2)
	}
}
