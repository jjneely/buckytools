package main

import (
	"flag"
	"fmt"
	"os"
)

import "github.com/jjneely/buckytools/fill"

// XXX: A generic version for the entire suite, should be in a
// more appropriate place
const version = "0.0.1"

func usage() {
	fmt.Printf("%s <src> <dst>\n", os.Args[0])
	fmt.Printf("Version: %s\n", version)
	fmt.Printf("\tCopies data points from the whisper database <src> to <dst>\n")
	fmt.Printf("\twithout overwriting existing data in <dst>.\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 2 {
		usage()
		os.Exit(1)
	}

	err := fill.All(flag.Arg(0), flag.Arg(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occured:\n\t%s\n", err)
		os.Exit(2)
	}
}
