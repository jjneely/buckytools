package main

import (
	"flag"
	"fmt"
	"os"
)

import "github.com/jjneely/buckytools"
import "github.com/jjneely/buckytools/fill"

func usage() {
	fmt.Printf("%s <src> <dst>\n", os.Args[0])
	fmt.Printf("Version: %s\n", buckytools.Version)
	fmt.Printf("\tCopies data points from the whisper database <src> to <dst>\n")
	fmt.Printf("\twithout overwriting existing data in <dst>.\n\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	version := flag.Bool("version", false, "Display version information.")
	flag.Parse()

	if *version {
		fmt.Printf("Buckytools version: %s\n", buckytools.Version)
		os.Exit(0)
	}
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
