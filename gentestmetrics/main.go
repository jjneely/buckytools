package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

// prefix is the string perpended to each generated metric.
var prefix string

// count is the number of metric names to generate.
var count int

// iterations is the number of timestamp/value pairs for each metric name.
var iterations int

// interval is the number of seconds between each consecutive timestamp
var interval int

// limit is the upper bound to randomly generated values
var limit int

func parseArgs() {
	flag.StringVar(&prefix, "p", "test.",
		"Prefix prepended to each metric name.")
	flag.IntVar(&count, "c", 1000,
		"Number of random metrics to generate.")
	flag.IntVar(&iterations, "x", 1,
		"How many timestamp/value pairs for each generated metric name.")
	flag.IntVar(&interval, "i", 60,
		"Seconds between each timestamp/value pair per metric.")
	flag.IntVar(&limit, "l", 100,
		"Upper limit to randomly generated integer values.")

	flag.Usage = usage
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Copyright 2015 42 Lines, Inc.\n")
	fmt.Fprintf(os.Stderr, "Original Author: Jack Neely <jjneely@42lines.net>\n\n")
	flag.PrintDefaults()
}

func main() {
	parseArgs()

	timestamp := int(time.Now().Unix())
	r := rand.New(rand.NewSource(int64(timestamp)))

	// Normalize the initial timestamp
	timestamp = timestamp - (timestamp % interval) - (interval * count)

	for i := 0; i < iterations; i++ {
		for c := 0; c < count; c++ {
			// Padding set for theoretical max of a million unique metric names
			value := r.Intn(limit)
			fmt.Printf("%s%06d %d %d\n", prefix, c, value, timestamp)
		}

		timestamp = timestamp + interval
	}
}
