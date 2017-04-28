package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
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

// metricsFile may contain a file which contains a newline delimited list
// of metric names.  Use this list rather than a count of randomly generated
// names.
var metricsFile string

// value is an optional value for each generated metric name / timestamp.
var value int

var startTime int64

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
	flag.IntVar(&value, "v", math.MinInt64,
		"Use the given value rather than a random integer.")
	flag.StringVar(&metricsFile, "f", "",
		"Use metric names found in this file.  Overrides -c.")
	flag.Int64Var(&startTime, "s", time.Now().Unix(),
		"Time stamp to start -- iterations go BACKWARDS from here")

	flag.Usage = usage
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Copyright 2015 42 Lines, Inc.\n")
	fmt.Fprintf(os.Stderr, "Original Author: Jack Neely <jjneely@42lines.net>\n\n")
	flag.PrintDefaults()
}

func getIterator() []string {
	ret := make([]string, 0)
	if metricsFile == "" {
		// We just count
		// Padding set for theoretical max of a million unique metric names
		for c := 0; c < count; c++ {
			ret = append(ret, fmt.Sprintf("%06d", c))
		}
	} else {
		blob, err := ioutil.ReadFile(metricsFile)
		if err != nil {
			log.Fatal(err)
		}
		for _, s := range strings.Split(string(blob), "\n") {
			s = strings.TrimSpace(s)
			if s != "" {
				ret = append(ret, s)
			}
		}
	}

	return ret
}

func main() {
	parseArgs()

	timestamp := int(startTime)
	r := rand.New(rand.NewSource(int64(time.Now().Unix())))

	// Normalize the initial timestamp
	timestamp = timestamp - (timestamp % interval) - (interval * (iterations - 1))

	for i := 0; i < iterations; i++ {
		for _, metric := range getIterator() {
			if value != math.MinInt64 {
				fmt.Printf("%s%s %d %d\n", prefix, metric, value, timestamp)
			} else {
				v := r.Intn(limit)
				fmt.Printf("%s%s %d %d\n", prefix, metric, v, timestamp)
			}
		}

		timestamp = timestamp + interval
	}
}
