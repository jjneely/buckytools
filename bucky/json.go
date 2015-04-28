package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func init() {
	usage := "[options] <metric expression>"
	short := "Dump a metric expression into a JSON array."
	long := `Tool to take a list of metrics and produce a JSON array.

The arguments are a series of one or more metric key names.  If the first
argument is a "-" then read a newline separated list from STDIN as our list of
metrics.  A JSON array will be written to STDOUT containing those metrics.`

	NewCommand(jsonCommand, "json", usage, short, long)
}

// JSONSliceMetrics returns the slice of metrics as a string containing a JSON
// array listing those metrics.
func JSONSliceMetrics(metrics []string) (string, error) {
	blob, err := json.Marshal(metrics)
	if err != nil {
		log.Printf("Error marshalling data: %s", err)
		return "", err
	}
	return string(blob), nil
}

// jsonCommand runs this subcommand.
func jsonCommand(c Command) int {
	var list string
	var err error
	if c.Flag.NArg() == 0 {
		log.Fatal("At least one argument is required.")
	} else if c.Flag.Arg(0) != "-" {
		list, err = JSONSliceMetrics(c.Flag.Args())
	} else {
		metrics := make([]string, 0)
		buf, err := ioutil.ReadAll(os.Stdin)
		for _, s := range strings.Split(string(buf), "\n") {
			s = strings.TrimSpace(s)
			if s != "" {
				metrics = append(metrics, s)
			}
		}
		if err == nil {
			list, err = JSONSliceMetrics(metrics)
		}
	}

	if err != nil {
		return 1
	}

	fmt.Println(list)

	return 0
}
