package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strings"
)

import "github.com/jjneely/buckytools/hashing"
import "code.google.com/p/go-uuid/uuid"

func getConfig(file string) []string {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	ret := make([]string, 0)
	for _, l := range strings.Split(string(data), "\n") {
		l = strings.TrimSpace(l)
		if len(l) == 0 || l[0] == '#' {
			continue
		}

		ret = append(ret, l)
	}

	return ret
}

func printAnalysis(hr *hashing.HashRing) {
	hash := hr.BucketsPerNode()
	keys := make([]string, 0)
	for k := range hash {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("Node %s:\t%d\n", k, hash[k])
	}
}

func makeRing(config []string) *hashing.HashRing {
	hr := hashing.NewHashRing()
	for _, n := range config {
		fields := strings.Split(n, ":")
		if len(fields) < 2 {
			fields = append(fields, uuid.New())
		} else if fields[1] == "" {
			fields[1] = uuid.New()
		}
		hr.AddNode(hashing.NewNode(fields[0], fields[1]))
	}

	return hr
}

func min(buckets map[string]int) int {
	min := 0xFFFF
	for _, v := range buckets {
		if v < min {
			min = v
		}
	}

	return min
}

func max(buckets map[string]int) int {
	max := 0
	for _, v := range buckets {
		if v > max {
			max = v
		}
	}

	return max
}

func main() {
	bestMax := flag.Int("max", 1900, "Maximum allowed buckets per host")
	bestMin := flag.Int("min", 1300, "Minimum allowed buckets per host")
	filter := flag.String("filter", "",
		"Show results of hosts matching this prefix")
	analyze := flag.Bool("analyze", false,
		"Print Hashring analysis of given configuration")
	flag.Parse()

	if flag.NArg() != 1 {
		log.Fatalf("Filename containing hash ring configuration is required")
	}

	config := getConfig(flag.Arg(0))
	average := float64(0xFFFF) / float64(len(config))
	fmt.Printf("Ideal bucket count per server: %.2f\n", average)
	if *analyze {
		printAnalysis(makeRing(config))
		return
	}

	spread := *bestMax - *bestMin
	for {
		hr := makeRing(config)
		buckets := hr.BucketsPerNode()

		maximum := max(buckets)
		minimum := min(buckets)

		if spread > maximum-minimum {
			fmt.Printf("Possible solution:\n")
			fmt.Printf("\tMax buckets per host: %d\n", maximum)
			fmt.Printf("\tMin buckets per host: %d\n", minimum)

			for k := range buckets {
				if strings.HasPrefix(k, *filter) {
					fmt.Printf("\t%s\n", k)
				}
			}

			spread = maximum - minimum
		}
	}
}
