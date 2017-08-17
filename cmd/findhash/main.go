package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
)

import "github.com/jjneely/buckytools/hashing"
import "github.com/pborman/uuid"

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

func printKeyAnalysis(hr *hashing.CarbonHashRing, file string) {
	keys := make(map[string]int)
	total := 0
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	for _, l := range strings.Split(string(data), "\n") {
		l = strings.TrimSpace(l)
		n := hr.GetNode(l)
		server := fmt.Sprintf("%s:%s", n.Server, n.Instance)
		keys[server] = keys[server] + 1
		total++
	}

	sortedKeys := make([]string, 0)
	for k := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	fmt.Printf("Keys per node:\n")
	average := float64(total) / float64(hr.Len())
	variance := float64(0)
	for _, k := range sortedKeys {
		fmt.Printf("%s\t%d\n", k, keys[k])
		variance = variance + math.Pow(float64(keys[k])-average, 2)
	}
	fmt.Printf("\nTotal Metric Keys: %d\n", total)
	fmt.Printf("Ideal keys per node: %.2f\n", average)
	fmt.Printf("Deviation: %.4f\n", math.Sqrt(variance/float64(len(keys))))
}

func printAnalysis(hr *hashing.CarbonHashRing) {
	hash := hr.BucketsPerNode()
	keys := make([]string, 0)
	min := 0xFFFF
	max := 0
	v := float64(0)
	average := float64(0xFFFF) / float64(hr.Len())

	for k := range hash {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("Node %s:\t%d\n", k, hash[k])
		if hash[k] > max {
			max = hash[k]
		}
		if hash[k] < min {
			min = hash[k]
		}
		v = v + math.Pow(float64(hash[k])-average, 2)
	}

	v = v / float64(hr.Len())
	fmt.Printf("\nIdeal bucket count per server: %.2f\n", average)
	fmt.Printf("Spread: %d - %d = %d\n", max, min, max-min)
	fmt.Printf("Deviation: %.4f\n", math.Sqrt(v))
}

func makeRing(config []string) *hashing.CarbonHashRing {
	hr := hashing.NewCarbonHashRing()
	for _, n := range config {
		fields := strings.Split(n, ":")
		if len(fields) == 1 {
			fields = append(fields, "2003")
			fields = append(fields, uuid.New())
		} else if len(fields) == 2 {
			_, err := strconv.Atoi(fields[1])
			if err != nil {
				// assume instance
				fields = append(fields, fields[1])
				fields[1] = "2003"
			} else {
				fields = append(fields, uuid.New())
			}
		} else {
			// 3 or more fields
			fields = fields[:3]
		}
		if fields[1] == "" {
			fields[1] = "2003"
		}
		if fields[2] == "" {
			fields[2] = uuid.New()
		}
		port, err := strconv.ParseUint(fields[1], 10, 16)
		if err != nil {
			port = 2003
		}
		hr.AddNode(hashing.NewNode(fields[0], int(port), fields[2]))
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
	keys := flag.String("keys", "",
		"Print analysis of key distribution using keys from the newline delimited file")
	flag.Parse()

	if flag.NArg() != 1 {
		log.Fatalf("Filename containing hash ring configuration is required")
	}

	config := getConfig(flag.Arg(0))
	if *analyze {
		hr := makeRing(config)
		printAnalysis(hr)
		if *keys != "" {
			printKeyAnalysis(hr, *keys)
		}
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
