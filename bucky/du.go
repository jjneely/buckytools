package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

// duTotal is the result of the du operation in bytes.
var duTotal int

func init() {
	usage := "[options] <metric expression>"
	short := "Find the storage space used."
	long := `Report the amount of storage consumed by matching metrics.

The default mode is to work with lists.  The arguments are a series of one or
more metric key names.  If the first argument is a "-" then read a JSON array
from STDIN as our list of metrics.

Use -r to enable regular expression mode.  The first argument is a regular
expression.  If metrics names match they will be included in the output.

Use -s to only find metrics found on the server specified by -h or the
BUCKYSERVER environment variable.`

	c := NewCommand(duCommand, "du", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)
	SetupJSON(c)

	c.Flag.BoolVar(&listRegexMode, "r", false,
		"Filter by a regular expression.")
	c.Flag.BoolVar(&listForce, "f", false,
		"Force metric re-inventory.")
	c.Flag.IntVar(&metricWorkers, "w", 5,
		"Downloader threads.")
}

func duWorker(workIn chan *DeleteWork, workOut chan int, wg *sync.WaitGroup) {
	for work := range workIn {
		stat, err := StatRemoteMetric(work.server, work.name)
		if err != nil {
			workerErrors = true
		} else {
			workOut <- int(stat.Size)
		}
	}
	wg.Done()
}

func duResults(workOut chan int, wg *sync.WaitGroup) {
	for size := range workOut {
		duTotal = duTotal + size
	}
	wg.Done()
}

func duMetrics(metricMap map[string][]string) (int, error) {
	wg := new(sync.WaitGroup)
	wg2 := new(sync.WaitGroup)
	workIn := make(chan *DeleteWork, 25)
	workOut := make(chan int, 25)

	wg.Add(metricWorkers)
	for i := 0; i < metricWorkers; i++ {
		go duWorker(workIn, workOut, wg)
	}

	wg2.Add(1)
	go duResults(workOut, wg2)

	c := 0
	l := countMap(metricMap)
	for server, metrics := range metricMap {
		if len(metrics) == 0 {
			continue
		}
		for _, m := range metrics {
			work := new(DeleteWork)
			work.server = server
			work.name = m
			workIn <- work
			c++
			if c%100 == 0 {
				log.Printf("Progress: %d/%d %.2f%%", c, l, float64(c)/float64(l)*100)
			}
		}
	}

	close(workIn)
	wg.Wait()

	close(workOut)
	wg2.Wait()

	log.Printf("Du operation complete.")
	if workerErrors {
		log.Printf("Errors occured in du operation.")
		return duTotal, fmt.Errorf("Errors occured in du operations.")
	}
	return duTotal, nil
}

func DuRegexMetrics(servers []string, regex string, force bool) (int, error) {
	metricMap, err := ListRegexMetrics(servers, regex, force)
	if err != nil {
		return 0, err
	}

	return duMetrics(metricMap)
}

func DuSliceMetrics(servers []string, metrics []string, force bool) (int, error) {
	metricMap, err := ListSliceMetrics(servers, metrics, force)
	if err != nil {
		return 0, err
	}

	return duMetrics(metricMap)
}

func DuJSONMetrics(servers []string, fd io.Reader, force bool) (int, error) {
	// Read the JSON from the file-like object
	blob, err := ioutil.ReadAll(fd)
	metrics := make([]string, 0)

	err = json.Unmarshal(blob, &metrics)
	// We could just package this up and query the server, but lets check the
	// JSON is valid first.
	if err != nil {
		log.Printf("Error unmarshalling JSON data: %s", err)
		return 0, err
	}

	return DuSliceMetrics(servers, metrics, force)
}

// duCommand runs this subcommand.
func duCommand(c Command) int {
	_, err := GetClusterConfig(HostPort)
	if err != nil {
		log.Print(err)
		return 1
	}

	var storage int
	if c.Flag.NArg() == 0 {
		log.Fatal("At least one argument is required.")
	} else if listRegexMode && c.Flag.NArg() > 0 {
		storage, err = DuRegexMetrics(Cluster.HostPorts(), c.Flag.Arg(0), listForce)
	} else if c.Flag.Arg(0) != "-" {
		storage, err = DuSliceMetrics(Cluster.HostPorts(), c.Flag.Args(), listForce)
	} else {
		storage, err = DuJSONMetrics(Cluster.HostPorts(), os.Stdin, listForce)
	}

	log.Printf("%d Bytes", storage)
	log.Printf("%.2f MiB", float64(storage)/float64(1024*1024))
	log.Printf("%.2f GiB", float64(storage)/float64(1024*1024*1024))

	if err != nil {
		return 1
	}
	return 0
}
