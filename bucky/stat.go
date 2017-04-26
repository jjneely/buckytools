package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

import . "github.com/jjneely/buckytools/metrics"

func init() {
	usage := "[options] <metric expression>"
	short := "Stat the remote Whisper DB"
	long := `Report the amount of storage consumed by matching metrics.

The default mode is to work with lists.  The arguments are a series of one or
more metric key names.  If the first argument is a "-" then read a JSON array
from STDIN as our list of metrics.

Use -r to enable regular expression mode.  The first argument is a regular
expression.  If metrics names match they will be included in the output.

Use -s to only find metrics found on the server specified by -h or the
BUCKYSERVER environment variable.`

	c := NewCommand(statCommand, "stat", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)
	SetupJSON(c)

	c.Flag.BoolVar(&listRegexMode, "r", false,
		"Filter by a regular expression.")
	c.Flag.BoolVar(&listForce, "f", false,
		"Force metric re-inventory.")
	c.Flag.IntVar(&metricWorkers, "w", 5,
		"Worker threads.")
}

func statWorker(workIn chan *DeleteWork, workOut chan *MetricData, wg *sync.WaitGroup) {
	for work := range workIn {
		stat, err := StatRemoteMetric(work.server, work.name)
		if err != nil {
			workerErrors = true
		} else {
			workOut <- stat
		}
	}
	wg.Done()
}

func statResults(workOut chan *MetricData, wg *sync.WaitGroup) {
	for stat := range workOut {
		t := time.Unix(stat.ModTime, 0).UTC().Format(time.RFC3339)
		fmt.Printf("%.2fKiB\t%s\t%s\n", float64(stat.Size)/1024.0, t, stat.Name)
	}
	wg.Done()
}

func statMetrics(metricMap map[string][]string) error {
	wg := new(sync.WaitGroup)
	wg2 := new(sync.WaitGroup)
	workIn := make(chan *DeleteWork, 25)
	workOut := make(chan *MetricData, 25)

	wg.Add(metricWorkers)
	for i := 0; i < metricWorkers; i++ {
		go statWorker(workIn, workOut, wg)
	}

	wg2.Add(1)
	go statResults(workOut, wg2)

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

	log.Printf("Stat operation complete.")
	if workerErrors {
		log.Printf("Errors occured in stat operation.")
		return fmt.Errorf("Errors occured in stat operations.")
	}
	return nil
}

func StatRegexMetrics(servers []string, regex string, force bool) error {
	metricMap, err := ListRegexMetrics(servers, regex, force)
	if err != nil {
		return err
	}

	return statMetrics(metricMap)
}

func StatSliceMetrics(servers []string, metrics []string, force bool) error {
	metricMap, err := ListSliceMetrics(servers, metrics, force)
	if err != nil {
		return err
	}

	return statMetrics(metricMap)
}

func StatJSONMetrics(servers []string, fd io.Reader, force bool) error {
	// Read the JSON from the file-like object
	blob, err := ioutil.ReadAll(fd)
	metrics := make([]string, 0)

	err = json.Unmarshal(blob, &metrics)
	if err != nil {
		log.Printf("Error unmarshalling JSON data: %s", err)
		return err
	}

	return StatSliceMetrics(servers, metrics, force)
}

// statCommand runs this subcommand.
func statCommand(c Command) int {
	_, err := GetClusterConfig(HostPort)
	if err != nil {
		log.Print(err)
		return 1
	}

	if c.Flag.NArg() == 0 {
		log.Fatal("At least one argument is required.")
	} else if listRegexMode && c.Flag.NArg() > 0 {
		err = StatRegexMetrics(Cluster.HostPorts(), c.Flag.Arg(0), listForce)
	} else if c.Flag.Arg(0) != "-" {
		err = StatSliceMetrics(Cluster.HostPorts(), c.Flag.Args(), listForce)
	} else {
		err = StatJSONMetrics(Cluster.HostPorts(), os.Stdin, listForce)
	}

	if err != nil {
		return 1
	}
	return 0
}
