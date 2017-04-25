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

var deleteRegexMode bool
var deleteForce bool

type DeleteWork struct {
	server string
	name   string
}

func init() {
	usage := "[options] <metric expression>"
	short := "List out matching metrics."
	long := `Deletes the referenced metrics on the remote server.

The default mode is to work with lists.  The arguments are a series of one or
more metric key names.  If the first argument is a "-" then read a JSON array
from STDIN as our list of metrics.

Use -r to enable regular expression mode.  The first argument is a regular
expression.  If metrics names match they will be included in the output.

Use -s to only delete metrics found on the server specified by -h or the
BUCKYSERVER environment variable.`

	c := NewCommand(deleteCommand, "delete", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)
	SetupJSON(c)

	c.Flag.BoolVar(&deleteRegexMode, "r", false,
		"Filter by a regular expression.")
	c.Flag.BoolVar(&deleteForce, "noconfirm", false,
		"No confirmation.")
	c.Flag.BoolVar(&listForce, "f", false,
		"Force metric re-inventory.")
	c.Flag.IntVar(&metricWorkers, "w", 5,
		"Downloader threads.")
}

func deleteWorker(workIn chan *DeleteWork, wg *sync.WaitGroup) {
	for work := range workIn {
		err := DeleteMetric(work.server, work.name)
		if err != nil {
			workerErrors = true
		}
	}
	wg.Done()
}

func deleteMetrics(metricMap map[string][]string) error {
	wg := new(sync.WaitGroup)
	workIn := make(chan *DeleteWork) // Purposely unbuffered

	wg.Add(metricWorkers)
	for i := 0; i < metricWorkers; i++ {
		go deleteWorker(workIn, wg)
	}

	for server, metrics := range metricMap {
		if len(metrics) == 0 {
			continue
		}
		msg := fmt.Sprintf("Deleting %d metrics on %s: Please Confirm:", len(metrics), server)
		if !deleteForce && !askForConfirmation(msg) {
			continue
		}
		log.Printf("Deleting %d metrics on %s...", len(metrics), server)
		for _, m := range metrics {
			work := new(DeleteWork)
			work.server = server
			work.name = m
			workIn <- work
		}
	}

	close(workIn)
	wg.Wait()

	log.Printf("Delete operation complete.")
	if workerErrors {
		log.Printf("Errors occured in delete operation.")
		return fmt.Errorf("Errors occured in delete operations.")
	}
	return nil
}

// DeleteRegexMetrics deletes metrics matched by the given regular
// expression.
func DeleteRegexMetrics(servers []string, regex string, force bool) error {
	metricMap, err := ListRegexMetrics(servers, regex, listForce)
	if err != nil {
		return err
	}

	return deleteMetrics(metricMap)
}

// DeleteSliceMetrics deletes metrics listed in the given metrics key
// slice.
func DeleteSliceMetrics(servers []string, metrics []string, force bool) error {
	metricMap, err := ListSliceMetrics(servers, metrics, listForce)
	if err != nil {
		return err
	}

	return deleteMetrics(metricMap)
}

// DeleteJSONMetrics deletes metrics listed in the JSON array read from
// the given io.Reader.
func DeleteJSONMetrics(servers []string, fd io.Reader, force bool) error {
	// Read the JSON from the file-like object
	blob, err := ioutil.ReadAll(fd)
	metrics := make([]string, 0)

	err = json.Unmarshal(blob, &metrics)
	// We could just package this up and query the server, but lets check the
	// JSON is valid first.
	if err != nil {
		log.Printf("Error unmarshalling JSON data: %s", err)
		return err
	}

	return DeleteSliceMetrics(servers, metrics, force)
}

// deleteCommand runs this subcommand.
func deleteCommand(c Command) int {
	_, err := GetClusterConfig(HostPort)
	if err != nil {
		log.Print(err)
		return 1
	}

	if c.Flag.NArg() == 0 {
		log.Fatal("At least one argument is required.")
	} else if deleteRegexMode && c.Flag.NArg() > 0 {
		err = DeleteRegexMetrics(Cluster.HostPorts(), c.Flag.Arg(0), deleteForce)
	} else if c.Flag.Arg(0) != "-" {
		err = DeleteSliceMetrics(Cluster.HostPorts(), c.Flag.Args(), deleteForce)
	} else {
		err = DeleteJSONMetrics(Cluster.HostPorts(), os.Stdin, deleteForce)
	}

	if err != nil {
		return 1
	}
	return 0
}
