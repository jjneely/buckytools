package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

// import "github.com/jjneely/buckytools/hashing"

type MigrateWork struct {
	oldName     string
	newName     string
	oldLocation string
	newLocation string
}

func init() {
	usage := "[options] <file containing JSON map>"
	short := "Backfill or rename old metrics to new."
	long := `Merge data from an old metric into a new metric name.

Backfill will non-destructively fill a new metric with data from the old.
It uses the same algorithm as whisper-fill.py which fills in missing data
points in the destination without overwriting existing points.

You must supply a file name as the first argument which must contain a
JSON map or hash of old metric name => new metric name.  If the first
argument is "-" then the JSON map is read from STDIN.

Use -s to operate on source or old metrics found on the initial host given
by -h or the BUCKYHOST environment variable.  Cluster health is not checked.
Backfills that result in metrics that live on a different host will be
completed, so other hosts will be affected even with -s.

This operation is idempotent.  Repeated runs will find new metrics already
filled and not overwrite data points.  The old or source metrics are not
modified or removed.

Set -w to change the number of worker threads used to upload the Whisper
DBs to the remote servers.`

	c := NewCommand(backfillCommand, "backfill", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)

	c.Flag.IntVar(&metricWorkers, "w", 5,
		"Downloader threads.")
	c.Flag.IntVar(&metricWorkers, "workers", 5,
		"Downloader threads.")
	c.Flag.BoolVar(&listForce, "f", false,
		"Force the remote daemons to rebuild their cache.")
}

func backfillWorker(workIn chan *MigrateWork, wg *sync.WaitGroup) {
	for work := range workIn {
		if Verbose {
			log.Printf("Backfilling [%s] %s => [%s] %s",
				work.oldLocation, work.oldName,
				work.newLocation, work.newName)
		}
		metric, err := GetMetricData(work.oldLocation, work.oldName)
		if err != nil {
			// errors already handled
			workerErrors = true
			continue
		}
		metric.Name = work.newName
		err = PostMetric(work.newLocation, metric)
		if err != nil {
			// errors already handled
			workerErrors = true
		}
	}
	wg.Done()
}

// BackfillMetrics takes a list of Graphite servers to operate on and a map
// of old metric => new metric.  The new metric name will have the data from
// the old metric backfilled into it.
func BackfillMetrics(metricMap map[string]string) error {
	hostPorts := Cluster.HostPorts()
	if len(hostPorts) == 0 || !Cluster.Healthy {
		log.Printf("Cluster is unhealthy or error finding cluster members.")
		return fmt.Errorf("Cluster is unhealthy.")
	}

	// Generate a list of srouce metrics
	srcMetrics := make([]string, 0)
	for k := range metricMap {
		srcMetrics = append(srcMetrics, k)
	}

	log.Printf("Requesting backfill of %d metrics.", len(srcMetrics))
	locations, err := ListSliceMetrics(hostPorts, srcMetrics, listForce)
	if err != nil {
		return err
	}

	// re-create our map, so that we spread out load, and lessen complexity
	backfillJob := make(map[string]string)
	for server, metrics := range locations {
		for _, m := range metrics {
			backfillJob[m] = server
		}
	}

	workIn := make(chan *MigrateWork, 25)
	wg := new(sync.WaitGroup)
	wg.Add(metricWorkers)
	for i := 0; i < metricWorkers; i++ {
		go backfillWorker(workIn, wg)
	}

	c := 0
	l := len(backfillJob)
	t := time.Now().Unix()
	for m, server := range backfillJob {
		work := new(MigrateWork)
		work.oldName = m
		work.newName = CleanMetric(metricMap[m])
		work.oldLocation = server
		work.newLocation = Cluster.Hash.GetNode(work.newName).Server

		workIn <- work
		c++
		if c%10 == 0 {
			now := time.Now().Unix()
			s := now - t
			if s == 0 {
				s = 1
			}
			log.Printf("Progress %d / %d: %.2f  Metrics/second: %.2f",
				c, l,
				100*float64(c)/float64(l),
				float64(c)/float64(s))
		}
	}

	close(workIn)
	wg.Wait()
	log.Printf("Backfill request complete.")
	if workerErrors {
		log.Printf("Errors are present.")
		return fmt.Errorf("Backfill errors are present.")
	}

	return nil
}

// readBackFillMap reads a JSON map from the file descriptor that is of the
// for old metric => new metric
func readBackfillMap(fd *os.File) (map[string]string, error) {
	blob, err := ioutil.ReadAll(fd)
	if err != nil {
		log.Printf("Error reading file descriptor: %s", err)
		return nil, err
	}

	metrics := make(map[string]string)
	err = json.Unmarshal(blob, &metrics)
	if err != nil {
		log.Printf("Error unmarshalling JSON data: %s", err)
		return nil, err
	}

	return metrics, nil
}

// backfillCommand runs this subcommand.
func backfillCommand(c Command) int {
	if c.Flag.NArg() == 0 {
		log.Fatal("At least one argument is required.")
	}

	var err error
	var fd *os.File
	_, err = GetClusterConfig(HostPort)
	if err != nil {
		log.Print(err)
		return 1
	}

	if c.Flag.Arg(0) != "-" {
		fd, err = os.Open(c.Flag.Arg(0))
		if err != nil {
			log.Fatal("Error opening json map: %s", err)
		}
		defer fd.Close()
	} else {
		fd = os.Stdin
	}

	metricMap, err := readBackfillMap(fd)
	if err == nil {
		err = BackfillMetrics(metricMap)
	}

	if err != nil {
		return 1
	}
	return 0
}
