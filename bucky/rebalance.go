package main

import (
	"fmt"
	"log"
	"sync"
)

var noDelete bool
var noOp bool

func init() {
	usage := "[options] [additional buckyd servers...]"
	short := "Rebalance a server or the entire cluster."
	long := `Locate metrics on the wrong server and move them.

Rebalance is a non-destructive operation that will find metrics that
are on the wrong server or that have duplicates and will near-atomically
move them to the correct server and backfill as needed.

You may optionally specify the network locations of other Buckyd daemons
as arguments to this command.  Metrics found via these daemons will be
relocated according to the hash ring.  This is useful for moving all
metrics off of a server when removing it from the cluster.  Metrics will be
deleted per normal according to the --no-delete flag.

Use -s to operate on metrics found on the initial host given by -h or the
BUCKYHOST environment variable.  Cluster health is not checked.  Moves that
result in metrics that live on a different host will be completed, so other
hosts will be affected even with -s.

Use --no-delete to leave the old metrics in place.  The default is to
remove metrics from their old location after they have been moved and
backfilled to the new location.

The --no-op option will not alter any metrics and print a report of what
would have been done.

Set -w to change the number of worker threads used to upload the Whisper
DBs to the remote servers.`

	c := NewCommand(rebalanceCommand, "rebalance", usage, short, long)
	SetupHostname(c)
	SetupSingle(c)

	c.Flag.BoolVar(&noDelete, "no-delete", false,
		"Do not delete metrics after moving them.")
	c.Flag.BoolVar(&noOp, "no-op", false,
		"Do not alter metrics and print report.")
	c.Flag.IntVar(&metricWorkers, "w", 5,
		"Downloader threads.")
	c.Flag.IntVar(&metricWorkers, "workers", 5,
		"Downloader threads.")
	c.Flag.BoolVar(&listForce, "f", false,
		"Force the remote daemons to rebuild their cache.")
}

func rebalanceWorker(workIn chan *MigrateWork, noDelete bool, wg *sync.WaitGroup) {
	for work := range workIn {
		log.Printf("Relocating [%s] %s => [%s] %s  Delete Source: %t",
			work.oldLocation, work.oldName,
			work.newLocation, work.newName, !noDelete)
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
			continue
		}

		// We only delete if there are no errors present
		if !noDelete {
			err = DeleteMetric(work.oldLocation, work.oldName)
			if err != nil {
				workerErrors = true
			}
		}
	}
	wg.Done()
}

// countMap returns the number of metrics in a server -> metrics mapping
func countMap(metricsMap map[string][]string) int {
	c := 0
	for _, metrics := range metricsMap {
		c = c + len(metrics)
	}
	return c
}

// RebalanceMetrics will relocate metrics on the wrong server or duplicate
// metrics and move them to the correct server, backfilling as needed.
// It will clean up the old location unless noDelete is true.  The goal
// is to be near atomic as we can.  Metrics are removed directly after
// they have been backfilled in place.
//
// Additional host:port strings can be given via extraHostPorts to
// locate additional Buckyd daemons not in the current hash ring.  This
// will effectively drain all metrics off of these hosts.
func RebalanceMetrics(noDelete bool, extraHostPorts []string) error {
	hostPorts := GetAllBuckyd()
	hostPorts = append(hostPorts, extraHostPorts...)
	if len(hostPorts) == 0 {
		log.Printf("Cluster is unhealthy or error finding cluster members.")
		return fmt.Errorf("Cluster is unhealthy.")
	}
	hr := buildHashRing(GetRings())

	metricMap, err := InconsistentMetrics(hostPorts)
	if err != nil {
		return err // error already reported
	}
	if len(metricMap) == 0 {
		log.Printf("Cluster is balanced.")
		return nil
	}

	l := countMap(metricMap)
	log.Printf("Relocating %d metrics.", l)
	workIn := make(chan *MigrateWork, 25)
	wg := new(sync.WaitGroup)
	wg.Add(metricWorkers)
	for i := 0; i < metricWorkers; i++ {
		go rebalanceWorker(workIn, noDelete, wg)
	}

	// build an order of jobs not dependent on location
	jobs := make(map[string]*MigrateWork)
	for server, metrics := range metricMap {
		for _, m := range metrics {
			work := new(MigrateWork)
			work.oldName = m
			work.newName = m
			work.oldLocation = server
			work.newLocation = hr.GetNode(work.newName).Server

			id := fmt.Sprintf("[%s] %s", server, m)
			jobs[id] = work

			if noOp {
				log.Printf("%s => %s", id, work.newLocation)
			}
		}
	}

	if noOp {
		log.Fatal("Halting.  No-op mode enganged.")
	}

	// Queue up and process work
	c := 0
	for work := range jobs {
		workIn <- jobs[work]
		c++
		if c%10 == 0 {
			log.Printf("Progress %d / %d: %.2f", c, l, 100*float64(c)/float64(l))
		}
	}

	close(workIn)
	wg.Wait()

	log.Printf("Rebalance complete.")
	if workerErrors {
		log.Printf("Errors are present in rebalance.")
		return fmt.Errorf("Errors present.")
	}
	return nil
}

// rebalanceCommand runs this subcommand.
func rebalanceCommand(c Command) int {
	var err error
	var oldBuckyd []string

	for i := 0; i < c.Flag.NArg(); i++ {
		oldBuckyd = append(oldBuckyd, c.Flag.Arg(i))
	}
	err = RebalanceMetrics(noDelete, oldBuckyd)

	if err != nil {
		return 1
	}
	return 0
}
