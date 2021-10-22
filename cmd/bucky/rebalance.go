package main

import (
	"fmt"
	"log"
	"sort"
	"strings"
)

var rebalanceConfig struct {
	allowedDsts string
}

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

Use --delete to delete metric source locations.  The default is to not remove
the source metrics.

The --no-op option will not alter any metrics and print a report of what
would have been done.

Set -w to change the number of worker threads used to upload the Whisper
DBs to the remote servers.

Set -offload=true to speed up rebalance.`

	c := NewCommand(rebalanceCommand, "rebalance", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)

	msFlags.registerFlags(c.Flag)
	c.Flag.BoolVar(&listForce, "f", false, "Force the remote daemons to rebuild their cache.")
	c.Flag.StringVar(&rebalanceConfig.allowedDsts, "allowed-dsts", "", "Only copy/rebanace metrics to the allowed destinations (ip1:port,ip2:port). By default (i.e. empty), all dsts are allowed.")
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
// It will clean up the old location unless doDelete is false.  The goal
// is to be near atomic as we can.  Metrics are removed directly after
// they have been backfilled in place.
//
// Additional host:port strings can be given via extraHostPorts to
// locate additional Buckyd daemons not in the current hash ring.  This
// will effectively drain all metrics off of these hosts.
func RebalanceMetrics(extraHostPorts []string) error {
	hostPorts := Cluster.HostPorts()
	hostPorts = append(hostPorts, extraHostPorts...)
	if len(hostPorts) == 0 || !Cluster.Healthy {
		log.Printf("Cluster is unhealthy or error finding cluster members.")
		return fmt.Errorf("Cluster is unhealthy.")
	}

	metricMap, err := InconsistentMetrics(hostPorts)
	if err != nil {
		return err // error already reported
	}
	if len(metricMap) == 0 {
		log.Printf("Cluster is balanced.")
		return nil
	}

	// build an order of jobs not dependent on location
	moves := make(map[string]int)
	servers := make([]string, 0)
	jobs := map[string]map[string][]*syncJob{}
	for src, metrics := range metricMap {
		servers = append(servers, src)
		for _, m := range metrics {
			job := new(syncJob)
			node := Cluster.Hash.GetNode(m)
			dst := fmt.Sprintf("%s:%d", node.Server, node.Port)

			job.oldName = m
			job.newName = m

			moves[src]++

			if _, ok := jobs[dst]; !ok {
				jobs[dst] = map[string][]*syncJob{}
			}
			jobs[dst][src] = append(jobs[dst][src], job)

			if msFlags.noop {
				log.Printf("[%s] %s => %s", src, m, dst)
			}
		}
	}

	if rebalanceConfig.allowedDsts != "" {
		allowm := map[string]bool{}
		for _, hostport := range strings.Split(rebalanceConfig.allowedDsts, ";") {
			allowm[strings.TrimSpace(hostport)] = true
		}

		newJobs := map[string]map[string][]*syncJob{}
		for dst, srcm := range jobs {
			if allowm[dst] {
				newJobs[dst] = srcm
			}

		}

		jobs = newJobs
	}

	sort.Strings(servers)
	for _, server := range servers {
		log.Printf("%d metrics on %s must be relocated", moves[server], server)
	}

	ms := newMetricSyncer(msFlags)

	ms.run(jobs)

	return nil
}

// rebalanceCommand runs this subcommand.
func rebalanceCommand(c Command) int {
	_, err := GetClusterConfig(HostPort)
	if err != nil {
		log.Print(err)
		return 1
	}

	var oldBuckyd []string
	for i := 0; i < c.Flag.NArg(); i++ {
		oldBuckyd = append(oldBuckyd, c.Flag.Arg(i))
	}
	err = RebalanceMetrics(oldBuckyd)

	if err != nil {
		return 1
	}
	return 0
}
