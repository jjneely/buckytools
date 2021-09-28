package main

import "log"

type copyCommand struct {
	src, dst  string
	listForce bool
	metricSyncer
}

func init() {
	usage := "[options] [additional buckyd servers...]"
	short := "Copy metrics from one server to another."
	long := `TODO`

	var copyCmd copyCommand

	c := NewCommand(copyCmd.do, "copy", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)

	// c.Flag.BoolVar(&doDelete, "delete", false, "Delete metrics after moving them.")
	c.Flag.StringVar(&copyCmd.src, "src", "", "Source host to copy metrics from.")
	c.Flag.StringVar(&copyCmd.dst, "dst", "", "Destination host to copy metrics to.")
	c.Flag.BoolVar(&copyCmd.flags.noop, "no-op", false, "Do not alter metrics and print report.")
	c.Flag.IntVar(&copyCmd.flags.workers, "w", 5, "Downloader threads.")
	c.Flag.IntVar(&copyCmd.flags.workers, "workers", 5, "Downloader threads.")
	c.Flag.BoolVar(&copyCmd.listForce, "f", false, "Force the remote daemons to rebuild their cache.")
	c.Flag.BoolVar(&copyCmd.flags.offloadFetch, "offload", false, "Offload metric data fetching to data nodes.")
	c.Flag.BoolVar(&copyCmd.flags.ignore404, "ignore404", false, "Do not treat 404 as errors.")
}

// rebalanceCommand runs this subcommand.
func (cc *copyCommand) do(c Command) int {
	cc.flags.verbose = Verbose

	if cc.src == "" || cc.dst == "" {
		log.Printf("Please specify -src and -dst.")
		return 1
	}

	metricsMap, err := ListAllMetrics([]string{cc.src}, cc.listForce)
	if err != nil {
		log.Printf("Failed to retrieve metrics: %s", err)
		return 1
	}

	log.Printf("Number of metrics to copy: %d", len(metricsMap[cc.src]))

	jobs := map[string]map[string][]*syncJob{cc.dst: {}}
	for _, m := range metricsMap[cc.src] {
		jobs[cc.dst][cc.src] = append(jobs[cc.dst][cc.src], &syncJob{oldName: m, newName: m})
	}

	cc.run(jobs)

	return 0
}
