package main

import "log"

type copyCommand struct {
	src, dst  string
	listForce bool
	*metricSyncer
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

	c.Flag.StringVar(&copyCmd.src, "src", "", "Source host to copy metrics from.")
	c.Flag.StringVar(&copyCmd.dst, "dst", "", "Destination host to copy metrics to.")
	c.Flag.BoolVar(&copyCmd.listForce, "f", false, "Force the remote daemons to rebuild their cache.")

	msFlags.registerFlags(c.Flag)

	copyCmd.metricSyncer = newMetricSyncer(msFlags)
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
