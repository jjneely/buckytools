package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

type metricSyncerFlags struct {
	workers      int
	delete       bool
	noop         bool
	offloadFetch bool
	ignore404    bool
	verbose      bool

	printDeletedMetrics bool

	goCarbonHealthCheck         bool
	goCarbonProtocol            string
	goCarbonPort                string
	goCarbonCacheThreshold      float64
	goCarbonHealthCheckInterval int
	syncSpeedUpInterval         int
	noRandomEasing              bool
	metricsPerSecond            int

	graphiteEndpoint      string
	graphiteMetricsPrefix string
	graphiteIPToHostname  bool
	graphiteStatInterval  int

	testingHelper struct {
		workerSleepSeconds int
	}
}

var msFlags = &metricSyncerFlags{}

func (msf *metricSyncerFlags) registerFlags(fs *flag.FlagSet) {
	fs.BoolVar(&msf.delete, "delete", false, "Delete metrics after moving them.")
	fs.BoolVar(&msf.noop, "no-op", false, "Do not alter metrics and print report.")
	fs.IntVar(&msf.workers, "w", 5, "Downloader threads.")
	fs.IntVar(&msf.workers, "workers", 5, "Downloader threads.")
	fs.BoolVar(&msf.offloadFetch, "offload", false, "Offload metric data fetching to data nodes.")
	fs.BoolVar(&msf.ignore404, "ignore404", false, "Do not treat 404 as errors.")
	fs.BoolVar(&msf.printDeletedMetrics, "print-deletion", false, "Print deleted metric names.")

	fs.BoolVar(&msf.goCarbonHealthCheck, "go-carbon-health-check", false, "Throttle rebalancing/backfilling if go-carbon cache size hits threshold specified -go-carbon-cache-threshold")
	fs.IntVar(&msf.goCarbonHealthCheckInterval, "go-carbon-health-check-interval", 10, "Go-Carbon health check interval (seconds). Should be less than 15 minutes.")
	fs.StringVar(&msf.goCarbonProtocol, "go-carbon-protocol", "http", "Use http or https to retrieve go-carbon /admin/info")
	fs.StringVar(&msf.goCarbonPort, "go-carbon-port", "8080", "Set carbon-server port to retrieve go-carbon /admin/info")
	fs.Float64Var(&msf.goCarbonCacheThreshold, "go-carbon-cache-threshold", 0.75, "Go-Carbon cache threshold")

	fs.IntVar(&msf.syncSpeedUpInterval, "sync-speed-up-interval", 600, "Incraese sync speed metrics-per-second by 1 at the specified interval (seconds). Requires -go-carbon-health-check. To disbale, set it to 0 or -1.")
	fs.IntVar(&msf.metricsPerSecond, "metrics-per-second", 1, "Sync rate (metrics per second) on each graphite storage (go-carbon) node. Requires -go-carbon-health-check.")
	fs.BoolVar(&msf.noRandomEasing, "no-random-easing", false, "Disable randomly slowing down metricsPerSecond. Requires -go-carbon-health-check.")

	fs.StringVar(&msf.graphiteEndpoint, "graphite-endpoint", "", "Send internal stats to graphite ingestion endpoint")
	fs.StringVar(&msf.graphiteMetricsPrefix, "graphite-metrics-prefix", "carbon.buckytools", "Internal graphite metric prefix")
	fs.BoolVar(&msf.graphiteIPToHostname, "graphite-ip-to-hostname", false, "Convert buckyd ip address to hostname in graphite metric prefix.")
	fs.IntVar(&msf.graphiteStatInterval, "graphite-stat-interval", 60, "How frequent should bucky tool generate graphite metrics (seconds)")

	fs.IntVar(&msf.testingHelper.workerSleepSeconds, "testing.worker-sleep-seconds", 0, "Testing helper flag: make worker sleep.")
}

type metricSyncer struct {
	flags *metricSyncerFlags

	stat struct {
		totalJobs    int64
		finishedJobs int64
		notFound     int64
		copyError    int64
		deleteError  int64

		nodes map[string]*syncPerNodeStat // key: buckyd address

		time struct {
			count int64
			total int64

			download, dump, fill, compress, copy, delete struct {
				count int64
				total int64
			}
		}
	}

	// goCarbonStates is nil if goCarbonHealthCheck is not enabled
	goCarbonStates map[string]*goCarbonState // key: buckyd address
}

type syncPerNodeStat struct {
	finishedJobs int64
}

func newMetricSyncer(flags *metricSyncerFlags) *metricSyncer {
	var ms metricSyncer

	ms.flags = flags
	ms.stat.nodes = map[string]*syncPerNodeStat{}

	return &ms
}

func (ms *metricSyncer) restTime() int64 {
	return atomic.LoadInt64(&ms.stat.time.total) - atomic.LoadInt64(&ms.stat.time.download.total) - atomic.LoadInt64(&ms.stat.time.dump.total) - atomic.LoadInt64(&ms.stat.time.fill.total) - atomic.LoadInt64(&ms.stat.time.compress.total) - atomic.LoadInt64(&ms.stat.time.copy.total) - atomic.LoadInt64(&ms.stat.time.delete.total)
}

type syncJob struct {
	srcServer string
	dstServer string
	oldName   string
	newName   string
}

func (ms *metricSyncer) run(jobsd map[string]map[string][]*syncJob) error {
	totalJobs := ms.countJobs(jobsd)
	log.Printf("Syncing %d metrics. Copy offload: %t.", totalJobs, ms.flags.offloadFetch)

	if ms.flags.noop {
		return errors.New("Halting.  No-op mode enganged.")
	}

	jobcs := map[string]map[string]chan *syncJob{}
	srcThrottling := map[string]chan struct{}{} // TODO: not really needed anymore
	workerWg := new(sync.WaitGroup)
	var totalWorkers int
	for dst, srcJobs := range jobsd {
		// jobc := make(chan *syncJob, ms.flags.workers)
		jobcs[dst] = map[string]chan *syncJob{}

		ms.stat.nodes[dst] = &syncPerNodeStat{}

		for src, jobs := range srcJobs {
			jobc := make(chan *syncJob, ms.flags.workers)
			jobcs[dst][src] = jobc

			ms.stat.nodes[src] = &syncPerNodeStat{}
			ms.stat.totalJobs += int64(len(jobs))

			// Why src nodes have 1.5x workers: eading data is less
			// expensive than writing data, so it should be ok for
			// source node to receive some more reading requests.
			if _, ok := srcThrottling[src]; !ok {
				srcThrottling[src] = make(chan struct{}, ms.flags.workers+ms.flags.workers/2)
			}

			for i := 0; i < ms.flags.workers; i++ {
				totalWorkers++

				workerWg.Add(1)
				go ms.sync(jobc, srcThrottling, workerWg)
			}
		}
	}

	// num of old servers * num of new servers * metric workers
	log.Printf("Total workers: %d", totalWorkers)

	if ms.flags.goCarbonHealthCheck {
		// de-duplicate server ips/servers
		servers := map[string]*goCarbonState{}
		for s := range jobsd {
			servers[s] = ms.newGoCarbonState(fmt.Sprintf("%s:%s", strings.Split(s, ":")[0], ms.flags.goCarbonPort), ms.flags.syncSpeedUpInterval)
		}
		for s := range srcThrottling {
			servers[s] = ms.newGoCarbonState(fmt.Sprintf("%s:%s", strings.Split(s, ":")[0], ms.flags.goCarbonPort), ms.flags.syncSpeedUpInterval)
		}

		go ms.monitorGoCarbonHealth(time.Second*time.Duration(ms.flags.goCarbonHealthCheckInterval), servers)
	}

	// Queue up and process work
	var serverWg sync.WaitGroup
	var progress int64
	// var servers = map[string]struct{}{}
	for dst, jobss := range jobsd {
		// servers[dst] = struct{}{}
		for src, jobs := range jobss {
			serverWg.Add(1)
			// servers[src] = struct{}{}

			go func(dst, src string, jobs []*syncJob) {
				jobc := jobcs[dst][src]

				for _, job := range jobs {
					job.srcServer = src
					job.dstServer = dst
					jobc <- job
					atomic.AddInt64(&progress, 1)
				}

				serverWg.Done()
			}(dst, src, jobs)
		}
	}

	progressDone := make(chan struct{})
	go ms.displayProgressOrExit(totalJobs, &progress, progressDone)
	if ms.flags.graphiteEndpoint != "" {
		go ms.reportToGraphite(time.Second*time.Duration(ms.flags.graphiteStatInterval), progressDone)
	}

	serverWg.Wait()
	for _, srcJobcs := range jobcs {
		for _, c := range srcJobcs {
			close(c)
		}
	}
	workerWg.Wait()

	close(progressDone)
	time.Sleep(time.Millisecond * 50) // give some time for printing the last progress info

	logbreakdownStat := func(name string, stat *struct{ count, total int64 }) {
		stotal := atomic.LoadInt64(&stat.total)
		scount := atomic.LoadInt64(&stat.count)

		ttotal := atomic.LoadInt64(&ms.stat.time.total)
		tcount := atomic.LoadInt64(&ms.stat.time.count)

		log.Printf("      %s: %s (%.2f%%) count: %d (%.2f%%)", name, time.Duration(stotal), 100*float64(stotal)/float64(ttotal), scount, 100*float64(scount)/float64(tcount))
	}

	log.Println("Sync completed:")
	log.Printf("  404 Counter: %d", ms.stat.notFound)
	log.Printf("  Copy failure: %d", ms.stat.copyError)
	log.Printf("  Delete failure: %d", ms.stat.deleteError)
	log.Printf("  Time Stats:")
	log.Printf("    Total Sync: %s", time.Duration(ms.stat.time.total))
	logbreakdownStat("Download", &ms.stat.time.download)
	logbreakdownStat("Dump", &ms.stat.time.dump)
	logbreakdownStat("Fill", &ms.stat.time.fill)
	logbreakdownStat("Compress", &ms.stat.time.compress)
	logbreakdownStat("Copy", &ms.stat.time.copy)
	logbreakdownStat("Delete", &ms.stat.time.delete)
	log.Printf("      Rest: %s (%.2f%%)", time.Duration(ms.restTime()), float64(100*ms.restTime())/float64(atomic.LoadInt64(&ms.stat.time.total)))

	if (!ms.flags.ignore404 && ms.stat.notFound > 0) || ms.stat.copyError > 0 || ms.stat.deleteError > 0 {
		log.Println("Errors are present in sync.")
		return fmt.Errorf("Errors present.")
	}

	return nil
}

// countMap returns the number of metrics in a server -> metrics mapping
func (ms *metricSyncer) countJobs(jobsd map[string]map[string][]*syncJob) int {
	c := 0
	for _, jobss := range jobsd {
		for _, jobs := range jobss {
			c = c + len(jobs)
		}
	}
	return c
}

func (ms *metricSyncer) sync(jobc chan *syncJob, srcThrottling map[string]chan struct{}, wg *sync.WaitGroup) {
	for job := range jobc {
		func() {
			src, dst := job.srcServer, job.dstServer

			// Make sure that src servers doesn't receive too many
			// read requests If this interfers with sync speed-up
			// with go-cabron health check, we can bypass the issue
			// by increase the worker count.
			srcThrottling[src] <- struct{}{}

			// go-carbon health check
			if ms.flags.goCarbonHealthCheck {
				ms.requestAccess(dst)
				ms.requestAccess(src)
			}

			syncStart := time.Now()
			atomic.AddInt64(&ms.stat.finishedJobs, 1)
			atomic.AddInt64(&ms.stat.nodes[src].finishedJobs, 1)
			atomic.AddInt64(&ms.stat.nodes[dst].finishedJobs, 1)
			defer func() {
				atomic.AddInt64(&ms.stat.time.count, 1)
				atomic.AddInt64(&ms.stat.time.total, int64(time.Since(syncStart)))

				<-srcThrottling[src]
			}()

			if ms.flags.verbose {
				log.Printf("Relocating [%s] %s => [%s] %s  Delete Source: %t",
					src, job.oldName,
					dst, job.newName, ms.flags.delete)
			}

			if ms.flags.testingHelper.workerSleepSeconds > 0 {
				time.Sleep(time.Second * time.Duration(ms.flags.testingHelper.workerSleepSeconds))
			}

			var mhstats *metricHealStats
			if ms.flags.offloadFetch {
				var err error
				mhstats, err = CopyMetric(src, dst, job.oldName, job.newName)
				if err != nil {
					// errors already loggged in the func
					if errors.Is(err, errNotFound) {
						atomic.AddInt64(&ms.stat.notFound, 1)
					} else {
						atomic.AddInt64(&ms.stat.copyError, 1)
					}

					return
				}
			} else {
				metric, err := GetMetricData(src, job.oldName)
				if err != nil {
					// errors already loggged in the func
					if errors.Is(err, errNotFound) {
						atomic.AddInt64(&ms.stat.notFound, 1)
					} else {
						atomic.AddInt64(&ms.stat.copyError, 1)
					}

					return
				}
				metric.Name = job.newName
				mhstats, err = PostMetric(dst, metric)
				if err != nil {
					// errors already loggged in the func
					if errors.Is(err, errNotFound) {
						atomic.AddInt64(&ms.stat.notFound, 1)
					} else {
						atomic.AddInt64(&ms.stat.copyError, 1)
					}

					return
				}
			}

			if mhstats != nil {
				if mhstats.Download > 0 {
					atomic.AddInt64(&ms.stat.time.download.count, 1)
					atomic.AddInt64(&ms.stat.time.download.total, mhstats.Download)
				}
				if mhstats.Dump > 0 {
					atomic.AddInt64(&ms.stat.time.dump.count, 1)
					atomic.AddInt64(&ms.stat.time.dump.total, mhstats.Dump)
				}
				if mhstats.Fill > 0 {
					atomic.AddInt64(&ms.stat.time.fill.count, 1)
					atomic.AddInt64(&ms.stat.time.fill.total, mhstats.Fill)
				}
				if mhstats.Compress > 0 {
					atomic.AddInt64(&ms.stat.time.compress.count, 1)
					atomic.AddInt64(&ms.stat.time.compress.total, mhstats.Compress)
				}
				if mhstats.Copy > 0 {
					atomic.AddInt64(&ms.stat.time.copy.count, 1)
					atomic.AddInt64(&ms.stat.time.copy.total, mhstats.Copy)
				}
			}

			// We only delete if there are no errors present
			if ms.flags.delete {
				deleteStart := time.Now()

				err := DeleteMetric(src, job.oldName)
				if err != nil {
					// errors already loggged in the func
					atomic.AddInt64(&ms.stat.deleteError, 1)
				}

				atomic.AddInt64(&ms.stat.time.delete.count, 1)
				atomic.AddInt64(&ms.stat.time.delete.total, int64(time.Since(deleteStart)))
			}
		}()
	}

	wg.Done()
}

func (ms *metricSyncer) displayProgressOrExit(total int, progress *int64, progressDone chan struct{}) {
	var start = time.Now().Unix()
	var ptime = time.Now().Unix()
	var pprogress int64
	var exit, forceExit bool
	var ticker = time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	manualQuit := make(chan os.Signal, 1)
	signal.Notify(manualQuit, syscall.SIGUSR1)

	for {
		select {
		case <-ticker.C:
		case <-manualQuit:
			forceExit = true
		case <-progressDone:
			exit = true
		}

		now := time.Now().Unix()

		deltaCurrent := now - ptime
		if deltaCurrent == 0 {
			deltaCurrent = 1
		}

		deltaAll := now - start
		if deltaAll == 0 {
			deltaAll = 1
		}

		cprogress := atomic.LoadInt64(progress)
		pdiff := cprogress - pprogress
		if pdiff == 0 {
			pdiff = 1
		}

		remaining := float64(int64(total) - cprogress)
		speedAll := float64(cprogress) / float64(deltaAll)
		speedCurrent := float64(pdiff) / float64(deltaCurrent)

		log.Printf(
			"Progress %d / %d: %.2f%%  Metrics/second: %.2f  Delete: %t ETA_All/Current: %s/%s",
			cprogress, total,
			100*float64(cprogress)/float64(total),
			float64(cprogress-pprogress)/float64(deltaCurrent),
			ms.flags.delete,
			time.Duration(remaining/speedAll)*time.Second,
			time.Duration(remaining/speedCurrent)*time.Second,
		)

		pprogress = cprogress
		ptime = now

		if exit {
			return
		} else if forceExit {
			log.Printf("Received SIGUSR1, exiting.")
			os.Exit(1)
		}
	}
}

type goCarbonState struct {
	addr       string
	cacheSize  int64
	cacheLimit int64

	refreshedAt atomic.Value

	metricsPerSecond int64         // metric per second
	syncTokens       chan struct{} // generated by metricsPerSecond
	fastReset        chan struct{} // quick go-carbon overload notification channel
}

func (ms *metricSyncer) newGoCarbonState(addr string, speedUpInterval int) *goCarbonState {
	if speedUpInterval <= 0 {
		// which means disable speed up
		speedUpInterval = int(time.Hour * 24 * 365 * 10) // in practice, it's impossible to have bucky rebalance running for 10 years.
	}

	mps := int64(ms.flags.metricsPerSecond)
	state := &goCarbonState{
		addr:             addr,
		metricsPerSecond: mps,
		syncTokens:       make(chan struct{}, 120),
		fastReset:        make(chan struct{}, 1),
	}

	max := func(v1, v2 int64) int64 {
		if v1 > v2 {
			return v1
		}
		return v2
	}

	go func() {
		speedUp := time.NewTicker(time.Second * time.Duration(speedUpInterval))
		speed := time.NewTicker(time.Second / 1)
		for {
			select {
			case <-state.fastReset:
				atomic.StoreInt64(&state.metricsPerSecond, mps)
				speed.Reset(time.Second / time.Duration(state.metricsPerSecond))
			case <-speedUp.C:
				if ms.isGoCarbonOverload(state) { // overload check
					atomic.StoreInt64(&state.metricsPerSecond, mps)
				} else if !ms.flags.noRandomEasing &&
					atomic.LoadInt64(&state.metricsPerSecond) >= mps*3 &&
					rand.Intn(10) <= 3 { // random easing
					atomic.StoreInt64(&state.metricsPerSecond, max(mps, rand.Int63n(atomic.LoadInt64(&state.metricsPerSecond))+1))
				} else { // randomly increased sync rate by 1-5 metrics per second
					atomic.AddInt64(&state.metricsPerSecond, rand.Int63n(5)+1)
				}

				// TODO: might need to add some proetections if bucky failed to talk to go-carbon at start-up.
				if refreshedAt := state.refreshedAt.Load(); refreshedAt != nil &&
					time.Since(refreshedAt.(time.Time)) >= time.Minute*15 {
					atomic.StoreInt64(&state.metricsPerSecond, mps)
				}

				speed.Reset(time.Second / time.Duration(atomic.LoadInt64(&state.metricsPerSecond)))
			case <-speed.C:
				select {
				case state.syncTokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	return state
}

func (ms *metricSyncer) isGoCarbonOverload(state *goCarbonState) bool {
	return float64(atomic.LoadInt64(&state.cacheSize)) >= float64(atomic.LoadInt64(&state.cacheLimit))*ms.flags.goCarbonCacheThreshold
}

func (ms *metricSyncer) requestAccess(server string) {
	if !ms.flags.goCarbonHealthCheck || ms.goCarbonStates == nil {
		return
	}

	state := ms.goCarbonStates[server]
	if state == nil {
		return
	}

	<-state.syncTokens

	return
}

func (ms *metricSyncer) monitorGoCarbonHealth(interval time.Duration, servers map[string]*goCarbonState) {
	ms.goCarbonStates = servers

	ticker := time.NewTicker(interval)
	for {
		<-ticker.C

		var wg sync.WaitGroup
		for _, state := range ms.goCarbonStates {
			wg.Add(1)
			go func(state *goCarbonState) {
				defer wg.Done()

				resp, err := httpClient.Get(fmt.Sprintf("%s://%s/admin/info", ms.flags.goCarbonProtocol, state.addr))
				if err != nil {
					log.Printf("Failed to retrieve health info from %s: %s", state.addr, err)
					return
				}
				defer resp.Body.Close()

				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Printf("Failed to read response from %s: %s", state.addr, body)
				}
				if resp.StatusCode != 200 {
					log.Printf("Non-200 result returned from %s: %s", state.addr, body)
					return
				}

				var info struct {
					Cache struct {
						Size  int64 `json:"size"`
						Limit int64 `json:"limit"`
					} `json:"cache"`
				}
				if err := json.Unmarshal(body, &info); err != nil {
					log.Printf("Failed to decode health info from %s: err: %s body: %s", state.addr, err, body)
					return
				}

				state.refreshedAt.Store(time.Now())
				atomic.StoreInt64(&state.cacheLimit, info.Cache.Limit)
				atomic.StoreInt64(&state.cacheSize, info.Cache.Size)

				if ms.isGoCarbonOverload(state) {
					state.fastReset <- struct{}{}
				}
			}(state)
		}

		wg.Wait()
	}
}

func (ms *metricSyncer) reportToGraphite(interval time.Duration, progressDone chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("reportToGraphite panics: %v\n%s", r, debug.Stack())
		}
	}()
	ticker := time.NewTicker(interval)
	nodeNameCache := map[string]string{}

	var exit bool
	for {
		select {
		case <-ticker.C:
		case <-progressDone:
			exit = true
		}

		tcpAddr, err := net.ResolveTCPAddr("tcp4", ms.flags.graphiteEndpoint)
		if err != nil {
			log.Printf("reportToGraphite: Failed to resolve graphite endpoint %s: %s", ms.flags.graphiteEndpoint, err)
			continue
		}
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			log.Printf("reportToGraphite: Failed to connect to graphite %s: %s", ms.flags.graphiteEndpoint, err)
			continue
		}

		ts := time.Now().Unix()
		fmt.Fprintf(conn, "%s.main.total_jobs.last %d %d\n", ms.flags.graphiteMetricsPrefix, ms.stat.totalJobs, ts)
		fmt.Fprintf(conn, "%s.main.finished_jobs.last %d %d\n", ms.flags.graphiteMetricsPrefix, ms.stat.finishedJobs, ts)
		fmt.Fprintf(conn, "%s.main.not_found.last %d %d\n", ms.flags.graphiteMetricsPrefix, ms.stat.notFound, ts)
		fmt.Fprintf(conn, "%s.main.copy_error.last %d %d\n", ms.flags.graphiteMetricsPrefix, ms.stat.copyError, ts)
		fmt.Fprintf(conn, "%s.main.delete_error.last %d %d\n", ms.flags.graphiteMetricsPrefix, ms.stat.deleteError, ts)

		for node, stat := range ms.stat.nodes {
			nodeName, ok := nodeNameCache[node]
			if !ok {
				nodeNameCache[node] = ms.getNodeName(node)
				nodeName = nodeNameCache[node]
			}

			fmt.Fprintf(conn, "%s.node.%s.finished_jobs.last %d %d\n", ms.flags.graphiteMetricsPrefix, nodeName, stat.finishedJobs, ts)

			if ms.flags.goCarbonHealthCheck {
				state := ms.goCarbonStates[node]
				fmt.Fprintf(conn, "%s.node.%s.metrics_per_second.last %d %d\n", ms.flags.graphiteMetricsPrefix, nodeName, atomic.LoadInt64(&state.metricsPerSecond), ts)
				fmt.Fprintf(conn, "%s.node.%s.cache_limit.last %d %d\n", ms.flags.graphiteMetricsPrefix, nodeName, state.cacheLimit, ts)
				fmt.Fprintf(conn, "%s.node.%s.cache_size.last %d %d\n", ms.flags.graphiteMetricsPrefix, nodeName, state.cacheSize, ts)
			}
		}

		conn.Close()

		if exit {
			break
		}
	}
}

func (ms *metricSyncer) getNodeName(src string) string {
	ip := strings.Split(src, ":")[0]
	if !ms.flags.graphiteIPToHostname {
		return strings.ReplaceAll(ip, ".", "_")
	}

	var resolver net.Resolver
	names, err := resolver.LookupAddr(context.Background(), ip)
	if err != nil || len(names) == 0 {
		if err != nil {
			log.Printf("Failed to resolve hostname for %s: %s", ip, err)
		}

		return strings.ReplaceAll(ip, ".", "_")
	}

	sort.Strings(names)
	return strings.ReplaceAll(names[0], ".", "_")
}
