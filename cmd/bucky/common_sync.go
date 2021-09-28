package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type metricSyncer struct {
	flags struct {
		workers      int
		delete       bool
		noop         bool
		offloadFetch bool
		ignore404    bool
		verbose      bool
	}

	stat struct {
		notFound    int
		copyError   int
		deleteError int

		time struct {
			count int64
			total int64

			download, dump, fill, compress, copy, delete struct {
				count int64
				total int64
			}
		}
	}
}

func newMetricSyncer(delete, noop, offloadFetch, ignore404, verbose bool, workers int) *metricSyncer {
	var ms metricSyncer

	ms.flags.workers = workers
	ms.flags.delete = delete
	ms.flags.noop = noop
	ms.flags.offloadFetch = offloadFetch
	ms.flags.ignore404 = ignore404
	ms.flags.verbose = verbose

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

	jobcs := map[string]chan *syncJob{}
	srcThrottling := map[string]chan struct{}{}
	workerWg := new(sync.WaitGroup)
	var totalWorkers int
	for dst, srcJobs := range jobsd {
		jobc := make(chan *syncJob, ms.flags.workers)
		jobcs[dst] = jobc

		for src := range srcJobs {
			// Why src nodes have 1.5x workers: eading data is less
			// expensive than writing data, so it should be ok for
			// source node to receive some more reading requests.
			if _, ok := srcThrottling[src]; !ok {
				srcThrottling[src] = make(chan struct{}, ms.flags.workers+ms.flags.workers/2)
			}
		}

		for i := 0; i < ms.flags.workers; i++ {
			totalWorkers++

			workerWg.Add(1)
			go ms.sync(jobc, srcThrottling, workerWg)
		}
	}

	// num of old servers * num of new servers * metric workers
	log.Printf("Total workers: %d", totalWorkers)

	// Queue up and process work
	var serverWg sync.WaitGroup
	var progress int64
	for dst, jobss := range jobsd {
		for src, jobs := range jobss {
			serverWg.Add(1)

			go func(dst, src string, jobs []*syncJob) {
				jobc := jobcs[dst]

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

	serverWg.Wait()
	for _, c := range jobcs {
		close(c)
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

	if (!ignore404 && ms.stat.notFound > 0) || ms.stat.copyError > 0 || ms.stat.deleteError > 0 {
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

			srcThrottling[src] <- struct{}{}

			syncStart := time.Now()
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

			var mhstats *metricHealStats
			if ms.flags.offloadFetch {
				var err error
				mhstats, err = CopyMetric(src, dst, job.oldName)
				if err != nil {
					// errors already loggged in the func
					if errors.Is(err, errNotFound) {
						ms.stat.notFound++
					} else {
						ms.stat.copyError++
					}

					return
				}
			} else {
				metric, err := GetMetricData(src, job.oldName)
				if err != nil {
					// errors already loggged in the func
					if errors.Is(err, errNotFound) {
						ms.stat.notFound++
					} else {
						ms.stat.copyError++
					}

					return
				}
				metric.Name = job.newName
				mhstats, err = PostMetric(dst, metric)
				if err != nil {
					// errors already loggged in the func
					if errors.Is(err, errNotFound) {
						ms.stat.notFound++
					} else {
						ms.stat.copyError++
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
					ms.stat.deleteError++
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
