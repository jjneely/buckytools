package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/buckytools/hashing"
	"github.com/go-graphite/go-whisper"
)

func nodeStr(n hashing.Node) string { return fmt.Sprintf("%s:%d", n.Server, n.Port) }

func main() {
	// 1. populate metrics
	// 2. start three buckyd instances
	// 3. run rebalance

	keepTestData := flag.Bool("keep-testdata", false, "keep test data after test")
	flag.Parse()

	log.SetFlags(log.Lshortfile)

	var failed bool
	defer func() {
		if failed {
			os.Exit(1)
		}
	}()

	testDir, err := os.MkdirTemp("./", "testdata_copy_*")
	if err != nil {
		panic(err)
	}
	defer func() {
		if !*keepTestData {
			os.RemoveAll(testDir)
		}
	}()

	log0, err := os.Create(filepath.Join(testDir, "server0.log"))
	if err != nil {
		panic(err)
	}
	log1, err := os.Create(filepath.Join(testDir, "server1.log"))
	if err != nil {
		panic(err)
	}
	log2, err := os.Create(filepath.Join(testDir, "server2.log"))
	if err != nil {
		panic(err)
	}
	copyLog, err := os.Create(filepath.Join(testDir, "copy.log"))
	if err != nil {
		panic(err)
	}

	var (
		server0 = hashing.Node{Server: "127.0.1.7", Port: 4242, Instance: "server0"}
		server1 = hashing.Node{Server: "127.0.1.8", Port: 4242, Instance: "server1"}
		server2 = hashing.Node{Server: "127.0.1.9", Port: 4242, Instance: "server2"}
	)

	if err := os.MkdirAll(filepath.Join(testDir, "server0"), 0755); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Join(testDir, "server1"), 0755); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Join(testDir, "server2"), 0755); err != nil {
		panic(err)
	}

	cmd0 := exec.Command("./buckyd", "-hash", "jump_fnv1a", "-b", nodeStr(server0), "-node", server0.Server, "-prefix", filepath.Join(testDir, "server0"), "-replicas", "1", "-sparse", nodeStr(server0), nodeStr(server1), nodeStr(server2))
	cmd0.Stdout = log0
	cmd0.Stderr = log0
	if err := cmd0.Start(); err != nil {
		panic(err)
	}

	cmd1 := exec.Command("./buckyd", "-hash", "jump_fnv1a", "-b", nodeStr(server1), "-node", server1.Server, "-prefix", filepath.Join(testDir, "server1"), "-replicas", "1", "-sparse", nodeStr(server0), nodeStr(server1), nodeStr(server2))
	cmd1.Stdout = log1
	cmd1.Stderr = log1
	if err := cmd1.Start(); err != nil {
		panic(err)
	}

	cmd2 := exec.Command("./buckyd", "-hash", "jump_fnv1a", "-b", nodeStr(server2), "-node", server2.Server, "-prefix", filepath.Join(testDir, "server2"), "-replicas", "1", "-sparse", nodeStr(server0), nodeStr(server1), nodeStr(server2))
	cmd2.Stdout = log2
	cmd2.Stderr = log2
	if err := cmd2.Start(); err != nil {
		panic(err)
	}
	defer func() {
		cmd0.Process.Kill()
		cmd1.Process.Kill()
		cmd2.Process.Kill()
	}()

	mrand.Seed(time.Now().Unix())

	var wg sync.WaitGroup
	var totalFiles = 100
	wg.Add(totalFiles)

	var metrics = map[string]hashing.Node{}

	var ring = hashing.NewJumpHashRing(3)
	ring.AddNode(server0)
	ring.AddNode(server1)

	filesStart := time.Now()
	for i := 0; i < totalFiles; i++ {
		b := make([]byte, 32)
		_, err := rand.Read(b)
		if err != nil {
			panic(fmt.Errorf("failed to read random bytes: %s", err))
		}
		rets, err := whisper.ParseRetentionDefs("1s:3h,10s:3d,1m:31d")
		if err != nil {
			panic(err)
		}
		metric := fmt.Sprintf("metric_%x", b)
		node := ring.GetNode(metric)
		metrics[metric] = node
		file, err := whisper.Create(filepath.Join(testDir, node.Instance, metric+".wsp"), rets, whisper.Sum, 0)
		if err != nil {
			panic(err)
		}
		go func() {
			var points []*whisper.TimeSeriesPoint
			var now = int(time.Now().Unix()) - 1800
			for i := 0; i < 1800; i++ {
				points = append(points, &whisper.TimeSeriesPoint{Time: now + i, Value: mrand.Float64()})
			}
			if err := file.UpdateMany(points); err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	log.Printf("finished creating whisper files. took %s\n", time.Since(filesStart))

	time.Sleep(time.Second * 3)
	rebalanceStart := time.Now()
	copyCmd := exec.Command("./bucky", "copy", "-f", "-offload", "-src", nodeStr(server0), "-dst", nodeStr(server2), "-w", "3", "-ignore404")

	copyCmd.Stdout = copyLog
	copyCmd.Stderr = copyLog

	log.Printf("copyCmd.String() = %+v\n", copyCmd.String())
	if err := copyCmd.Run(); err != nil {
		log.Printf("failed to run rebalance command: %s", err)
		failed = true
		return
	}

	log.Printf("finished copying. took %s\n", time.Since(rebalanceStart))

	files0, err := os.ReadDir(filepath.Join(testDir, "server0"))
	if err != nil {
		panic(err)
	}
	files2, err := os.ReadDir(filepath.Join(testDir, "server2"))
	if err != nil {
		panic(err)
	}
	if len(files0) != len(files2) {
		log.Printf("file count doesn't match on server0 (%d) and server2 (%d)", len(files0), len(files2))
		failed = true
		return
	}

	log.Printf("%d files relocated.", len(files0))

	var inconsistentMetrics []string
	for _, m := range files0 {
		newf, err := whisper.Open(filepath.Join(testDir, "server2", m.Name()))
		if err != nil {
			panic(err)
		}
		oldf, err := whisper.Open(filepath.Join(testDir, "server0", m.Name()))
		if err != nil {
			panic(err)
		}
		nrets := newf.Retentions()
		orets := oldf.Retentions()
		if !reflect.DeepEqual(nrets, orets) {
			log.Printf("rention policy not equal:\n  new: %#v\n  old: %#v\n", nrets, orets)
		}
		now := int(time.Now().Unix())
		for _, ret := range nrets {
			ndata, err := newf.Fetch(now-ret.MaxRetention(), now)
			if err != nil {
				panic(err)
			}
			odata, err := oldf.Fetch(now-ret.MaxRetention(), now)
			if err != nil {
				panic(err)
			}
			if ndata == nil {
				log.Printf("failed to retrieve data from file %s\n", newf.File().Name())
				continue
			}
			if odata == nil {
				log.Printf("failed to retrieve data from file %s\n", newf.File().Name())
				continue
			}

			var count int
			var npoints = ndata.Points()
			var opoints = odata.Points()
			for i, opoint := range opoints {
				if !math.IsNaN(opoint.Value) && !math.IsNaN(npoints[i].Value) && opoint != npoints[i] {
					count++
					log.Printf("opoints = %+v\n", opoints[i])
					log.Printf("npoints = %+v\n", npoints[i])

					if len(inconsistentMetrics) == 0 || inconsistentMetrics[len(inconsistentMetrics)-1] != m.Name() {
						inconsistentMetrics = append(inconsistentMetrics, m.Name())
					}
				}
			}

			if count > 0 {
				log.Printf("metric %s %s: %d points not equal", m.Name(), ret, count)
			}
		}

		newf.Close()
		oldf.Close()
	}

	if len(inconsistentMetrics) > 0 {
		log.Printf("%d copied metrics not matching original metrics: %s", len(inconsistentMetrics), strings.Join(inconsistentMetrics, ","))
		failed = true
		return
	} else {
		log.Printf("metrics are copied properly.")
	}
}
