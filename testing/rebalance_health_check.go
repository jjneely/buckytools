package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	mrand "math/rand"
	"net"
	"net/http"
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

	testDir, err := os.MkdirTemp("./", "testdata_rebalance_health_check_*")
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
	rebalanceLog, err := os.Create(filepath.Join(testDir, "rebalance.log"))
	if err != nil {
		panic(err)
	}

	// sudo ip addr add 10.0.1.7 dev lo
	// sudo ip addr add 10.0.1.8 dev lo
	// sudo ip addr add 10.0.1.9 dev lo
	var (
		server0 = hashing.Node{Server: "10.0.1.7", Port: 40000, Instance: "server0"}
		server1 = hashing.Node{Server: "10.0.1.8", Port: 40001, Instance: "server1"}
		server2 = hashing.Node{Server: "10.0.1.9", Port: 40002, Instance: "server2"}
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

	go func() {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:33002")
		if err != nil {
			log.Printf("Failed to resolve 127.0.0.1:33002: %s", err)
			os.Exit(1)
		}
		listener, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			log.Printf("Failed to listen 127.0.0.1:33002: %s", err)
			os.Exit(1)
		}
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Failed to accept graphite data: %s", err)
				continue
			}
			data, err := ioutil.ReadAll(conn)
			if err != nil {
				continue
			}
			log.Printf("metrics:\n%s\n", data)
			conn.Close()
		}
	}()

	for _, node := range []hashing.Node{server0, server1, server2} {
		go func(node hashing.Node) {
			mux := http.NewServeMux()
			start := time.Now()
			mux.HandleFunc("/admin/info", func(w http.ResponseWriter, r *http.Request) {
				if time.Since(start) >= time.Second*10 && time.Since(start) <= time.Second*20 {
					fmt.Fprintf(w, `{"cache": {"size": 1000, "limit": 100000}}`)
				} else {
					fmt.Fprintf(w, `{"cache": {"size": 1000, "limit": 1000}}`)
				}
			})

			s := &http.Server{
				Addr:           fmt.Sprintf("%s:8080", node.Server),
				Handler:        mux,
				ReadTimeout:    10 * time.Second,
				WriteTimeout:   10 * time.Second,
				MaxHeaderBytes: 1 << 20,
			}

			log.Fatal(s.ListenAndServe())
		}(node)
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
	// rebalanceCmd := exec.Command("./bucky", "rebalance", "-f", "-h", nodeStr(server0), "-offload", "-w", "3", "-ignore404")
	rebalanceCmd := exec.Command(
		"./bucky", "rebalance", "-f", "-h", nodeStr(server0),
		"-offload", "-ignore404",
		"-testing.worker-sleep-seconds", "10",
		"-graphite-ip-to-hostname",
		"-graphite-metrics-prefix", "carbon.minutely.buckytool.rebalance.misc.dst.ams4a.src.ams4b",
		"-graphite-endpoint", "127.0.0.1:33002",
		"-graphite-stat-interval", "3",
		"-go-carbon-health-check=true",
		"-go-carbon-health-check-interval", "3",
		"-go-carbon-port", "8080",
		"-go-carbon-protocol", "http",
		"-go-carbon-cache-threshold", "0.75",
		// "-min-workers", "2",
		"-sync-speed-up-interval", "2",
		"-workers", "10",
	)

	rebalanceCmd.Stdout = rebalanceLog
	rebalanceCmd.Stderr = rebalanceLog

	log.Printf("rebalanceCmd.String() = %+v\n", rebalanceCmd.String())
	if err := rebalanceCmd.Run(); err != nil {
		log.Printf("failed to run rebalance command: %s", err)
		failed = true
		return
	}

	log.Printf("finished rebalancing. took %s\n", time.Since(rebalanceStart))

	files, err := os.ReadDir(filepath.Join(testDir, "server2"))
	if err != nil {
		panic(err)
	}
	if len(files) == 0 {
		log.Printf("failed to rebalance cluster: 0 files are relocated.")
		failed = true
		return
	}

	log.Printf("%d files relocated.", len(files))

	var inconsistentMetrics []string
	for _, m := range files {
		newf, err := whisper.Open(filepath.Join(testDir, "server2", m.Name()))
		if err != nil {
			panic(err)
		}
		oldf, err := whisper.Open(filepath.Join(testDir, metrics[strings.TrimSuffix(m.Name(), ".wsp")].Instance, m.Name()))
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
		log.Printf("%d rebalanced metrics not matching original metrics: %s", len(inconsistentMetrics), strings.Join(inconsistentMetrics, ","))
		failed = true
		return
	} else {
		log.Printf("metrics are rebalanced properly.")
	}
}

func nodeStr(n hashing.Node) string { return fmt.Sprintf("%s:%d", n.Server, n.Port) }
