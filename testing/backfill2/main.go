package main

import (
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
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

	testDir, err := os.MkdirTemp("./", "testdata_backfill2_*")
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
	testCommandLog, err := os.Create(filepath.Join(testDir, "backfill2.log"))
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
	var totalFiles = 10
	wg.Add(totalFiles)

	var metrics = map[string]hashing.Node{}

	var ring = hashing.NewJumpHashRing(3)
	ring.AddNode(server0)
	ring.AddNode(server1)
	ring.AddNode(server2)

	filesStart := time.Now()
	for i := 0; i < totalFiles; i++ {
		rets, err := whisper.ParseRetentionDefs("1s:3h,10s:3d,1m:31d")
		if err != nil {
			panic(err)
		}
		metric := fmt.Sprintf("metric_%d", i)
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
	testCommandStart := time.Now()
	testCommandCmd := exec.Command(
		"./bucky", "backfill2",
		"-metric-map-file", "./testing/data/map1.json",
		"-offload", "-w", "3", "-ignore404", "-delete",
		"-src-cluster-seed", nodeStr(server0),
		"-dst-cluster-seed", nodeStr(server1),
	)

	testCommandCmd.Stdout = testCommandLog
	testCommandCmd.Stderr = testCommandLog

	log.Printf("testCommandCmd.String() = %+v\n", testCommandCmd.String())
	if err := testCommandCmd.Run(); err != nil {
		log.Printf("failed to run command: %s", err)
		failed = true
		return
	}

	log.Printf("finished command. took %s\n", time.Since(testCommandStart))

	files0, _ := os.ReadDir(filepath.Join(testDir, "server0"))
	files1, _ := os.ReadDir(filepath.Join(testDir, "server1"))
	files2, _ := os.ReadDir(filepath.Join(testDir, "server2"))
	if len(files0) != 2 || len(files1) != 6 || len(files2) != 2 {
		log.Printf("failed to backfill command: files are not balanced.")
		failed = true
		return
	} else {
		log.Printf("backfill command succeeded.")
		failed = false
		return
	}
}
