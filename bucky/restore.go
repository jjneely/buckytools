package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
)

import "github.com/jjneely/buckytools/metrics"

var tarPrefix string

func init() {
	usage := "[options] <tar file>"
	short := "Restore a tar archive of metrics back to Graphite."
	long := `Restores metrics from a tar archive back to the Graphite cluster.

A uncompressed tar archive must be specifed.  If the first argument is "-"
then the tar archive will be read from STDIN.  Metrics are extracted from
the archive and placed on the correct host in the Graphite cluster according
to the consistent hash ring.

Use -s to only restore metrics to the host specified by -h or the BUCKYSERVER
environment variable.  That hosts hash ring dictates the ring and only metrics
that hash to this hostname will be restored.  Cluster health and the
consistency of the hash ring is not verified.

If this tar file contains a specific directory of metrics that is not rooted at
the top level whisper storage directory on the Graphite servers you can use the
-p option to provide an additional path based prefix.  Joining the given prefix
path and the path contained in the tar file must result in the relative path to
the metric on the Graphite server rooted at the whisper storage directory.

Set -w to change the number of worker threads used to upload the Whisper
DBs to the remote servers.`

	c := NewCommand(restoreCommand, "restore", usage, short, long)
	SetupHostname(c)
	SetupSingle(c)

	c.Flag.IntVar(&metricWorkers, "w", 5,
		"Downloader threads.")
	c.Flag.IntVar(&metricWorkers, "workers", 5,
		"Downloader threads.")
	c.Flag.StringVar(&tarPrefix, "p", "",
		"Prefix all metrics in the tar file with this path.")
}

// PostMetric sends a POST request with new metric data to the given server.
// A post request does a backfill if this metric is already present on disk.
func PostMetric(server string, metric *MetricData) error {
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", server, Cluster.Port),
		Path:   "/metrics/" + metric.Name,
	}

	buf := bytes.NewBuffer(metric.Data)
	r, err := http.NewRequest("POST", u.String(), buf)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return err
	}
	r.Header.Set("Content-Type", "application/octet-stream")

	// This doesn't return until the backfill operation completes
	resp, err := httpClient.Do(r)
	if err != nil {
		log.Printf("Error communicating with server: %s", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		msg := fmt.Sprintf("Error reported by server: %s for metric %s",
			resp.Status, metric.Name)
		log.Printf("%s", msg)
		return fmt.Errorf("%s", msg)
	}

	return nil
}

func restoreTarWorker(workIn chan *MetricData, servers []string, wg *sync.WaitGroup) {
	for work := range workIn {
		server := Cluster.Hash.GetNode(work.Name).Server
		if SingleHost && server != servers[0] {
			log.Printf("In single mode, skipping metric %s for server %s", work.Name, server)
			continue
		}
		log.Printf("Uploading %s => %s", work.Name, server)
		err := PostMetric(server, work)
		if err != nil {
			workerErrors = true
		}
	}
	wg.Done()
}

func RestoreTar(servers []string, fd *os.File) error {
	wg := new(sync.WaitGroup)
	workIn := make(chan *MetricData, 25)
	tr := tar.NewReader(fd)

	wg.Add(metricWorkers)
	for i := 0; i < metricWorkers; i++ {
		go restoreTarWorker(workIn, servers, wg)
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		if err != nil {
			log.Printf("Error reading tar archive: %s", err)
			return err
		}
		buf := new(bytes.Buffer)
		metric := new(MetricData)
		metric.Name = metrics.RelativeToMetric(filepath.Join(tarPrefix, hdr.Name))
		metric.Size = hdr.Size
		metric.Mode = hdr.Mode
		metric.ModTime = hdr.ModTime.Unix()

		if _, err := io.Copy(buf, tr); err != nil {
			log.Printf("Error reading data from tar: %s", err)
			return err
		}
		metric.Data = buf.Bytes()
		if int64(len(metric.Data)) != metric.Size {
			log.Printf("Error: Data from tar file not the correct size.")
			return fmt.Errorf("Data from tar file not the correct size.")
		}
		workIn <- metric
	}

	close(workIn)
	wg.Wait()

	log.Printf("Restore complete.")
	if workerErrors {
		log.Printf("Errors are present in restore.")
		return fmt.Errorf("Errors uploading metric data present.")
	}
	return nil
}

// restoreCommand runs this subcommand.
func restoreCommand(c Command) int {
	_, err := GetClusterConfig(HostPort)
	if err != nil {
		log.Print(err)
		return 1
	}

	if c.Flag.NArg() == 0 {
		log.Fatal("At least one argument is required.")
	}
	if !Cluster.Healthy {
		log.Printf("Cluster is not optimal.")
		return 1
	}

	if c.Flag.Arg(0) != "-" {
		fd, err := os.Open(c.Flag.Arg(0))
		if err != nil {
			log.Fatal("Error opening tar archive: %s", err)
		}
		err = RestoreTar(Cluster.HostPorts(), fd)
	} else {
		err = RestoreTar(Cluster.HostPorts(), os.Stdin)
	}

	if err != nil {
		return 1
	}
	return 0
}
