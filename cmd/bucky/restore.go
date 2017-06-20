package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

import . "github.com/jjneely/buckytools/metrics"

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
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)

	c.Flag.IntVar(&metricWorkers, "w", 5,
		"Downloader threads.")
	c.Flag.IntVar(&metricWorkers, "workers", 5,
		"Downloader threads.")
	c.Flag.StringVar(&tarPrefix, "p", "",
		"Prefix all metrics in the tar file with this path.")
}

func restoreTarWorker(workIn chan *MetricData, servers []string, wg *sync.WaitGroup) {
	for work := range workIn {
		server := Cluster.Hash.GetNode(work.Name).Server
		if SingleHost && server != servers[0] {
			log.Printf("In single mode, skipping metric %s for server %s", work.Name, server)
			continue
		}
		if err := MetricEncode(work, EncSnappy); err != nil {
			log.Printf("Skipping %s due to encoding error: %s", work.Name, err)
			workerErrors = true
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
		if (hdr.Typeflag != tar.TypeRegA) && (hdr.Typeflag != tar.TypeReg) && (hdr.Typeflag != tar.TypeGNUSparse) {
			// A non-normal file, probably directory
			log.Printf("Non-restorable file/directory. Type: 0x%X Name: %s",
				hdr.Typeflag, hdr.Name)
			continue
		}

		buf := new(bytes.Buffer)
		metric := new(MetricData)
		metric.Name = RelativeToMetric(filepath.Join(tarPrefix, hdr.Name))
		metric.Size = hdr.Size
		metric.Mode = hdr.Mode
		metric.ModTime = hdr.ModTime.Unix()
		metric.Encoding = EncIdentity

		if _, err := io.Copy(buf, tr); err != nil {
			log.Printf("Error reading data from tar: %s", err)
			return err
		}
		metric.Data = buf.Bytes()
		if int64(len(metric.Data)) != metric.Size {
			log.Printf("Error: Data from tar file not the correct size.")
			return fmt.Errorf("Data from tar file not the correct size.")
		}
		// XXX: Snappy Compress for transit?
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
