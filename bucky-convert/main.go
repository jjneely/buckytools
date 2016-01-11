package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

import (
	"github.com/jjneely/buckytools"
	"github.com/jjneely/buckytools/metrics"
	"github.com/jjneely/buckytools/whisper"
)

var httpClient = new(http.Client)
var host string

func usage() {
	fmt.Printf("%s [options] buckydaemon:port prefix\n", os.Args[0])
	fmt.Printf("Version: %s\n\n", buckytools.Version)
	fmt.Printf("Given prefix is walked to find .wsp files and the path is\n")
	fmt.Printf("to generate a metric name.  This metric name and associated\n")
	fmt.Printf("data points are committed to the Bucky TSJ store.")
	fmt.Printf("\n\n")
	flag.PrintDefaults()
}

func FindValidDataPoints(wsp *whisper.Whisper) (*whisper.TimeSeries, error) {
	retentions := whisper.RetentionsByPrecision{wsp.Retentions()}
	sort.Sort(retentions)

	start := int(time.Now().Unix())
	from := 0
	for _, r := range retentions.Iterator() {
		from = int(time.Now().Unix()) - r.MaxRetention()

		ts, err := wsp.Fetch(from, start)
		if err != nil {
			return nil, err
		}
		return ts, nil
	}

	// we don't get here.
	return nil, nil
}

func examine(path string, info os.FileInfo, err error) error {
	// Did the Walk function hit an error on this file?
	if err != nil {
		log.Printf("%s\n", err)
		return nil
	}

	// Sanity check our file
	if info.IsDir() {
		if strings.HasPrefix(path, ".") {
			return filepath.SkipDir
		}
		return nil
	}
	if !info.Mode().IsRegular() {
		// Not a regular file
		return nil
	}
	if !strings.HasSuffix(path, ".wsp") {
		// Not a Whisper Database
		return nil
	}

	wsp, err := whisper.Open(path)
	if err != nil {
		log.Printf("%s\n", err)
		return err
	}
	defer wsp.Close()

	metric := metrics.PathToMetric(path)
	ts, err := FindValidDataPoints(wsp)
	if err != nil {
		log.Printf("%s\n", err)
		return err
	}

	commitTimeSeries(ts, metric)
	return nil
}

func commitTimeSeries(ts *whisper.TimeSeries, metric string) {
	tsj := new(metrics.TimeSeries)
	tsj.Epoch = int64(ts.FromTime())
	tsj.Interval = int64(ts.Step())
	tsj.Values = ts.Values()

	u := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/timeseries/" + metric,
	}

	blob, err := json.Marshal(tsj)
	buf := bytes.NewBuffer(blob)
	r, err := httpClient.Post(u.String(), "application/json", buf)
	if err != nil {
		log.Printf("HTTP POST Failed: %s", err)
		return
	}
	if r.StatusCode != 200 {
		log.Printf("Failed: %s", metric)
		body, err := ioutil.ReadAll(r.Body)
		if err == nil {
			log.Printf("%s: %s", r.Status, string(body))
		} else {
			log.Printf("%s: No body", r.Status)
		}
	}
	os.Exit(2)
}

func main() {
	var version bool
	flag.Usage = usage
	flag.BoolVar(&version, "version", false, "Display version information.")
	flag.Parse()

	if version {
		fmt.Printf("Buckytools version: %s\n", buckytools.Version)
		os.Exit(0)
	}

	if flag.NArg() < 2 {
		usage()
		return
	}

	if flag.NArg() >= 2 {
		// Start our walk
		host = flag.Arg(0)
		for i := 1; i < flag.NArg(); i++ {
			err := filepath.Walk(flag.Arg(i), examine)
			if err != nil {
				log.Printf("%s\n", err)
			}
		}
	}
}
