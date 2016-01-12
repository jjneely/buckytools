package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

import "github.com/golang/protobuf/proto"

import (
	"github.com/jjneely/buckytools"
	"github.com/jjneely/buckytools/metrics"
	"github.com/jjneely/buckytools/whisper"
)

var httpClient = new(http.Client)
var host string
var confirm bool

func usage() {
	fmt.Printf("%s [options] buckydaemon:port prefix\n", os.Args[0])
	fmt.Printf("Version: %s\n\n", buckytools.Version)
	fmt.Printf("Given prefix is walked to find .wsp files and the path is\n")
	fmt.Printf("to generate a metric name.  This metric name and associated\n")
	fmt.Printf("data points are committed to the Bucky TSJ store.")
	fmt.Printf("\n\n")
	flag.PrintDefaults()
}

func FindValidDataPoints(wsp *whisper.Whisper) (*metrics.TimeSeries, error) {
	retentions := whisper.RetentionsByPrecision{wsp.Retentions()}
	sort.Sort(retentions)

	start := int(time.Now().Unix())
	from := 0
	r := retentions.Iterator()[0]
	from = int(time.Now().Unix()) - r.MaxRetention()

	ts, err := wsp.Fetch(from, start)
	if err != nil {
		return nil, err
	}

	tsj := new(metrics.TimeSeries)
	tsj.Epoch = int64(ts.FromTime())
	tsj.Interval = int64(ts.Step())
	tsj.Values = ts.Values()
	for math.IsNaN(tsj.Values[0]) {
		tsj.Epoch = tsj.Epoch + tsj.Interval
		tsj.Values = tsj.Values[1:]
	}
	for math.IsNaN(tsj.Values[len(tsj.Values)-1]) {
		tsj.Values = tsj.Values[:len(tsj.Values)-1]
	}

	return tsj, nil
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

	log.Printf("Converting : %s", path)
	log.Printf("Metric name: %s", metric)
	commitTimeSeries(ts, metric)
	return nil
}

func commitTimeSeries(tsj *metrics.TimeSeries, metric string) {
	log.Printf("Committing: Epoch = %d, Int = %d, len(values) = %d",
		tsj.Epoch, tsj.Interval, len(tsj.Values))

	u := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/timeseries/" + metric,
	}

	blob, err := proto.Marshal(tsj)
	if err != nil {
		log.Printf("Error marshaling protobuf data: %s", err)
		return
	}
	buf := bytes.NewBuffer(blob)
	r, err := httpClient.Post(u.String(), "application/protobuf", buf)
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
	if confirm {
		confirmData(metric, tsj)
	}
}

func confirmData(metric string, data *metrics.TimeSeries) {
	u := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/timeseries/" + metric,
	}
	r, err := httpClient.Get(u.String())
	if err != nil {
		log.Printf("Error GET'ing back data: %s", err)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body to confirm data: %s", err)
		return
	}
	if r.StatusCode != 200 {
		log.Printf("GET request failed with: %s: %s", r.Status, string(body))
		return
	}
	tsj := new(metrics.TimeSeries)
	err = proto.Unmarshal(body, tsj)
	if err != nil {
		log.Printf("Could not unmarshal remote's protobuf to compare: %s", err)
		return
	}

	if tsj.Epoch != data.Epoch || tsj.Interval != data.Interval {
		log.Printf("Warning: Data returns has unmatched E/I: %d / %d",
			tsj.Epoch, tsj.Interval)
		return
	}
	if len(tsj.Values) != len(data.Values) {
		log.Printf("Warning: Value slices not the same length: %d / %d",
			len(tsj.Values), len(data.Values))
		return
	}
	var flag = true
	for i, _ := range tsj.Values {
		if !math.IsNaN(tsj.Values[i]) && !math.IsNaN(data.Values[i]) && tsj.Values[i] != data.Values[i] {
			log.Printf("Index: %d:  %v != %v", i, tsj.Values[i], data.Values[i])
			flag = false
		}
	}
	if !flag {
		log.Printf("Warning: Data in series doesn't match")
	}
}

func main() {
	var version bool
	flag.Usage = usage
	flag.BoolVar(&version, "version", false, "Display version information.")
	flag.BoolVar(&confirm, "confirm", false, "Query time series data back and compare.")
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
