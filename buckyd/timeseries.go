package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

import "github.com/golang/protobuf/proto"

import "github.com/jjneely/buckytools/metrics"
import "github.com/jjneely/journal/timeseries"
import "github.com/jjneely/journal"

func serveTimeSeries(w http.ResponseWriter, r *http.Request) {
	logRequest(r)

	metric := r.URL.Path[len("/timeseries/"):]
	if len(metric) == 0 {
		http.Error(w, "Metric name missing.", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		getTimeSeries(w, r, metric)
	case "POST":
		postTimeSeries(w, r, metric)
	default:
		http.Error(w, "Bad method request.", http.StatusBadRequest)
	}
}

func getTimeSeries(w http.ResponseWriter, r *http.Request, metric string) {
	// XXX: Need to know about the data partitions we have on disk
	// XXX: Support Whisper DB fallback?
	var from, until int64
	var err error
	path := metrics.MetricToPath(metric, ".tsj")
	if r.FormValue("from") == "" {
		from = 0
	} else {
		from, err = strconv.ParseInt(r.FormValue("from"), 0, 64)
		if err != nil {
			http.Error(w, "from: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if r.FormValue("until") == "" {
		until = time.Now().Unix()
	} else {
		until, err = strconv.ParseInt(r.FormValue("until"), 0, 64)
		if err != nil {
			http.Error(w, "until: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	if from >= until {
		http.Error(w, "Bad time range request", http.StatusBadRequest)
		return
	}

	j, err := timeseries.Open(path)
	if os.IsNotExist(err) {
		http.Error(w, "File not found.", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error opening journal: %s", err)
		return
	}
	defer j.Close()

	ret, err := metrics.JournalFetch(j, from, until)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error reading journal: %s", err)
		return
	}

	// Marshal the data back as a JSON blob
	blob, err := proto.Marshal(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error marshaling data: %s", err)
	} else {
		w.Header().Set("Content-Type", "application/protobuf")
		w.Write(blob)
	}
}

func postTimeSeries(w http.ResponseWriter, r *http.Request, metric string) {
	// XXX: Need to know about the data partitions we have on disk
	// XXX: Support Whisper DB fallback?
	path := metrics.MetricToPath(metric, ".tsj")

	// Does this request look sane?
	if r.Header.Get("Content-Type") != "application/protobuf" {
		http.Error(w, "Accepted Content-Type: application/prtobuf",
			http.StatusBadRequest)
		log.Printf("postTimeSeries: content-type of %s, abort!",
			r.Header.Get("Content-Type"))
		return
	}

	blob, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error reading body in postTimeSeries: %s", err)
		return
	}
	ts := new(metrics.TimeSeries)
	err = proto.Unmarshal(blob, ts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error unmarshalling json: %s", err)
		return
	}

	for i := 0; i < 20 && i < len(ts.Values); i++ {
		log.Printf("Saw %v", ts.Values[i])
	}

	j, err := timeseries.Open(path)
	if os.IsNotExist(err) {
		j, err = timeseries.Create(path, MetricInterval(metric),
			journal.NewFloat64ValueType(), make([]int64, 0))
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error opening/creating timeseries journal: %s", err)
		return
	}
	defer j.Close()

	err = metrics.JournalUpdate(j, ts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error updating journal: %s", err)
		return
	}
}
