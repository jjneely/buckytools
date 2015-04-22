package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

// listMetrics retrieves a list of metrics on the localhost and sends
// it to the client.
func listMetrics(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	// Check our methods.  We handle GET/POST.
	if r.Method != "GET" && r.Method != "POST" {
		http.Error(w, "Bad request method.", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "GET/POST parameter parsing error.", http.StatusBadRequest)
		return
	}

	// Do we need to init the metricsCache?
	if metricsCache == nil {
		metricsCache = NewMetricsCache()
	}

	// Handle case when we are currently building the cache
	if r.Form.Get("force") != "" && metricsCache.IsAvailable() {
		metricsCache.RefreshCache()
	}
	metrics, ok := metricsCache.GetMetrics()
	if !ok {
		http.Error(w, "Cache update in progress.", http.StatusAccepted)
		return
	}

	// Options
	if r.Form.Get("regex") != "" {
		m, err := FilterRegex(r.Form.Get("regex"), metrics)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		metrics = m
	}
	if r.Form.Get("list") != "" {
		filter, err := unmarshalList(r.Form.Get("list"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		metrics = FilterList(filter, metrics)
	}

	// Marshal the data back as a JSON list
	blob, err := json.Marshal(metrics)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error marshaling data: %s", err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(blob)
	}
}

func serveMetrics(w http.ResponseWriter, r *http.Request) {
	logRequest(r)

	metric := r.URL.Path[len("/metrics/"):]
	path := MetricToPath(metric)
	if len(metric) == 0 {
		http.Error(w, "Metric name missing.", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		// XXX: This is a little HTML-specific but I think it
		// will work
		http.ServeFile(w, r, path)
	case "DELETE":
		// XXX: Auth?  What's our safety switch?
		err := os.Remove(path)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "Metric not found.", http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	case "PUT":
	case "POST":
	default:
		http.Error(w, "Bad method request.", http.StatusBadRequest)
	}
}
