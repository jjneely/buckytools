package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

import . "github.com/jjneely/buckytools"
import . "github.com/jjneely/buckytools/metrics"
import "github.com/jjneely/buckytools/fill"

// listMetrics retrieves a list of metrics on the localhost and sends
// it to the client.
func listMetrics(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	// Check our methods.  We handle GET/POST.
	if r.Method != "GET" && r.Method != "POST" {
		http.Error(w, "Bad request method.", http.StatusBadRequest)
		return
	}

	// Do we need to init the metricsCache?
	if metricsCache == nil {
		metricsCache = NewMetricsCache()
	}

	// XXX: Calling r.FormValue will set a safety limit on the size of
	// the body of 10MiB which may be small for the amount of JSON data
	// included in a list command.  Set the limit higher here.  How
	// can we do this better?  This is 160MiB.
	r.Body = http.MaxBytesReader(w, r.Body, 10<<24)

	// Handle case when we are currently building the cache
	if r.FormValue("force") != "" && metricsCache.IsAvailable() {
		go metricsCache.RefreshCache()
	}
	metrics, ok := metricsCache.GetMetrics()
	if !ok {
		http.Error(w, "Cache update in progress.", http.StatusAccepted)
		return
	}

	// Options
	if r.FormValue("regex") != "" {
		m, err := FilterRegex(r.FormValue("regex"), metrics)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		metrics = m
	}
	if r.FormValue("list") != "" {
		filter, err := unmarshalList(r.FormValue("list"))
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
	case "HEAD":
		_, err := statMetric(w, metric, path)
		w.Header().Set("Content-Length", "0")
		status := http.StatusOK
		if err != nil {
			// XXX: Type switch and better error reporting
			status = http.StatusNotFound
		}
		w.WriteHeader(status)
		// HEAD seems to behave a bit differently, forcing the headers
		// seems to get the connection closed after the request.
	case "GET":
		serveMetric(w, r, path, metric)
	case "DELETE":
		// XXX: Auth?  Holodeck safeties are off!
		deleteMetric(w, path, true)
	case "PUT":
		// Replace metric data on disk
		// XXX: Metric will still be deleted if an error in heal occurrs
		err := deleteMetric(w, path, false)
		if err == nil {
			healMetric(w, r, path)
		}
	case "POST":
		// Backfill
		healMetric(w, r, path)
	default:
		http.Error(w, "Bad method request.", http.StatusBadRequest)
	}
}

// statMetric stat()s the given metric file system path and add the
// X-Metric-Stat header to the response as JSON encoded data.  It returns
// the modTime on the file and an error code.
func statMetric(w http.ResponseWriter, metric, path string) (time.Time, error) {
	s, err := os.Stat(path)
	if err != nil {
		return s.ModTime(), err
	}

	stat := new(MetricStatType)
	stat.Name = metric
	stat.Size = s.Size()
	stat.Mode = uint32(s.Mode())
	stat.ModTime = s.ModTime().Unix()

	// We should be able to marshal this struct without the funcs
	blob, err := json.Marshal(stat)
	if err != nil {
		return s.ModTime(), err
	}

	w.Header().Set("X-Metric-Stat", string(blob))
	return s.ModTime(), nil
}

// deleteMetric removes a metric DB from the file system and handles
// reporting any associated errors back to the client.  Set fatal to true
// to treat file not found as an error rather than success.
func deleteMetric(w http.ResponseWriter, path string, fatal bool) error {
	err := os.Remove(path)
	if err != nil {
		if os.IsNotExist(err) && fatal {
			http.Error(w, "Metric not found.", http.StatusNotFound)
			return err
		} else if !os.IsNotExist(err) {
			log.Printf("Error deleting metric %s: %s", path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}
	}
	return nil
}

// healMetric will use the Whisper DB in the body of the request to
// backfill the metric found at the given filesystem path.  If the metric
// doesn't exist it will be created as an identical copy of the DB found
// in the request.
func healMetric(w http.ResponseWriter, r *http.Request, path string) {
	// Does this request look sane?
	if r.Header.Get("Content-Type") != "application/octet-stream" {
		http.Error(w, "Content-Type must be application/octet-stream.",
			http.StatusBadRequest)
		log.Printf("Got send a content-type of %s, abort!", r.Header.Get("Content-Type"))
		return
	}
	i, err := strconv.Atoi(r.Header.Get("Content-Length"))
	if err != nil || i <= 28 {
		// Whisper file headers are 28 bytes and we need data too.
		// Something is wrong here
		log.Printf("Whisper data in request too small: %d bytes", i)
		http.Error(w, "Whisper data in request too small.", http.StatusBadRequest)
	}

	// Does the destination path on dist exist?
	dstExists := true
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Error stat'ing file %s: %s", path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		err := os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			log.Printf("Error creating %s: %s", filepath.Dir(path), err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		dstExists = false
	}

	// Write request body to a tmpfile
	fd, err := ioutil.TempFile(tmpDir, "buckyd")
	if err != nil {
		log.Printf("Error creating temp file: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = io.Copy(fd, r.Body)
	if err != nil {
		log.Printf("Error writing to temp file: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		fd.Close()
		os.Remove(fd.Name())
		return
	}
	srcName := fd.Name()
	fd.Sync()
	fd.Close()
	defer os.Remove(srcName) // not concerned with errors here

	// XXX: How can we check the tmpfile for sanity?
	if dstExists {
		err := fill.All(srcName, path)
		if err != nil {
			log.Printf("Error backfilling %s => %s: %s", srcName, path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		src, err := os.Open(srcName)
		if err != nil {
			log.Printf("Error opening tmp file %s: %s", srcName, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer src.Close()
		if err = syscall.Flock(int(src.Fd()), syscall.LOCK_EX); err != nil {
			log.Printf("Error locking file %s: %s", srcName, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		dst, err := os.Create(path)
		if err != nil {
			log.Printf("Error opening metric file %s: %s", path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer dst.Close()
		if err = syscall.Flock(int(dst.Fd()), syscall.LOCK_EX); err != nil {
			log.Printf("Error locking file %s: %s", path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = io.Copy(dst, src)
		if err != nil {
			log.Printf("Error copying %s => %s: %s", srcName, path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

}

// serveMetric will serve a GET request for the metric that path
// refers to.  Effort is made to serve file data that is pristine and
// not in the middle of an update by carbon-cache.  The parameter metric is
// the dotted notation of the metric name.
func serveMetric(w http.ResponseWriter, r *http.Request, path, metric string) {
	modTime, err := statMetric(w, metric, path)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Metric not found.", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	fd, err := os.Open(path)
	if err != nil {
		// I know the file exists
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer fd.Close()
	if err = syscall.Flock(int(fd.Fd()), syscall.LOCK_EX); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.ServeContent(w, r, path, modTime, fd)
}
