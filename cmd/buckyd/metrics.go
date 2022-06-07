package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-graphite/buckytools/fill"
	. "github.com/go-graphite/buckytools/metrics"
	whisper "github.com/go-graphite/go-whisper"
	"github.com/golang/snappy"
)

const buckydAuthHeader = "X-Buckyd-Authorization"

// listMetrics retrieves a list of metrics on the localhost and sends
// it to the client.
func listMetrics(w http.ResponseWriter, r *http.Request) {
	logRequest(r)

	if err := isTokenValid("*", r.Header.Get(buckydAuthHeader), ACLReadMetrics); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

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
	if r.ContentLength >= 10<<24 {
		// Query is too big, give the user an error
		http.Error(w, "Query larger than 160MiB", http.StatusBadRequest)
		return
	}
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
		if err := isTokenValid(metric, r.Header.Get(buckydAuthHeader), ACLReadMetrics); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		stat, err := statMetric(metric, path)
		w.Header().Set("Content-Length", "0")
		status := http.StatusOK
		if err != nil {
			// XXX: Type switch and better error reporting
			status = http.StatusNotFound
		}
		err = setStatHeader(w, stat)
		if err != nil {
			log.Printf("serveMetric HEAD: %s", err)
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)
		// HEAD seems to behave a bit differently, forcing the headers
		// seems to get the connection closed after the request.
	case "GET":
		if err := isTokenValid(metric, r.Header.Get(buckydAuthHeader), ACLReadMetrics); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		serveMetric(w, r, path, metric)
	case "DELETE":
		if err := isTokenValid(metric, r.Header.Get(buckydAuthHeader), ACLDeleteMetrics); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		deleteMetric(w, path, true)
	case "PUT":
		// Replace metric data on disk
		// XXX: Metric will still be deleted if an error in heal occurs

		if err := isTokenValid(metric, r.Header.Get(buckydAuthHeader), ACLReplaceMetrics); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		err := deleteMetric(w, path, false)
		if err == nil {
			healMetric(w, r, path)
		}
	case "POST":
		// Backfill
		if err := isTokenValid(metric, r.Header.Get(buckydAuthHeader), ACLUpdateMetrics); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		healMetric(w, r, path)
	default:
		http.Error(w, "Bad method request.", http.StatusBadRequest)
	}
}

// statMetric stat()s the given metric file system path and builds a
// *MetricData struct representing this metric.  Data is not attached
// and the Encoding is intentionally left as the zero value.
func statMetric(metric, path string) (*MetricData, error) {
	s, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	stat := new(MetricData)
	stat.Name = metric
	stat.Size = s.Size()
	stat.Mode = int64(s.Mode())
	stat.ModTime = s.ModTime().Unix()

	return stat, nil
}

// setStatHeader takes a ResponseWriter and a *MetricData and adds the
// X-Metric-Stat header to the ResponseWriter.  It should be used before
// the body is written.
func setStatHeader(w http.ResponseWriter, metric *MetricData) error {
	blob, err := json.Marshal(metric)
	if err != nil {
		return err
	}

	w.Header().Set("X-Metric-Stat", string(blob))
	return nil
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
	// removing an empty dirs left after a metric removal
	// it will keep iterating thru a dirs till it get an error or reach the Prefix
	// Normally the error should be "directory not empty"
	for path = filepath.Dir(path); path != Prefix; path = filepath.Dir(path) {
		err := os.Remove(path)
		if err != nil {
			break
		}
	}

	return nil
}

// healMetric will use the Whisper DB in the body of the request to
// backfill the metric found at the given filesystem path.  If the metric
// doesn't exist it will be created as an identical copy of the DB found
// in the request.
func healMetric(w http.ResponseWriter, r *http.Request, path string) {
	var err error
	var data io.Reader
	var stat *MetricData
	var stats struct {
		Download time.Duration
		Dump     time.Duration
		Fill     time.Duration
		Compress time.Duration
		Copy     time.Duration
	}
	var hasError bool
	httpError := func(w http.ResponseWriter, error string, code int) {
		hasError = true
		http.Error(w, error, code)
	}
	defer func() {
		if hasError {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			log.Printf("Failed to rerturn stats: %s", err)
		}
	}()

	if r.URL.Query().Get("fetch_offload") == "true" {
		start := time.Now()

		stat, err = getMetricData(r.FormValue("no_encoding") == "true", r.FormValue("server"), r.FormValue("metric"))
		if err != nil {
			if errors.Is(err, errNotFound) {
				httpError(w, "Error fetching metric: "+err.Error(), http.StatusNotFound)
				return
			}

			httpError(w, "Error fetching metric: "+err.Error(), http.StatusBadRequest)
			return
		}
		data = bytes.NewReader(stat.Data)
		if stat.Encoding == EncSnappy {
			data = snappy.NewReader(data)
		}

		stats.Download = time.Since(start)
	} else {
		// Does this request look sane?
		if r.Header.Get("Content-Type") != "application/octet-stream" {
			httpError(w, "Content-Type must be application/octet-stream.",
				http.StatusBadRequest)
			log.Printf("Got send a content-type of %s, abort!", r.Header.Get("Content-Type"))
			return
		}
		if r.Header.Get("content-encoding") == "snappy" {
			data = snappy.NewReader(r.Body)
		} else {
			data = r.Body
		}

		stat = new(MetricData)
		err = json.Unmarshal([]byte(r.Header.Get("X-Metric-Stat")), &stat)
		if err != nil {
			log.Printf("Error decoding X-Metric-Stat header: %s", err)
			httpError(w, "Error decoding X-Metric-Stat header", http.StatusBadRequest)
			return
		}
	}

	if stat.Size <= 28 {
		// Whisper file headers are 28 bytes and we need data too.
		log.Printf("Whisper data in request too small: %d bytes", stat.Size)
		httpError(w, "Whisper data in request too small.", http.StatusBadRequest)
		return
	}

	// Does the destination path on dist exist?
	dstExists := true
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Error stat'ing file %s: %s", path, err)
			httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		err := os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			log.Printf("Error creating %s: %s", filepath.Dir(path), err)
			httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		dstExists = false
	}

	defer func() {
		// fix dst file mtime if needed
		if obeyRemoteMtime {
			// Set both access time and modified time of the file to the metric ModTime
			// we should preserve atime but in most of time that's doesn't matter or it's even ignored
			mtime := time.Unix(stat.ModTime, 0)
			err := os.Chtimes(path, mtime, mtime)
			if err != nil {
				log.Printf("Error chtimes on whisper file %s: %s", path, err)
				httpError(w, "Error writing whisper data", http.StatusInternalServerError)
			}
		}
	}()
	if dstExists {
		// Write request body to a tmpfile
		tmpStart := time.Now()
		fd, err := ioutil.TempFile(tmpDir, "buckyd")
		if err != nil {
			log.Printf("Error creating temp file: %s", err)
			httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		nr, err := io.Copy(fd, data)
		srcName := fd.Name()
		fd.Close()
		stats.Dump = time.Since(tmpStart)

		fillStart := time.Now()
		defer os.Remove(srcName) // not concerned with errors here
		if err != nil || nr != stat.Size {
			if err != nil {
				log.Printf("Error writing to temp file: %s", err)
				httpError(w, err.Error(), http.StatusInternalServerError)
			} else {
				log.Printf("Decoded whisper file data does not match size %d != %d",
					nr, stat.Size)
				httpError(w, "Malformed whisper data", http.StatusBadRequest)
			}
			return
		}

		// XXX: How can we check the tmpfile for sanity?
		err = fill.All(srcName, path)
		if err != nil {
			log.Printf("Error backfilling %s => %s: %s", srcName, path, err)
			httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stats.Fill = time.Since(fillStart)
	} else {
		if compressed {
			bdata := bufio.NewReader(data)
			data = bdata
			magic := []byte("whisper_compressed")
			magicFromData, err := bdata.Peek(len(magic))
			if err != nil {
				log.Printf("Failed to read compressed magic string: %s", err)
				httpError(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// src file is not compressed, dump it and compress it
			if !bytes.Equal(magicFromData, magic) {
				tmpStart := time.Now()
				fd, err := ioutil.TempFile(tmpDir, "buckyd")
				if err != nil {
					log.Printf("Error creating temp file: %s", err)
					httpError(w, err.Error(), http.StatusInternalServerError)
					return
				}
				nr, err := io.Copy(fd, data)
				srcName := fd.Name()
				fd.Close()
				stats.Dump = time.Since(tmpStart)

				defer os.Remove(srcName) // not concerned with errors here
				if err != nil || nr != stat.Size {
					if err != nil {
						log.Printf("Error writing to temp file: %s", err)
						httpError(w, err.Error(), http.StatusInternalServerError)
					} else {
						log.Printf("Decoded whisper file data does not match size %d != %d",
							nr, stat.Size)
						httpError(w, "Malformed whisper data", http.StatusBadRequest)
					}
					return
				}

				compressStart := time.Now()
				srcw, err := whisper.Open(srcName)
				if err != nil {
					log.Printf("Failed to open source whisper file: %s", err)
					httpError(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer srcw.Close()

				if err := srcw.CompressTo(path); err != nil {
					log.Printf("Failed to compress to whisper file %s: %s", path, err)
					httpError(w, err.Error(), http.StatusInternalServerError)
					return
				}
				stats.Compress = time.Since(compressStart)

				return
			}

			// src file is compressed, continue like standard whisper file, just copy it
		}

		copyStart := time.Now()
		// Open and lock destination
		dst, err := os.Create(path)
		if err != nil {
			log.Printf("Error opening metric file %s: %s", path, err)
			httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer dst.Close()
		if err = syscall.Flock(int(dst.Fd()), syscall.LOCK_EX); err != nil {
			log.Printf("Error locking file %s: %s", path, err)
			httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var nr int64
		if sparseFiles && !compressed {
			nr, err = copySparse(dst, data)
		} else {
			nr, err = io.Copy(dst, data)
		}
		if err != nil || nr != stat.Size {
			if err != nil {
				log.Printf("Error writing whisper data to %s: %s", path, err)
				httpError(w, "Error writing whisper data", http.StatusInternalServerError)
			} else {
				log.Printf("Decoded whisper file data does not match size %d != %d",
					nr, stat.Size)
				httpError(w, "Malformed whisper data", http.StatusBadRequest)
			}
			defer os.Remove(dst.Name()) // not concerned with errors here
			return
		}
		stats.Copy = time.Since(copyStart)
	}
}

var httpClient = &http.Client{
	Timeout: 600 * time.Second,
	Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 1 * time.Second,
		}).Dial,
		IdleConnTimeout:       10 * time.Minute,
		MaxIdleConnsPerHost:   300,
		ResponseHeaderTimeout: time.Minute,
	},
}

var errNotFound = errors.New("metric not found")

func getMetricData(noEncoding bool, server, name string) (*MetricData, error) {
	var err error
	u := &url.URL{
		Scheme: "http",
		Host:   server,
		Path:   "/metrics/" + name,
	}
	r, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return nil, err
	}
	if !noEncoding {
		r.Header.Set("accept-encoding", "snappy")
	}
	injectAuthHeaderIfEnabled(r)

	resp, err := httpClient.Do(r)
	if err != nil {
		log.Printf("Error downloading metric data: %s", err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Error: Fetching [%s]:%s returned status code: %d  Body: %s",
			server, name, resp.StatusCode, string(body))

		if resp.StatusCode == 404 {
			return nil, errNotFound
		}

		return nil, fmt.Errorf("Fetching metric returned status code: %s", resp.Status)
	}

	data := new(MetricData)
	err = json.Unmarshal([]byte(resp.Header.Get("X-Metric-Stat")), &data)
	if err != nil {
		log.Printf("Error unmarshalling X-Metric-Stat header for [%s]:%s: %s", server, name, err)
		return nil, err
	}

	data.Data, err = ioutil.ReadAll(resp.Body)
	encoding := resp.Header.Get("Content-Encoding")
	switch encoding {
	case "snappy":
		data.Encoding = EncSnappy
	default:
		data.Encoding = EncIdentity
	}
	if err != nil {
		log.Printf("Error reading response body: %s", err)
		return nil, err
	}

	return data, nil
}

// serveMetric will serve a GET request for the metric that path
// refers to.  Effort is made to serve file data that is pristine and
// not in the middle of an update by carbon-cache.  The parameter metric is
// the dotted notation of the metric name.
func serveMetric(w http.ResponseWriter, r *http.Request, path, metric string) {
	var content io.ReadSeeker
	stat, err := statMetric(metric, path)
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

	if r.Header.Get("accept-encoding") == "snappy" {
		blob, err := copySnappy(fd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if blob.Len() == 0 {
			msg := "Compressed length is 0, no data to transfer"
			log.Printf("Error: %s", msg)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}
		stat.Encoding = EncSnappy
		w.Header().Set("content-encoding", "snappy")
		content = bytes.NewReader(blob.Bytes())
	} else {
		content = fd
	}

	err = setStatHeader(w, stat)
	if err != nil {
		log.Printf("Error: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, path, time.Unix(stat.ModTime, 0), content)
}

func injectAuthHeaderIfEnabled(r *http.Request) {
	if authJWTRootAPIToken == "" {
		// API token not enabled, do nothing
		return
	}

	r.Header.Add(buckydAuthHeader, authJWTRootAPIToken)
}
