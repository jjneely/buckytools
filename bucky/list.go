package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var listRegexMode bool
var listForce bool
var listLocation bool

// RetryReader is a buffer for HTTP request bodies that is replayable.
// As the request transport will close the body after its sent we
// hook the close to seek back to the begining of the buffer and
// provide http.NewRequest with an io.ReadCloser.
type RetryReader struct {
	*strings.Reader
}

func (r *RetryReader) Close() error {
	r.Seek(0, 0)
	return nil
}

func NewRetryReader(s string) *RetryReader {
	reader := strings.NewReader(s)
	return &RetryReader{reader}
}

func init() {
	usage := "[options] <metric expression>"
	short := "List out matching metrics."
	long := `Dump to STDOUT text or JSON of matching metrics.  Without any
arguments / options this will list every metric in the cluster.

The default mode is to work with lists.  The arguments are a series of
one or more metric key names and the results will contain these key names
if they are present on the cluster/host.  If the first argument is a "-"
then read a JSON array from STDIN as our list of metrics.

Use -r to enable regular expression mode.  The first argument is a
regular expression.  If metrics names match they will be included in the
output.

With -l we list the server that the metric resides on.  This is the
actual location of the metric and not the location computed by the
consistent hash ring.  Combined with -j the JSON output will be a hash.`

	c := NewCommand(listCommand, "list", usage, short, long)
	SetupHostname(c)
	SetupSingle(c)
	SetupJSON(c)

	c.Flag.BoolVar(&listRegexMode, "r", false,
		"Filter by a regular expression.")
	c.Flag.BoolVar(&listForce, "f", false,
		"Force the remote daemons to rebuild their cache.")
	c.Flag.BoolVar(&listLocation, "l", false,
		"List the metric's real relocation.")
}

// getMetricCache accepts an *http.Request type that defines a request to
// retrieve metrics from a remote buckyd daemon.  This can be a GET/POST
// with various query parameters as specified by the REST API notes.  This
// blocks if the remote daemon is building its metric cache (202 code)
// and returns a map of slices of strings after a successful 200 and parsing
// of the JSON data.  Map is server => metrics.
func getMetricCache(r *http.Request) (map[string][]string, error) {
	resp, err := HTTPFetch(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err := fmt.Errorf("Error fetching remote metric cache: %s", resp.Status)
		log.Print(err)
		return nil, err
	}

	host := strings.Split(r.URL.Host, ":")[0]
	metrics := make([]string, 0)
	blob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %s", err)
		return nil, err
	}
	err = json.Unmarshal(blob, &metrics)
	if err != nil {
		log.Printf("Error unmarshalling JSON data: %s", err)
		return nil, err
	}

	log.Printf("%s returned %d metrics", host, len(metrics))
	return map[string][]string{host: metrics}, nil
}

// HTTPFetch attempts to execute an *http.Request with a Fibonacci backoff
// for handling errors and 202 status codes (results not available)
func HTTPFetch(r *http.Request) (*http.Response, error) {
	fib := []time.Duration{0, 1, 1, 2, 3, 5, 8, 13, 21, 34}
	httpClient = GetHTTP()
	resp, err := httpClient.Do(r)
	if err != nil {
		log.Printf("Error communicating with server: %s", r.URL.Host)
		log.Printf("%s", err)
		return nil, err
	}

	i := 0
	for resp.StatusCode == 202 {
		// Sleep and retry until results are available
		log.Printf("Results from %s not available. Sleeping.", r.URL.Host)
		if i == len(fib) {
			time.Sleep(fib[i-1] * time.Second)
		} else {
			time.Sleep(fib[i] * time.Second)
			i++
		}

		resp.Body.Close()
		resp, err = httpClient.Do(r)
		if err != nil {
			log.Printf("Error communicating: %s", err)
			return nil, err
		}
	}

	return resp, err
}

// multiplexListRequests issues the given slice of Requests in parallel
// and merges the results.  The error will indicate an error with
// one or more http request/response that has already been handled.
func multiplexListRequests(r []*http.Request) (map[string][]string, error) {
	var wg sync.WaitGroup
	comms := make(chan map[string][]string, 10)
	wg.Add(len(r))
	errors := false

	for _, v := range r {
		go func(req *http.Request) {
			metrics, err := getMetricCache(req)
			if err == nil {
				// Errors reported by getMetricsCache
				comms <- metrics
			} else {
				errors = true
			}
			wg.Done()
		}(v)
	}
	go func() {
		wg.Wait()
		close(comms)
	}()

	results := make(map[string][]string)
	for hash := range comms {
		for k, v := range hash {
			// We only get one k/v pair for each hash off the channel
			results[k] = v
		}
	}

	if errors {
		return results, fmt.Errorf("Errors occured fetching metric keys.")
	}
	return results, nil
}

// ListAllMetrics interates through the host:port strings given in servers
// contact those buckyd daemons, gets the list of all known metrics on that
// server, returns a map of server => list of metrics
func ListAllMetrics(servers []string, force bool) (map[string][]string, error) {
	requests := make([]*http.Request, 0)

	for _, buckyd := range servers {
		u := url.URL{
			Scheme: "http",
			Host:   buckyd,
			Path:   "/metrics",
		}
		if force {
			query := url.Values{}
			query.Set("force", "true")
			u.RawQuery = query.Encode()
		}
		r, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			log.Printf("Building request object for %s failed.", buckyd)
			return nil, err
		}
		requests = append(requests, r)
	}

	return multiplexListRequests(requests)
}

// ListRegexMetrics queries buckyd daemons specified in servers for all
// metrics matching the given regex.  If successful matching metrics from
// all servers returned in a map of server => slice of metrics
func ListRegexMetrics(servers []string, regex string, force bool) (map[string][]string, error) {
	requests := make([]*http.Request, 0)

	for _, buckyd := range servers {
		u := fmt.Sprintf("http://%s/metrics", buckyd)
		form := url.Values{}
		if force {
			form.Set("force", "true")
		}
		form.Set("regex", regex)
		r, err := http.NewRequest("GET", u, nil)
		r.URL.RawQuery = form.Encode()
		if err != nil {
			log.Printf("Building request object for %s failed.", buckyd)
			return nil, err
		}
		requests = append(requests, r)
	}

	return multiplexListRequests(requests)
}

// ListSliceMetrics queries buckyd daemons specified in servers for all
// metrics that are known by that buckyd daemon and listed in the slice
// metrics.  Results from all servers are returned in a map of server =>
// slice of metrics.
func ListSliceMetrics(servers []string, metrics []string, force bool) (map[string][]string, error) {
	requests := make([]*http.Request, 0)

	for _, buckyd := range servers {
		u := fmt.Sprintf("http://%s/metrics", buckyd)
		data := url.Values{}
		if force {
			data.Set("force", "true")
		}
		blob, err := json.Marshal(metrics)
		if err != nil {
			log.Printf("Error marshalling JSON data: %s", err)
			return nil, err
		}
		data.Set("list", string(blob))
		r, err := http.NewRequest("POST", u, NewRetryReader(data.Encode()))
		if err != nil {
			log.Printf("Building request object for %s failed.", buckyd)
			return nil, err
		}
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		requests = append(requests, r)
	}

	return multiplexListRequests(requests)
}

// ListJSONMetrics queries buckyd daemons specified in servers for all
// metrics known to that buckyd daemon and that are present in the
// io.Reader interface which points to a data source containing a JSON
// array.  Results are returned in a map of server => metrics.
func ListJSONMetrics(servers []string, fd io.Reader, force bool) (map[string][]string, error) {
	// Read the JSON from the file-like object
	blob, err := ioutil.ReadAll(fd)
	metrics := make([]string, 0)

	err = json.Unmarshal(blob, &metrics)
	// We could just package this up and query the server, but lets check the
	// JSON is valid first.
	if err != nil {
		log.Printf("Error unmarshalling JSON data: %s", err)
		return nil, err
	}

	return ListSliceMetrics(servers, metrics, force)
}

// listCommand runs this subcommand.
func listCommand(c Command) int {
	servers := GetAllBuckyd()
	if servers == nil {
		return 1
	}

	var list map[string][]string
	var err error
	if c.Flag.NArg() == 0 {
		list, err = ListAllMetrics(servers, listForce)
	} else if listRegexMode && c.Flag.NArg() > 0 {
		list, err = ListRegexMetrics(servers, c.Flag.Arg(0), listForce)
	} else if c.Flag.Arg(0) != "-" {
		list, err = ListSliceMetrics(servers, c.Flag.Args(), listForce)
	} else {
		list, err = ListJSONMetrics(servers, os.Stdin, listForce)
	}

	results := make([]string, 0)
	if listLocation {
		for _, v := range list {
			sort.Strings(v)
		}
	} else {
		// Merge and sort the results
		for _, v := range list {
			for _, m := range v {
				results = append(results, m)
			}
		}
		sort.Strings(results)
	}

	if JSONOutput {
		var blob []byte
		if listLocation {
			blob, err = json.MarshalIndent(list, "", "\t")
		} else {
			blob, err = json.MarshalIndent(results, "", "\t")
		}
		if err != nil {
			log.Printf("%s", err)
		} else {
			os.Stdout.Write(blob)
			os.Stdout.Write([]byte("\n"))
		}
	} else {
		if listLocation {
			for server, metrics := range list {
				for _, m := range metrics {
					fmt.Printf("%s: %s\n", server, m)
				}
			}
		} else {
			for _, v := range results {
				fmt.Printf("%s\n", v)
			}
		}
	}

	if err != nil {
		return 1
	}
	return 0
}
