package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"
)

// import . "github.com/jjneely/buckytools"

var listRegexMode bool
var listForce bool

func init() {
	usage := "[options]"
	short := "List out matching metrics."
	long := `Dump to STDOUT text or JSON of matching metrics.  Without any
arguments / options this will list every metric in the cluster.

The default mode is to work with lists.  The arguments are a series of
one or more metric key names and the results will contain these key names
if they are present on the cluster/host.  If the first argument is a "-"
then read a JSON array from STDIN as our list of metrics.

Use -r to enable regular expression mode.  The first argument is a
regular expression.  If metrics names match they will be included in the
output.`

	c := NewCommand(listCommand, "list", usage, short, long)
	SetupHostname(c)
	SetupSingle(c)
	SetupJSON(c)

	c.Flag.BoolVar(&listRegexMode, "r", false,
		"Filter by a regular expression.")
	c.Flag.BoolVar(&listRegexMode, "f", false,
		"Force the remote daemons to rebuild their cache.")
}

// getMetricCache accepts an *http.Request type that defines a request to
// retrieve metrics from a remote buckyd daemon.  This can be a GET/POST
// with various query parameters as specified by the REST API notes.  This
// blocks if the remote daemon is building its metric cache (202 code)
// and returns a slice of strings after a successful 200 and parsing
// of the JSON data.
func getMetricCache(r *http.Request) ([]string, error) {
	resp, err := HTTPFetch(r)
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err := fmt.Errorf("Error fetching remote metric cache: %s", resp.Status)
		log.Print(err)
		return nil, err
	}

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

	log.Printf("%s returned %d metrics", r.URL.Host, len(metrics))
	return metrics, nil
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
			continue // We heard from the client recently, try again
		}
	}

	return resp, err
}

// multiplexListRequests issues the given slice of Requests in parallel
// and merges the results.  The error will indicate an error with
// one or more http request/response that has already been handled.
func multiplexListRequests(r []*http.Request) ([]string, error) {
	var wg sync.WaitGroup
	comms := make(chan []string, 10)
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

	results := make([]string, 0)
	for i := range comms {
		for _, v := range i {
			// consume the slice of strings
			results = append(results, v)
		}
	}

	sort.Strings(results)
	if errors {
		return results, fmt.Errorf("Errors occured fetching metric keys.")
	}
	return results, nil
}

// ListAllMetrics interates through the host:port strings given in servers
// contact those buckyd daemons, gets the list of all known metrics on that
// server, merges them into one slice of strings that is returned.
func ListAllMetrics(servers []string, force bool) ([]string, error) {
	requests := make([]*http.Request, 0)

	for _, buckyd := range servers {
		url := fmt.Sprintf("http://%s/metrics", buckyd)
		r, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Building request object for %s failed.", buckyd)
			return nil, err
		}
		if force {
			r.Form.Set("force", "true")
		}
		log.Printf("Built Request for %s", buckyd)
		requests = append(requests, r)
	}

	return multiplexListRequests(requests)
}

// ListRegexMetrics queries buckyd daemons specified in servers for all
// metrics matching the given regex.  If successful matching metrics from
// all servers are merged into one slice of strings and returned.
func ListRegexMetrics(servers []string, regex string, force bool) ([]string, error) {
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
// metrics.  Results from all servers are merged and returned.
func ListSliceMetrics(servers []string, metrics []string, force bool) ([]string, error) {
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
		r, err := http.NewRequest("POST", u, bytes.NewBufferString(data.Encode()))
		if err != nil {
			log.Printf("Building request object for %s failed.", buckyd)
			return nil, err
		}
		requests = append(requests, r)
	}

	return multiplexListRequests(requests)
}

// ListJSONMetrics queries buckyd daemons specified in servers for all
// metrics known to that buckyd daemon and that are present in the
// io.Reader interface which points to a data source containing a JSON
// array.  Results are merged and returned.
func ListJSONMetrics(servers []string, fd io.Reader, force bool) ([]string, error) {
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

	var list []string
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

	if JSONOutput {
		blob, err := json.Marshal(list)
		if err != nil {
			log.Printf("%s", err)
		} else {
			os.Stdout.Write(blob)
			os.Stdout.Write([]byte("\n"))
		}
	} else {
		for _, v := range list {
			fmt.Printf("%s\n", v)
		}
	}

	if err != nil {
		return 1
	}
	return 0
}
