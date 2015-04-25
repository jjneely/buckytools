package main

import (
	"encoding/json"
	"fmt"
	"io"
	//"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
)

// import . "github.com/jjneely/buckytools"

var listRegexMode bool

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
}

// getMetricCache accepts an *http.Request type that defines a request to
// retrieve metrics from a remote buckyd daemon.  This can be a GET/POST
// with various query parameters as specified by the REST API notes.  This
// blocks if the remote daemon is building its metric cache (202 code)
// and returns a slice of strings after a successful 200 and parsing
// of the JSON data.
func getMetricCache(r *http.Request) ([]string, error) {
	return nil, nil
}

// multiplexRequests issues the given slice of Requests in parallel
// and merges the results.  The error will indicate an error with
// one or more http request/response that has already been handled.
func multiplexRequests(r []*http.Request) ([]string, error) {
	var wg sync.WaitGroup
	comms := make(chan []string, 10)
	wg.Add(len(r))
	errors := false

	for _, v := range r {
		go func() {
			metrics, err := getMetricCache(v)
			if err == nil {
				// Errors reported by getMetricsCache
				comms <- metrics
			} else {
				errors = true
			}
			wg.Done()
		}()
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
func ListAllMetrics(servers []string) ([]string, error) {
	return []string{"foo", "bar"}, nil
}

// ListRegexMetrics queries buckyd daemons specified in servers for all
// metrics matching the given regex.  If successful matching metrics from
// all servers are merged into one slice of strings and returned.
func ListRegexMetrics(servers []string, regex string) ([]string, error) {
	return ListAllMetrics(servers)
}

// ListSliceMetrics queries buckyd daemons specified in servers for all
// metrics that are known by that buckyd daemon and listed in the slice
// metrics.  Results from all servers are merged and returned.
func ListSliceMetrics(servers []string, metrics []string) ([]string, error) {
	return ListAllMetrics(servers)
}

// ListJSONMetrics queries buckyd daemons specified in servers for all
// metrics known to that buckyd daemon and that are present in the
// io.Reader interface which points to a data source containing a JSON
// array.  Results are merged and returned.
func ListJSONMetrics(servers []string, fd io.Reader) ([]string, error) {
	return ListAllMetrics(servers)
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
		list, err = ListAllMetrics(servers)
	} else if listRegexMode && c.Flag.NArg() > 0 {
		list, err = ListRegexMetrics(servers, c.Flag.Arg(0))
	} else if c.Flag.Arg(0) != "-" {
		list, err = ListSliceMetrics(servers, c.Flag.Args())
	} else {
		list, err = ListJSONMetrics(servers, os.Stdin)
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
