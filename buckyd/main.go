package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
)

import . "github.com/jjneely/buckytools"
import "github.com/jjneely/buckytools/metrics"

var metricsCache *metrics.MetricsCacheType
var tmpDir string
var hashring *JSONRingType

func usage() {
	t := []string{
		"%s [options] <graphite-node1> <graphite-node2> ...\n",
		"Version: %s\n\n",
		"\tA daemon that provides a REST API for remotely uploading,\n",
		"\tdownloading, deleting, backfilling and getting metadata\n",
		"\tfor the Graphite Whisper DBs in the local machine's data\n",
		"\tstore.  The non-optional arguments are the members of your\n",
		"\tconsistent hashring as found in your carbon-relay configuration\n.",
		"\tAll of the daemons in your cluster need to be able to build\n",
		"\tthe same hashring.  Specifying the port number between the\n",
		"\tserver and instance is optional.\n\n",
	}

	fmt.Printf(strings.Join(t, ""), os.Args[0], Version)
	flag.PrintDefaults()
}

func logRequest(r *http.Request) {
	log.Printf("%s - - %s %s", r.RemoteAddr, r.Method, r.RequestURI)
}

func unmarshalList(encoded string) ([]string, error) {
	data := make([]string, 0)
	err := json.Unmarshal([]byte(encoded), &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// parseRing builds a representation of the hashring from the command
// line arguments
func parseRing(hostname, algo string, replicas int) *JSONRingType {
	if flag.NArg() < 1 {
		log.Fatalf("You must have at least 1 node in your hash ring")
	}
	ring := new(JSONRingType)
	ring.Nodes = make([]string, 0)
	ring.Name = hostname
	ring.Algo = algo
	ring.Replicas = replicas
	for i := 0; i < flag.NArg(); i++ {
		var n string
		switch strings.Count(flag.Arg(i), ":") {
		case 0:
			// server name only
			n = flag.Arg(i)
		case 1:
			// server:instance
			fields := strings.Split(flag.Arg(i), ":")
			n = fmt.Sprintf("%s:%s", fields[0], fields[1])
		case 2:
			// server:port:instance -- port is not considered
			fields := strings.Split(flag.Arg(i), ":")
			n = fmt.Sprintf("%s:%s", fields[0], fields[2])
		default:
			log.Fatalf("Error parsing hashring members from cli.")
		}

		ring.Nodes = append(ring.Nodes, n)
	}

	return ring
}

func main() {
	var replicas int
	var hashType string
	var bindAddress string
	var graphiteBind string
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "UNKNOWN"
	}

	flag.Usage = usage
	flag.StringVar(&tmpDir, "tmpdir", os.TempDir(),
		"Temporary file location.")
	flag.StringVar(&tmpDir, "t", os.TempDir(),
		"Temporary file location.")
	flag.StringVar(&bindAddress, "bind", "0.0.0.0:4242",
		"IP:PORT to listen for HTTP requests.")
	flag.StringVar(&bindAddress, "b", "0.0.0.0:4242",
		"IP:PORT to listen for HTTP requests.")
	flag.StringVar(&hostname, "node", hostname,
		"This node's name in the Graphite consistent hash ring.")
	flag.StringVar(&hostname, "n", hostname,
		"This node's name in the Graphite consistent hash ring.")
	flag.StringVar(&hashType, "hash", "carbon",
		fmt.Sprintf("Consistent Hash algorithm to use: %v", SupportedHashTypes))
	flag.IntVar(&replicas, "replicas", 1,
		"Number of copies of each metric in the cluster.")
	flag.StringVar(&graphiteBind, "graphite", "",
		"Run a Graphite line protocol server at the given bind address:port")
	flag.Parse()

	i := sort.SearchStrings(SupportedHashTypes, hashType)
	if i == len(SupportedHashTypes) || SupportedHashTypes[i] != hashType {
		log.Fatalf("Invalide hash type.  Supported types: %v",
			SupportedHashTypes)
	}
	hashring = parseRing(hostname, hashType, replicas)

	http.HandleFunc("/", http.NotFound)
	http.HandleFunc("/metrics", listMetrics)
	http.HandleFunc("/metrics/", serveMetrics)
	http.HandleFunc("/hashring", listHashring)
	http.HandleFunc("/timeseries/", serveTimeSeries)

	if graphiteBind != "" {
		log.Printf("Starting Graphite server on %s", graphiteBind)
		go runCarbonServer(graphiteBind)
	}
	log.Printf("Starting server on %s", bindAddress)
	err = http.ListenAndServe(bindAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
