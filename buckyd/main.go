package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

import . "github.com/jjneely/buckytools"

var metricsCache *MetricsCacheType
var tmpDir string
var hashring JSONRingType

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
func parseRing() {
	if flag.NArg() < 2 {
		log.Fatalf("You must have at least 2 nodes in your hash ring.")
	}
	hashring.Nodes = make([]string, 0)
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

		hashring.Nodes = append(hashring.Nodes, n)
	}
}

func main() {
	var bindAddress string
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
	flag.StringVar(&hashring.Name, "node", hostname,
		"This node's name in the Graphite consistent hash ring.")
	flag.StringVar(&hashring.Name, "n", hostname,
		"This node's name in the Graphite consistent hash ring.")
	flag.Parse()
	parseRing()

	http.HandleFunc("/", http.NotFound)
	http.HandleFunc("/metrics", listMetrics)
	http.HandleFunc("/metrics/", serveMetrics)
	http.HandleFunc("/hashring", listHashring)

	log.Printf("Starting server on %s", bindAddress)
	err = http.ListenAndServe(bindAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
