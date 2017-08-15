package main

import (
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
import "github.com/jjneely/buckytools/hashing"

var metricsCache *metrics.MetricsCacheType
var tmpDir string
var hashring *hashing.JSONRingType

// sparseFiles defines if we create and manage sparse files.
var sparseFiles bool

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
		"\tthe same hashring.  You may specify nodes in the following\n",
		"\tformat: HOST[:PORT][=INSTANCE]\n\n",
	}

	fmt.Printf(strings.Join(t, ""), os.Args[0], Version)
	flag.PrintDefaults()
}

// parseRing builds a representation of the hashring from the command
// line arguments
func parseRing(hostname, algo string, replicas int) *hashing.JSONRingType {
	if flag.NArg() < 1 {
		log.Printf("You must have at least 1 node in your hash ring")
		usage()
		os.Exit(1)
	}
	ring := new(hashing.JSONRingType)
	ring.Name = hostname
	ring.Algo = algo
	ring.Replicas = replicas
	for _, v := range flag.Args() {
		n, err := hashing.NewNodeParser(v)
		if err != nil {
			log.Fatalf("Error parsing hashring: %s", err.Error())
		}
		ring.Nodes = append(ring.Nodes, n)
	}

	return ring
}

func main() {
	var replicas int
	var hashType string
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
	flag.StringVar(&hostname, "node", hostname,
		"This node's name in the Graphite consistent hash ring.")
	flag.StringVar(&hostname, "n", hostname,
		"This node's name in the Graphite consistent hash ring.")
	flag.BoolVar(&sparseFiles, "sparse", false,
		"Be aware of sparse Whisper DB files.")
	flag.StringVar(&hashType, "hash", "carbon",
		fmt.Sprintf("Consistent Hash algorithm to use: %v", SupportedHashTypes))
	flag.IntVar(&replicas, "replicas", 1,
		"Number of copies of each metric in the cluster.")
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

	log.Printf("Starting server on %s", bindAddress)
	err = http.ListenAndServe(bindAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
