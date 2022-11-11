package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strings"

	. "github.com/go-graphite/buckytools"
	"github.com/go-graphite/buckytools/hashing"
	"github.com/go-graphite/buckytools/metrics"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/pyroscope-io/client/pyroscope"
)

var metricsCache *metrics.MetricsCacheType
var tmpDir string
var hashring *hashing.JSONRingType

// sparseFiles defines if we create and manage sparse files.
var sparseFiles bool
var compressed bool
var authJWTSecretKey []byte
var authJWTRootAPIToken string
var obeyRemoteMtime bool

func usage() {
	t := []string{
		"%s [options] <graphite-node1> <graphite-node2> ...\n",
		"Version: %s\n\n",
		"Usage help: %s --help\n\n",
		"\tA daemon that provides a REST API for remotely uploading,\n",
		"\tdownloading, deleting, backfilling and getting metadata\n",
		"\tfor the Graphite Whisper DBs in the local machine's data\n",
		"\tstore.  The non-optional arguments are the members of your\n",
		"\tconsistent hashring as found in your carbon-relay configuration.\n",
		"\tAll of the daemons in your cluster need to be able to build\n",
		"\tthe same hashring.  You may specify nodes in the following\n",
		"\tformat: HOST[:PORT][=INSTANCE]\n\n",
	}

	fmt.Printf(strings.Join(t, ""), os.Args[0], Version, os.Args[0])
	//flag.PrintDefaults()
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
	var pprofAddress string
	var pyroscopeAddress string
	var help bool
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "UNKNOWN"
	}

	var authJWTSecretFile string

	flag.Usage = usage
	flag.StringVar(&tmpDir, "tmpdir", os.TempDir(),
		"Temporary file location.")
	flag.StringVar(&tmpDir, "t", os.TempDir(),
		"Temporary file location.")
	flag.StringVar(&bindAddress, "bind", "0.0.0.0:4242",
		"IP:PORT to listen for HTTP requests.")
	flag.StringVar(&bindAddress, "b", "0.0.0.0:4242",
		"IP:PORT to listen for HTTP requests.")
	flag.StringVar(&pprofAddress, "pprof", "localhost:6060",
		"IP:PORT to listen for pprof.")
	flag.StringVar(&pyroscopeAddress, "pyroscope", "",
		"IP:PORT of Pyroscope server")
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
	flag.BoolVar(&compressed, "compressed", false,
		"Create new whisper file in compressed format.")
	flag.StringVar(&authJWTSecretFile, "auth-jwt-secret-file", "",
		"API auth JWT private secret file.")
	flag.BoolVar(&obeyRemoteMtime, "mtime", false,
		"Use modify time from metric as mtime of local file (default false)")
	flag.BoolVar(&help, "help", false,
		"Usage help")
	flag.Parse()

	if help {
		usage()
		flag.PrintDefaults()
		os.Exit(0)
	}

	i := sort.SearchStrings(SupportedHashTypes, hashType)
	if i == len(SupportedHashTypes) || SupportedHashTypes[i] != hashType {
		log.Fatalf("Invalide hash type.  Supported types: %v",
			SupportedHashTypes)
	}
	hashring = parseRing(hostname, hashType, replicas)

	if authJWTSecretFile != "" {
		secret, err := ioutil.ReadFile(authJWTSecretFile)
		if err != nil {
			log.Fatal(err)
		}
		authJWTSecretKey = bytes.TrimSpace(secret)
		authJWTRootAPIToken, err = generateRootAPITokenForInterBuckydAPICalls()
		if err != nil {
			log.Fatal(err)
		}
	}

	if pprofAddress != "" {
		log.Printf("Starting pprof server on %s", pprofAddress)
		p := mux.NewRouter()
		p.HandleFunc("/debug/pprof/", pprof.Index)
		p.HandleFunc("/debug/pprof/profile", pprof.Profile)
		p.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		p.HandleFunc("/debug/pprof/trace", pprof.Trace)
		go func() {
			log.Println(http.ListenAndServe(pprofAddress, handlers.CombinedLoggingHandler(os.Stdout, p)))
		}()
	}

	if pyroscopeAddress != "" {
		log.Printf("Pushing flamegraphs to pyroscope server on %s", pyroscopeAddress)
		// These 2 lines are only required if you're using mutex or block profiling
		// Read the explanation below for how to set these rates:
		runtime.SetMutexProfileFraction(5)
		runtime.SetBlockProfileRate(5)
		pyroscope.Start(pyroscope.Config{
			ApplicationName: "buckyd",
			// replace this with the address of pyroscope server
			ServerAddress: pyroscopeAddress,
			// you can disable logging by setting this to nil
			Logger: nil,
			// optionally, if authentication is enabled, specify the API key:
			// AuthToken: os.Getenv("PYROSCOPE_AUTH_TOKEN"),
			ProfileTypes: []pyroscope.ProfileType{
				// these profile types are enabled by default:
				pyroscope.ProfileCPU,
				pyroscope.ProfileAllocObjects,
				pyroscope.ProfileAllocSpace,
				pyroscope.ProfileInuseObjects,
				pyroscope.ProfileInuseSpace,
				// these profile types are optional:
				pyroscope.ProfileGoroutines,
				pyroscope.ProfileMutexCount,
				pyroscope.ProfileMutexDuration,
				pyroscope.ProfileBlockCount,
				pyroscope.ProfileBlockDuration,
			},
		})
	}

	r := mux.NewRouter()
	r.HandleFunc("/", http.NotFound)
	r.HandleFunc("/metrics", listMetrics)
	r.HandleFunc("/metrics/", serveMetrics)
	r.HandleFunc("/hashring", listHashring)
	// disabling pprof routing on main port
	r.HandleFunc("/debug/pprof/", http.NotFound)
	r.HandleFunc("/debug/pprof/cmdline", http.NotFound)
	r.HandleFunc("/debug/pprof/profile", http.NotFound)
	r.HandleFunc("/debug/pprof/symbol", http.NotFound)
	r.HandleFunc("/debug/pprof/trace", http.NotFound)
	log.Printf("Starting server on %s", bindAddress)
	err = http.ListenAndServe(bindAddress, handlers.LoggingHandler(os.Stdout, r))
	if err != nil {
		log.Fatal(err)
	}
}
