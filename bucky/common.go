package main

import (
	"log"
	"net/http"
	"os"
	"strings"
	//"time"
)

// HttpClient is a cached http.Client. Use GetHTTP() to setup and return.
var httpClient *http.Client

// GetHTTP returns a *http.Client that can be used to interact with remote
// buckyd daemons.
func GetHTTP() *http.Client {
	if httpClient != nil {
		return httpClient
	}

	httpClient = new(http.Client)

	// Set a 30 second timeout on all operations
	//httpClient.Timeout = 30 * time.Second

	return httpClient
}

// GetBuckyPort returns the port number as a string where the buckyd
// daemon runs.  Useful for discovering hosts in the hash ring and
// knowing what port to find buckyd on.  We assume that all hosts in
// the cluster are configured consistently.
func GetBuckyPort() string {
	fields := strings.Split(HostPort, ":")
	switch len(fields) {
	case 0:
	case 1:
	case 2:
		return fields[1]
	default:
		log.Fatalf("Unable to determine port buckyd runs on.")
	}
	return "4242"
}

// HostPort is a convenience variable for sub-commands.  This holds the
// HOST:PORT to connect to if SetupHostname() is called in init()
var HostPort string

// SetupHostname sets up a generic find the host to connect to flag
func SetupHostname(c Command) {
	var host string
	if os.Getenv("BUCKYHOST") != "" {
		host = os.Getenv("BUCKYHOST")
	} else {
		host = "localhost:4242"
	}

	c.Flag.StringVar(&HostPort, "h", host,
		"HOST:PORT to find a remote buckyd daemon. Port is optional.")
	c.Flag.StringVar(&HostPort, "host", host,
		"HOST:PORT to find a remote buckyd daemon. Port is optional.")
}

// SingleHost is a convenience variable for sub-commands.  A sub-command
// must call SetupSingle() from their init() to enable.  This is true if
// -s or --single is present and restricts the operation to the original
// host and not the entire cluster.
var SingleHost bool

// SetupSingle sets up the -s|--single command flag
func SetupSingle(c Command) {
	c.Flag.BoolVar(&SingleHost, "s", false,
		"Operate on the given hostname only, do not discover the cluster members.")
	c.Flag.BoolVar(&SingleHost, "single", false,
		"Operate on the given hostname only, do not discover the cluster members.")
}

// JSONOuput is a convenience variable for sub-commands.  If setup by calling
// SetupJSON() from a sub-command's init() this will be true if the -j or
// --json flags are present and the command should dump out JSON encoded data.
var JSONOutput bool

// SetupJSON installs the -j and --json flags in the given Command
func SetupJSON(c Command) {
	c.Flag.BoolVar(&JSONOutput, "j", false,
		"Instead of text ouput JSON encoded data.")
	c.Flag.BoolVar(&JSONOutput, "json", false,
		"Instead of text ouput JSON encoded data.")
}
