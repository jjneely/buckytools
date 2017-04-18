package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
)

import . "github.com/jjneely/buckytools"

// httpClient is a cached http.Client. Use GetHTTP() to setup and return.
var httpClient *http.Client

// MetricData represents an individual metric and its raw data.
// XXX: Unify this with MetricStatType?
type MetricData struct {
	Name    string
	Size    int64
	Mode    int64
	ModTime int64
	Data    []byte
}

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

// DeleteMetric sends a DELETE request for the given metric to the given
// server.  The port is assumed the same for all Bucky daemons in the
// hash ring.
func DeleteMetric(server, metric string) error {
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", server, Cluster.Port),
		Path:   "/metrics/" + metric,
	}

	r, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return err
	}

	resp, err := httpClient.Do(r)
	if err != nil {
		log.Printf("Error communicating: %s", err)
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		log.Printf("DELETED: %s", metric)
	case 404:
		log.Printf("Not found / Not deleted: %s", metric)
		return fmt.Errorf("Metric not found.")
	case 500:
		msg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			msg = []byte(err.Error())
		}
		log.Printf("Error: Internal Server Error: %s", string(msg))
		return fmt.Errorf("Error: Internal Server Error: %s", string(msg))
	default:
		log.Printf("Error: Unknown response from server.  Code %s", resp.Status)
		return fmt.Errorf("Unknown response from server.  Code %s", resp.Status)
	}

	return nil
}

// GetMetricData retrieves the binary Whisper data for a given metric
// name that lives on the given server.  The port buckyd runs on is
// assumed to be the same as other servers in the hash ring.
func GetMetricData(server, name string) (*MetricData, error) {
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Path:   "/metrics/" + name,
	}
	host, port, err := net.SplitHostPort(server)
	if err != nil {
		log.Printf("Malformed hostname: %s", server)
		return nil, err
	}
	if port == "" {
		port = Cluster.Port
	}
	u.Host = net.JoinHostPort(host, port)
	r, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return nil, err
	}

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
		return nil, fmt.Errorf("Fetching metric returned status code: %s", resp.Status)
	}

	data := new(MetricData)
	err = json.Unmarshal([]byte(resp.Header.Get("X-Metric-Stat")), &data)
	if err != nil {
		log.Printf("Error unmarshalling X-Metric-Stat header for [%s]:%s: %s", server, name, err)
		return nil, err
	}

	data.Data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %s", err)
		return nil, err
	}

	return data, nil
}

func StatRemoteMetric(server, metric string) (*MetricStatType, error) {
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", server, Cluster.Port),
		Path:   "/metrics/" + metric,
	}

	r, err := http.NewRequest("HEAD", u.String(), nil)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return nil, err
	}

	resp, err := httpClient.Do(r)
	if err != nil {
		log.Printf("Error communicating: %s", err)
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		data := resp.Header.Get("X-Metric-Stat")
		if data == "" {
			log.Printf("No stat data returned for: %s", metric)
			return nil, fmt.Errorf("No stat data returned for: %s", metric)
		}
		stat := new(MetricStatType)
		err := json.Unmarshal([]byte(data), &stat)
		if err != nil {
			log.Printf("Error: Could not parse X-Metric-Stat header for %s", metric)
			return nil, err
		}
		return stat, nil
	case 404:
		log.Printf("Metric not found: %s", metric)
		return nil, fmt.Errorf("Metric not found.")
	case 500:
		msg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			msg = []byte(err.Error())
		}
		log.Printf("Error: Internal Server Error: %s", string(msg))
		return nil, fmt.Errorf("Error: Internal Server Error: %s", string(msg))
	default:
		log.Printf("Error: Unknown response from server.  Code %s", resp.Status)
		return nil, fmt.Errorf("Unknown response from server.  Code %s", resp.Status)
	}

	// shouldn't get here
	return nil, fmt.Errorf("Unexpected error in StatRemoteMetric()")
}

// GetSingleHashRing connects to the given server and returns a JSONRingType
// representing its hashring configuration.  An error value is
// set if we could not retrieve the hashring information.  The server
// string must include any port information.
func GetSingleHashRing(server string) (*JSONRingType, error) {
	u := &url.URL{
		Scheme: "http",
		Host:   server,
		Path:   "/hashring",
	}
	httpClient := GetHTTP()

	r, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return nil, err
	}
	resp, err := httpClient.Do(r)
	if err != nil {
		log.Printf("Error retrieving URL: %s", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Abort: /hashring API called returned: %s", resp.Status)
		return nil, fmt.Errorf("/hashring API called returned: %s", resp.Status)
	}

	ring := new(JSONRingType)
	blob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %s", err)
		return nil, err
	}
	err = json.Unmarshal(blob, &ring)
	if err != nil {
		log.Printf("Could not unmarshal JSON from host %s: %s", server, err)
		return nil, err
	}

	return ring, nil
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

// CleanMetric sanitizes the given metric key by removing adjacent "."
// characters and replacing any "/" characters with "."
func CleanMetric(m string) string {
	// Slash isn't really illegal but gets interperated as a directory
	// on the Graphite server
	m = strings.Replace(m, "/", ".", -1)

	// Adjacent "." end up producing non-normalized paths
	for strings.Index(m, "..") != -1 {
		m = strings.Replace(m, "..", ".", -1)
	}

	// Can't begin with a "."
	if strings.Index(m, ".") == 0 {
		m = m[1:]
	}

	return m
}
