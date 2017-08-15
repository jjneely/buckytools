package main

import (
	"bytes"
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

import "github.com/golang/snappy"

import . "github.com/jjneely/buckytools/metrics"
import "github.com/jjneely/buckytools/hashing"

// HostPort is a convenience variable for sub-commands.  This holds the
// HOST:PORT to connect to if SetupHostname() is called in init()
var HostPort string

// NoEncoding is a flag to disable compression of transferred Whisper
// files.  Or other possible encodings of transferred files.
var NoEncoding bool

// Verbose is a flag to indicate verbose logging
var Verbose bool

// httpClient is a cached http.Client. Use GetHTTP() to setup and return.
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

// MetricDecode accepts a MetricData struct and returns a slice of bytes
// that is the data from the MetricData struct decoded.
func MetricDecode(metric *MetricData) ([]byte, error) {
	var data []byte
	var err error

	switch metric.Encoding {
	case EncIdentity:
		return metric.Data, nil
	case EncSnappy:
		snp := snappy.NewReader(bytes.NewBuffer(metric.Data))
		data, err = ioutil.ReadAll(snp)
	}
	if int64(len(data)) != metric.Size {
		log.Printf("Encoding error: Unencoded data size does not match original %d != %d",
			len(data), metric.Size)
		return data, fmt.Errorf("Encoding error")
	}

	return data, err
}

// MetricEncode takes a completed MetricData struct and upgrades the
// encoding provided the initial encoding is the identity encoding.
// Basically, this compresses the data.
func MetricEncode(metric *MetricData, encoding int) error {
	if metric.Encoding != 0 {
		return fmt.Errorf("Metric already encoded to encoding type %d",
			metric.Encoding)
	}
	if encoding == 0 {
		// Success!
		return nil
	}
	if encoding >= EncMax || encoding < 0 {
		return fmt.Errorf("Invalid encoding type %d", encoding)
	}

	buf := new(bytes.Buffer)
	writer := snappy.NewBufferedWriter(buf)
	_, ew := writer.Write(metric.Data)
	if ew != nil {
		return ew
	}
	writer.Close()
	metric.Data = buf.Bytes()
	metric.Encoding = EncSnappy

	return nil
}

// SanitizeHostPort parses and sanitizes the host:port string.  If no port
// is present the Cluster.Port configuration value will be used as the port.
// The returned hostport string will have a host and port.
func SanitizeHostPort(hostport string) (string, error) {
	host, port, err := net.SplitHostPort(hostport)
	// Can't just compare the err.String() as it has the hostname in it too
	if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
		port = Cluster.Port
		host = hostport
	} else if err != nil {
		return "", err
	}

	return net.JoinHostPort(host, port), nil
}

// DeleteMetric sends a DELETE request for the given metric to the given
// server.  The port is assumed the same for all Bucky daemons in the
// hash ring.
func DeleteMetric(server, metric string) error {
	var err error
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Path:   "/metrics/" + metric,
	}
	u.Host, err = SanitizeHostPort(server)
	if err != nil {
		log.Printf("Malformed hostname: %s", err)
		return err
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
	var err error
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Path:   "/metrics/" + name,
	}
	u.Host, err = SanitizeHostPort(server)
	if err != nil {
		log.Printf("Malformed hostname: %s", err)
		return nil, err
	}
	r, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return nil, err
	}
	if !NoEncoding {
		r.Header.Set("accept-encoding", "snappy")
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

func StatRemoteMetric(server, metric string) (*MetricData, error) {
	var err error
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Path:   "/metrics/" + metric,
	}
	u.Host, err = SanitizeHostPort(server)
	if err != nil {
		log.Printf("Malformed hostname: %s", err)
		return nil, err
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
		stat := new(MetricData)
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

// PostMetric sends a POST request with new metric data to the given server.
// A post request does a backfill if this metric is already present on disk.
func PostMetric(server string, metric *MetricData) error {
	var err error
	httpClient := GetHTTP()
	u := &url.URL{
		Scheme: "http",
		Path:   "/metrics/" + metric.Name,
	}
	u.Host, err = SanitizeHostPort(server)
	if err != nil {
		log.Printf("Malformed hostname: %s", err)
		return nil
	}

	buf := bytes.NewBuffer(metric.Data)
	r, err := http.NewRequest("POST", u.String(), buf)
	if err != nil {
		log.Printf("Error building request: %s", err)
		return err
	}
	statInfo, err := json.Marshal(metric)
	if err != nil {
		return err
	}
	r.Header.Set("X-Metric-Stat", string(statInfo))
	r.Header.Set("Content-Type", "application/octet-stream")
	switch metric.Encoding {
	case EncSnappy:
		r.Header.Set("Content-Encoding", "snappy")
	}

	// This doesn't return until the backfill operation completes
	resp, err := httpClient.Do(r)
	if err != nil {
		log.Printf("Error communicating with server: %s", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		msg := fmt.Sprintf("Error reported by server: %s for metric %s",
			resp.Status, metric.Name)
		log.Printf("%s", msg)
		return fmt.Errorf("%s", msg)
	}

	return nil
}

// GetSingleHashRing connects to the given server and returns a JSONRingType
// representing its hashring configuration.  An error value is
// set if we could not retrieve the hashring information.  The server
// string must include any port information.
func GetSingleHashRing(server string) (*hashing.JSONRingType, error) {
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

	ring := new(hashing.JSONRingType)
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

// SetupCommon sets up common flags for all modules
func SetupCommon(c Command) {
	c.Flag.BoolVar(&Verbose, "v", false,
		"Verbose log output.")
	c.Flag.BoolVar(&Verbose, "verbose", false,
		"Verbose log output.")
	c.Flag.BoolVar(&NoEncoding, "no-encoding", false,
		"Disable Content-Encoding methods for HTTP API calls.")
}

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
