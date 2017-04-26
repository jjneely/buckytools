package metrics

import (
	"flag"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Supported Encodings
const (
	EncIdentity = iota
	EncSnappy
	EncMax
)

// MetricData represents an individual metric and its raw data.
type MetricData struct {
	Name     string
	Size     int64
	Mode     int64
	ModTime  int64
	Encoding int
	Data     []byte `json:"-"` // We never JSON encode metric data
}

type MetricsCacheType struct {
	metrics   []string
	timestamp int64
	lock      sync.Mutex
	updating  bool
}

var Prefix string

// Init common bits
func init() {
	flag.StringVar(&Prefix, "prefix", "/opt/graphite/storage/whisper",
		"The root of the whisper database store.")
	flag.StringVar(&Prefix, "p", "/opt/graphite/storage/whisper",
		"The root of the whisper database store.")
}

// MetricToPath takes a metric name and return an absolute path
// using the --prefix flag.
func MetricToPath(metric string) string {
	p := MetricToRelative(metric)
	return path.Join(Prefix, p)
}

// MetricToRelative take a metric name and returns a relative path
// to the Whisper DB.  This path combined with the root path to the
// DB store would create a proper absolute path.
func MetricToRelative(metric string) string {
	p := strings.Replace(metric, ".", "/", -1) + ".wsp"
	return path.Clean(p)
}

// MetricsToPaths operates on a slice of metric names and returns a
// slice of absolute paths using the --prefix flag.
func MetricsToPaths(metrics []string) []string {
	p := make([]string, 0)
	for _, m := range metrics {
		p = append(p, MetricToPath(m))
	}

	return p
}

// PathToMetric takes an absolute path that begins with the --prefix flag
// and returns the metric name.  The path is path.Clean()'d before
// transformed.
func PathToMetric(p string) string {
	// XXX: What do we do with absolute paths that don't begin with Prefix?
	p = path.Clean(p)
	if strings.HasPrefix(p, Prefix) {
		p = p[len(Prefix):]
	}
	if strings.HasPrefix(p, "/") {
		p = p[1:]
	}

	p = strings.Replace(p, ".wsp", "", 1)
	return strings.Replace(p, "/", ".", -1)
}

// RelativeToMetric takes a relative path from the root of your DB store
// and translates it into a metric name.  Path is path.Clean()'d before
// transformed.
func RelativeToMetric(p string) string {
	p = path.Clean(p)
	p = strings.Replace(p, ".wsp", "", 1)
	return strings.Replace(p, "/", ".", -1)
}

// PathsToMetrics operates on a slice of absolute paths prefixed with
// the --prefix flag and returns a slice of metric names.
func PathsToMetrics(p []string) []string {
	ret := make([]string, 0)
	for _, v := range p {
		ret = append(ret, PathToMetric(v))
	}

	return ret
}

// FilterList returns a slice of strings that contain only the string found
// in both arguments.  Set intersection.
func FilterList(filter, metrics []string) []string {
	result := make([]string, 0)
	hash := make(map[string]bool)
	for _, v := range filter {
		hash[v] = true
	}

	for _, v := range metrics {
		if hash[v] {
			result = append(result, v)
		}
	}

	return result
}

// FilterRegex returns a sub set of metrics that match the given regex pattern.
func FilterRegex(regex string, metrics []string) ([]string, error) {
	r, err := regexp.Compile(regex)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)

	for _, v := range metrics {
		if r.MatchString(v) {
			result = append(result, v)
		}
	}

	return result, nil
}

// checkWalk is a helper function to sanity check for *.wsp files in a
// file tree walk.  If the file is valid, normal *.wsp nil is returned.
// Otherwise a non-nil error value is returned.
func checkWalk(path string, info os.FileInfo, err error) (bool, error) {
	// Did the Walk function hit an error on this file?
	if err != nil {
		log.Printf("%s\n", err)
		// File perm or exists error, log and skip
		return false, nil
	}

	// Sanity check our file
	if info.IsDir() {
		// Ignore dot-files and dot-directories
		if strings.HasPrefix(info.Name(), ".") {
			return false, filepath.SkipDir
		}
		return false, nil
	}
	if !info.Mode().IsRegular() {
		// Not a regular file
		return false, nil
	}
	if !strings.HasSuffix(path, ".wsp") {
		// Not a Whisper Database
		return false, nil
	}

	return true, nil
}

// NewMetricsCache creates and returns a MetricsCacheType object
func NewMetricsCache() *MetricsCacheType {
	m := new(MetricsCacheType)
	m.metrics = nil
	m.updating = false
	return m
}

// IsAvailable returns a boolean true value if the MetricsCache is avaliable
// for use.  Rebuilding the cache can take some time.
func (m *MetricsCacheType) IsAvailable() bool {
	return m.metrics != nil && !m.updating
}

// TimedOut returns true if the cache hasn't been refresed recently.
func (m *MetricsCacheType) TimedOut() bool {
	// 1 hour cache timeout
	return time.Now().Unix()-m.timestamp > 3600
}

// RefreshCache updates the list of metric names in the cache from the local
// file store.  Blocks until completion.  Does not check cache freshness
// so use with care.
func (m *MetricsCacheType) RefreshCache() error {
	m.lock.Lock()
	m.updating = true

	examine := func(path string, info os.FileInfo, err error) error {
		ok, err := checkWalk(path, info, err)
		if err != nil {
			return err
		}
		if ok {
			//log.Printf("Found %s or %s", path, PathToMetric(path))
			m.metrics = append(m.metrics, PathToMetric(path))
		}
		return nil
	}

	// Create new empty slice
	log.Printf("Scaning %s for metrics...", Prefix)
	m.metrics = make([]string, 0)
	err := filepath.Walk(Prefix, examine)
	log.Printf("Scan complete.")
	if err != nil {
		log.Printf("Scan returned an Error: %s", err)
	}

	m.timestamp = time.Now().Unix()
	m.updating = false
	m.lock.Unlock()
	return nil
}

// GetMetrics returns a slice of metric key names and an ok boolean.
// This function returns immediately even if the metric cache is out of
// date and is being refreshed.  In this case ok will be false until
// the cache is rebuilt.
func (m *MetricsCacheType) GetMetrics() ([]string, bool) {
	if m.IsAvailable() && !m.TimedOut() {
		return m.metrics, true
	}

	if !m.updating {
		go m.RefreshCache()
	}
	return nil, false
}
