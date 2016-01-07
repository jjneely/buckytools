package metrics

import (
	"flag"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// TimeSeries is the in memory and transit representation of time series
// data.
type TimeSeries struct {
	Epoch    int64     `json: epoch`
	Interval int64     `json: interval`
	Values   []float64 `json: values`
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
// using the --prefix flag.  You must supply the metric file's path suffix
// such as ".wsp".
func MetricToPath(metric, suffix string) string {
	p := MetricToRelative(metric, suffix)
	return path.Join(Prefix, p)
}

// MetricToRelative take a metric name and returns a relative path
// to the Whisper DB.  This path combined with the root path to the
// DB store would create a proper absolute path.  You must supply the file's
// path suffix such as ".wsp".
func MetricToRelative(metric, suffix string) string {
	p := strings.Replace(metric, ".", "/", -1) + suffix
	return path.Clean(p)
}

// MetricsToPaths operates on a slice of metric names and returns a
// slice of absolute paths using the --prefix flag.  Suffix such as ".wsp"
// is required.
func MetricsToPaths(metrics []string, suffix string) []string {
	p := make([]string, 0)
	for _, m := range metrics {
		p = append(p, MetricToPath(m, suffix))
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
	return RelativeToMetric(p)
}

// RelativeToMetric takes a relative path from the root of your DB store
// and translates it into a metric name.  Path is path.Clean()'d before
// transformed.
func RelativeToMetric(p string) string {
	p = path.Clean(p)
	dir, file := path.Split(p)
	dir = strings.Replace(dir, "/", ".", -1)
	// Any "." in the file begins meta information, like .wsp for Whisper DBs
	file = strings.Split(file, ".")[0]
	return dir + file
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

// checkWalk is a helper function to sanity check for timeseries DB files in a
// file tree walk.  If the file is valid, normal timeseries nil is returned.
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
	if strings.HasSuffix(path, ".tsj") {
		return true, nil
	}
	if strings.HasSuffix(path, ".wsp") {
		return true, nil
	}

	// Not a Whisper Database
	return false, nil
}

// NewMetricsCache creates and returns a MetricsCacheType object
func NewMetricsCache() *MetricsCacheType {
	m := new(MetricsCacheType)
	m.metrics = nil
	m.updating = false
	return m
}

// IsAvailable returns a boolean true value if the MetricsCache is available
// for use.  Rebuilding the cache can take some time.
func (m *MetricsCacheType) IsAvailable() bool {
	return m.metrics != nil && !m.updating
}

// TimedOut returns true if the cache hasn't been refreshed recently.
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

	// Sort and Unique
	sort.Strings(m.metrics)
	length := len(m.metrics) - 1
	for i := 0; i < length; i++ {
		if m.metrics[i] == m.metrics[i+1] {
			m.metrics = append(m.metrics[:i], m.metrics[i+1:]...)
		}
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
