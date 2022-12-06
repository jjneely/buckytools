package metrics

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
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
	version2MagicString     = "\x7fflc02" // magic string from go carbon
	flcv2StatFieldSize      = 8
	flcv2StatFieldCount     = 4
	flcv2EntrySeparatorSize = 1
	flcv2EntryStatLen       = flcv2StatFieldSize*flcv2StatFieldCount + flcv2EntrySeparatorSize
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

var (
	CachePath string
	Prefix    string
)

// CacheTimeOut is the time in seconds that a metrics cache is considered
// fresh.  If the update timestamp is less than time.Now() - timeout then
// the cache must be refreshed.
var CacheTimeOut int64

// Init common bits
func init() {
	flag.Int64Var(&CacheTimeOut, "timeout", 3600,
		"Maximum time in seconds metric keyspace cache is valid")
	flag.StringVar(&Prefix, "prefix", "/opt/graphite/storage/whisper",
		"The root of the whisper database store.")
	flag.StringVar(&Prefix, "p", "/opt/graphite/storage/whisper",
		"The root of the whisper database store.")
	flag.StringVar(&CachePath, "cache_path", "/var/lib/carbon/carbonserver-file-list-cache.gzip",
		"The path to go-carbon Trie Index Tree cache file")
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

// TimedOut returns true if the cache hasn't been refreshed recently.
func (m *MetricsCacheType) TimedOut() bool {
	return time.Now().Unix()-m.timestamp > CacheTimeOut
}

type fileListCacheV2 struct {
	path   string
	file   *os.File
	reader *gzip.Reader
}

func (flc fileListCacheV2) Close() error {
	var errs []string
	if err := flc.file.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("could not close file %s", err))
	}
	if err := flc.reader.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("could not close reader %s", err))
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ";"))
	}
	return nil
}

func (flc fileListCacheV2) Read() (string, error) {
	var plenBuf [8]byte
	if _, err := io.ReadFull(flc.reader, plenBuf[:]); err != nil {
		err = fmt.Errorf("flcv2: failed to read path len: %w", err)
		return "", err
	}
	plen := int(binary.BigEndian.Uint64(plenBuf[:]))

	// filepath on linux has a 4k limit, but we are timing it 2 here just to
	// be flexible and avoid bugs or corruptions to causes panics or oom in
	// go-carbon
	//
	// * https://man7.org/linux/man-pages/man3/realpath.3.html#NOTES
	// * https://www.ibm.com/docs/en/spectrum-protect/8.1.9?topic=parameters-file-specification-syntax
	const maxPathLen = 4096 * 2
	if plen > maxPathLen {
		err := fmt.Errorf("flcv2: illegal file path length %d (max: %d)", plen, maxPathLen)
		return "", err
	}

	data := make([]byte, plen+flcv2EntryStatLen)
	if _, err := io.ReadFull(flc.reader, data); err != nil {
		err = fmt.Errorf("flcv2: failed to read full data: %w", err)
		return "", err
	}
	return string(data[:plen]), nil
}

func NewFileListCache(path string) (*fileListCacheV2, error) {
	flc := fileListCacheV2{}
	var err error
	flc.file, err = os.Open(path)
	if err != nil {
		return nil, err
	}
	flc.reader, err = gzip.NewReader(flc.file)
	if err != nil {
		return nil, err
	}
	magic := make([]byte, len(version2MagicString))

	switch n, err := flc.reader.Read(magic); {
	case err != nil:
		return nil, err
	case n != len(version2MagicString):
		return nil, fmt.Errorf("failed to read full v2 magic string (%d): %d", len(version2MagicString), n)
	case !bytes.Equal(magic, []byte(version2MagicString)):
		flc.Close()
		return nil, fmt.Errorf("wrong cache file version")
	}

	return &flc, nil
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
	log.Printf("Scanning %s for metrics...", Prefix)
	readFromCache := false
	if CachePath != "" {
		readFromCache = true
		m.metrics = make([]string, 0)
		flc, err := NewFileListCache(CachePath)
		if err != nil {
			log.Printf("Could not open File Cache: %s", err)
			readFromCache = false
		} else {
			for {
				metricPath, err := flc.Read()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						log.Printf("Could not read File Cache: %s", err)
						readFromCache = false
					}
					log.Printf("File cache scan complete.")
					break
				}
				m.metrics = append(m.metrics, PathToMetric(metricPath))
			}
		}
	}

	if !readFromCache {
		err := filepath.Walk(Prefix, examine)
		log.Printf("Whisper files scan complete.")
		if err != nil {
			log.Printf("Scan returned an Error: %s", err)
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
