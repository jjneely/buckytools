package main

import (
	"container/heap"
	"log"
	"math"
	"os"
	"sort"
	"time"
)

import "github.com/jjneely/buckytools/metrics"
import "github.com/jjneely/journal"
import "github.com/jjneely/journal/timeseries"

const (
	// MAX_CACHE is the maximum number of data points (which take 8 bytes)
	// that we will store in the internal cache.  This should create a 1G
	// cache.
	MAX_CACHE = 1024 * 1024 * 1024 / 8

	// EVICT_TIME is the number of milliseconds to wait before running a
	// cache eviction after receiving a metric.
	EVICT_TIME = 500
)

// CacheHeap is a Heap object that forms a priority queue to manage
// writing data points to disk.
type CacheHeap []*CacheItem

// CacheStore is an ordered slice of timeseries in memory.  Sorted by
// metric name.
type CacheStore []*CacheItem

type CacheItem struct {
	metric string
	index  int
	ts     *metrics.TimeSeries // Epoch, Interval, Values
}

func (c CacheStore) Len() int {
	return len(c)
}

func (c CacheStore) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c CacheStore) Less(i, j int) bool {
	return c[i].metric < c[j].metric
}

// Search performs a binary search over the CacheStore to locate a metric.
func (c CacheStore) Search(metric string) int {
	cmp := func(i int) bool { return c[i].metric < metric }
	return sort.Search(c.Len(), cmp)
}

func (c CacheHeap) Len() int {
	return len(c)
}

func (c CacheHeap) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c CacheHeap) Less(i, j int) bool {
	// This is backwards to create a MaxHeap.  We write to disk timeseries
	// cache with the most points first.
	return len(c[j].ts.Values) < len(c[i].ts.Values)
}

func (c *CacheHeap) Push(x interface{}) {
	n := len(*c)
	item := x.(*CacheItem)
	item.index = n
	*c = append(*c, item)
}

func (c *CacheHeap) Pop() interface{} {
	old := *c
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*c = old[0 : n-1]
	return item
}

func (c *CacheHeap) update(item *CacheItem, dp *TimeSeriesPoint) {
	// Adjust timestamp for regular intervals
	timestamp := dp.Timestamp - (dp.Timestamp % item.ts.Interval)
	i := (item.ts.Epoch - timestamp) % item.ts.Interval
	length := int64(len(item.ts.Values)) // do casting only once

	switch {
	case i < 0:
		// Handle change of epoch back in time
		values := make([]float64, -i)
		values[0] = dp.Value
		for j := 1; j < len(values); j++ {
			values[j] = math.NaN()
		}
		item.ts.Values = append(values, item.ts.Values...)
		item.ts.Epoch = timestamp
	case i < length:
		item.ts.Values[i] = dp.Value
	case i == length:
		item.ts.Values = append(item.ts.Values, dp.Value)
	default: // i > len(items.ts.Values)
		values := make([]float64, i-length)
		values[len(values)-1] = dp.Value
		for j := 0; j < len(values)-1; j++ {
			values[j] = math.NaN()
		}
		item.ts.Values = append(item.ts.Values, values...)
	}

	heap.Fix(c, item.index)
}

func newCacheItem(metric *TimeSeriesPoint) *CacheItem {
	c := new(CacheItem)
	ts := new(metrics.TimeSeries)

	ts.Epoch = metric.Timestamp
	ts.Interval = 60 // XXX: Figure out schema
	ts.Values = make([]float64, 1)
	ts.Values[0] = metric.Value

	c.metric = metric.Metric
	c.ts = ts
	return c
}

func runCache() chan *TimeSeriesPoint {
	// Limits on data in cache
	timer := time.NewTimer(0)
	limit := int(math.Sqrt(MAX_CACHE))
	var timerCh <-chan time.Time

	// Data structures and resulting channel
	// XXX: cache/search need to be global or shared via reference better
	c := make(chan *TimeSeriesPoint)
	cache := make(CacheHeap, 0)
	search := make(CacheStore, 0)
	heap.Init(&cache)

	go func() {
		defer close(c)
		defer evictAll(search, cache)
		for {
			select {
			case m, ok := <-c:
				if !ok {
					// channel closed
					return
				}
				updateCache(m, search, cache)
				if timerCh == nil {
					timer.Reset(EVICT_TIME * time.Millisecond)
					timerCh = timer.C
				}
			case <-timerCh:
				evictItem(search, cache)
				timerCh = nil
				i := len(cache)
				for i > 0 && (i > limit || len(cache[i-1].ts.Values) > limit) {
					evictItem(search, cache)
				}
			}
		}
	}()

	return c
}

func updateCache(m *TimeSeriesPoint, search CacheStore, cache CacheHeap) {
	i := search.Search(m.Metric)
	switch {
	case i == search.Len():
		item := newCacheItem(m)
		search = append(search, item)
		heap.Push(&cache, item)
	case search[i].metric == m.Metric:
		cache.update(search[i], m)
	case search[i].metric != m.Metric:
		item := newCacheItem(m)
		search = append(search, nil)
		copy(search[i+1:], search[i:])
		search[i] = item
		heap.Push(&cache, item)
	}
}

func evictItem(search CacheStore, cache CacheHeap) {
	// XXX: If this fails it tosses metrics on the floor
	item := heap.Pop(&cache).(*CacheItem)
	i := search.Search(item.metric)
	copy(search[i:], search[i+1:])

	path := metrics.MetricToPath(item.metric, ".tsj")
	j, err := timeseries.Open(path)
	if os.IsNotExist(err) {
		j, err = timeseries.Create(path, item.ts.Interval,
			journal.NewFloat64ValueType(), make([]int64, 0))
	}
	if err != nil {
		log.Printf("Error opening/creating timeseries journal: %s", err)
		return
	}
	defer j.Close()

	err = metrics.JournalUpdate(j, item.ts)
	if err != nil {
		log.Printf("Error updating journal: %s", err)
		return
	}
}

func evictAll(search CacheStore, cache CacheHeap) {
	// XXX: If this fails it tosses metrics on the floor
	for cache.Len() > 0 {
		evictItem(search, cache)
	}
}
