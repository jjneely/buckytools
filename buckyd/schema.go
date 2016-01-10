package main

import (
	"strings"
)

// MetricInterval examines the given Graphite style metric name and returns
// the associated interval in seconds.  XXX: This should be based off a
// configuration source.
func MetricInterval(metric string) int64 {
	switch {
	case strings.HasPrefix(metric, "1sec."):
		return 1
	case strings.HasPrefix(metric, "1min."):
		return 60
	case strings.HasPrefix(metric, "5min."):
		return 300
	case strings.HasPrefix(metric, "10min."):
		return 600
	case strings.HasPrefix(metric, "15min."):
		return 900
	case strings.HasPrefix(metric, "hourly."):
		return 3600
	case strings.HasPrefix(metric, "daily."):
		return 86400
	}

	// Default
	return 60
}
