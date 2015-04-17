package main

import (
	"flag"
	"path"
	"strings"
)

var Prefix string

// Init common bits
func init() {
	flag.StringVar(&Prefix, "prefix", "/opt/graphite/storage/whisper",
		"The root of the whisper database store.")
	flag.StringVar(&Prefix, "p", "/opt/graphite/storage/whisper",
		"The root of the whisper database store.")
}

func MetricToPath(metric string) string {
	p := strings.Replace(metric, ".", "/", -1) + ".wsp"
	return path.Join(Prefix, p)
}

func MetricsToPaths(metrics []string) []string {
	p := make([]string, 0)
	for _, m := range metrics {
		p = append(p, MetricToPath(m))
	}

	return p
}

func PathToMetric(p string) string {
	// XXX: What do we do with absoluate paths that don't begin with Prefix?
	if strings.HasPrefix(p, Prefix+"/") {
		p = p[len(Prefix)+1:]
	}
	if strings.HasPrefix(p, "/") {
		p = p[1:]
	}

	p = strings.Replace(p, ".wsp", "", 1)
	return strings.Replace(p, ".", "/", -1)
}

func PathsToMetrics(p []string) []string {
	ret := make([]string, 0)
	for _, v := range p {
		ret = append(ret, PathToMetric(v))
	}

	return ret
}
