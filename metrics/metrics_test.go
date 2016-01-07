package metrics

import (
	"testing"
)

var testMetrics = map[string]string{
	"bobby.sue.foo.bar": "/opt/graphite/storage/whisper/bobby/sue/foo/bar.wsp",
}

func TestMetricToPath(t *testing.T) {
	path := MetricToPath("bobby.sue.foo.bar", ".foo")
	if path != "/opt/graphite/storage/whisper/bobby/sue/foo/bar.foo" {
		t.Errorf("MetricToPath returned %s for %s, rather than %s",
			path, "bobby.sue.foo.bar", "/opt/graphite/storage/whisper/sue/foo/bar.foo")
	}
}

func TestPathToMetric(t *testing.T) {
	metric := PathToMetric("/opt/graphite/storage/whisper/bobby/sue/foo/bar.2011.wsp")
	if metric != "bobby.sue.foo.bar" {
		t.Errorf("PathToMetric returned %s for %s, rather than %s",
			metric, "/opt/graphite/storage/whisper/bobby/sue/foo/bar.2011.wsp",
			"bobby.sue.foo.bar")
	}
}
