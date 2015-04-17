package main

import (
	"testing"
)

func TestMetricToPath(t *testing.T) {
	Prefix = "/foo"
	path := MetricToPath("bobby.sue.foo.bar")
	if path != "/foo/bobby/sue/foo/bar.wsp" {
		t.Errorf("MetricToPath returned %s for %s, rather than %s",
			path, "bobby.sue.foo.bar", "/foo/bobby/sue/foo/bar.wsp")
	}
}
