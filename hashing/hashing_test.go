package hashing

import (
	"fmt"
	"testing"
)

func makeRing() *CarbonHashRing {
	hr := NewCarbonHashRing()
	hr.AddNode(NewNode("graphite010-g5", 0, "a"))
	hr.AddNode(NewNode("graphite010-g5", 0, "b"))
	hr.AddNode(NewNode("graphite010-g5", 0, "c"))

	hr.AddNode(NewNode("graphite011-g5", 0, "a"))
	hr.AddNode(NewNode("graphite011-g5", 0, "b"))
	hr.AddNode(NewNode("graphite011-g5", 0, "c"))

	hr.AddNode(NewNode("graphite012-g5", 0, "a"))
	hr.AddNode(NewNode("graphite012-g5", 0, "b"))
	hr.AddNode(NewNode("graphite012-g5", 0, "c"))

	hr.AddNode(NewNode("graphite013-g5", 0, "a"))
	hr.AddNode(NewNode("graphite013-g5", 0, "b"))
	hr.AddNode(NewNode("graphite013-g5", 0, "c"))

	hr.AddNode(NewNode("graphite014-g5", 0, "a"))
	hr.AddNode(NewNode("graphite014-g5", 0, "b"))
	hr.AddNode(NewNode("graphite014-g5", 0, "c"))

	hr.AddNode(NewNode("graphite015-g5", 0, "a"))
	hr.AddNode(NewNode("graphite015-g5", 0, "b"))
	hr.AddNode(NewNode("graphite015-g5", 0, "c"))

	hr.AddNode(NewNode("graphite016-g5", 0, "a"))
	hr.AddNode(NewNode("graphite016-g5", 0, "b"))
	hr.AddNode(NewNode("graphite016-g5", 0, "c"))

	hr.AddNode(NewNode("graphite017-g5", 0, "a"))
	hr.AddNode(NewNode("graphite017-g5", 0, "b"))
	hr.AddNode(NewNode("graphite017-g5", 0, "c"))

	hr.AddNode(NewNode("graphite018-g5", 0, "a"))
	hr.AddNode(NewNode("graphite018-g5", 0, "b"))
	hr.AddNode(NewNode("graphite018-g5", 0, "c"))

	hr.AddNode(NewNode("graphite-data019-g5", 0, "a"))
	hr.AddNode(NewNode("graphite-data019-g5", 0, "b"))
	hr.AddNode(NewNode("graphite-data019-g5", 0, "c"))

	hr.AddNode(NewNode("graphite-data020-g5", 0, "a"))
	hr.AddNode(NewNode("graphite-data020-g5", 0, "b"))
	hr.AddNode(NewNode("graphite-data020-g5", 0, "c"))

	hr.AddNode(NewNode("graphite-data021-g5", 0, "a"))
	hr.AddNode(NewNode("graphite-data021-g5", 0, "b"))
	hr.AddNode(NewNode("graphite-data021-g5", 0, "c"))

	hr.AddNode(NewNode("graphite-data022-g5", 0, "a"))
	hr.AddNode(NewNode("graphite-data022-g5", 0, "b"))
	hr.AddNode(NewNode("graphite-data022-g5", 0, "c"))

	return hr
}

func TestEmptyInstance(t *testing.T) {
	hr := NewCarbonHashRing()

	for _, s := range []string{"test01", "test02"} {
		n := NewNode(s, 0, "")
		hr.AddNode(n)
	}

	expected := map[string]string{
		"statsd.disk.free1": "test02:0=None",
		"statsd.disk.free2": "test02:0=None",
		"statsd.disk.free3": "test01:0=None",
	}
	for k, v := range expected {
		node := hr.GetNode(k)
		if node.String() != v {
			t.Errorf("Hash not compatible: %s !=> %s, rather %s", k, v, node)
		}
	}
}

func TestNewNode(t *testing.T) {
	n := NewNode("graphite010-g5", 0, "a")
	if n.CarbonKeyValue() != "('graphite010-g5', 'a')" {
		t.Error("NewNode() did not produce a tuple string format")
	}

	if NewNode("graphite011-g5", 0, "").CarbonKeyValue() != "('graphite011-g5', None)" {
		t.Error("NewNode() did not handle a None instance value")
	}

	if n.Server != "graphite010-g5" {
		t.Error("Node type can't store servers properly")
	}

	if n.Instance != "a" {
		t.Error("Node type can't store instances properly")
	}

	if n.String() != "graphite010-g5:0=a" {
		t.Error("Node string representation is broken")
	}
}

func TestNewHashRing(t *testing.T) {
	hr := NewCarbonHashRing()
	hr.SetReplicas(5)
	if hr.Replicas() != 5 {
		t.Error("HashRing replica setting error")
	}

	n := NewNode("a", 0, "a")
	hr.AddNode(n)
	if hr.String() != "[carbon: 1 nodes, 5 replicas, 5 ring members a:0=a]" {
		t.Error("HashRing string representation or AddNode()")
	}

	hr.RemoveNode(n)
	if hr.String() != "[carbon: 0 nodes, 5 replicas, 0 ring members ]" {
		t.Error("HashRing string representation or AddNode()")
	}
}

func TestGraphiteCompatible(t *testing.T) {
	hr := makeRing()

	repr := "[carbon: 39 nodes, 100 replicas, 3900 ring members graphite010-g5:0=a graphite010-g5:0=b graphite010-g5:0=c graphite011-g5:0=a graphite011-g5:0=b graphite011-g5:0=c graphite012-g5:0=a graphite012-g5:0=b graphite012-g5:0=c graphite013-g5:0=a graphite013-g5:0=b graphite013-g5:0=c graphite014-g5:0=a graphite014-g5:0=b graphite014-g5:0=c graphite015-g5:0=a graphite015-g5:0=b graphite015-g5:0=c graphite016-g5:0=a graphite016-g5:0=b graphite016-g5:0=c graphite017-g5:0=a graphite017-g5:0=b graphite017-g5:0=c graphite018-g5:0=a graphite018-g5:0=b graphite018-g5:0=c graphite-data019-g5:0=a graphite-data019-g5:0=b graphite-data019-g5:0=c graphite-data020-g5:0=a graphite-data020-g5:0=b graphite-data020-g5:0=c graphite-data021-g5:0=a graphite-data021-g5:0=b graphite-data021-g5:0=c graphite-data022-g5:0=a graphite-data022-g5:0=b graphite-data022-g5:0=c]"

	fmt.Printf("%s\n", hr)
	if hr.String() != repr {
		t.Error("Graphite Ring init failure")
	}

	// A known mapping of keys to nodes from Graphite's Python implementation
	expected := map[string]string{
		"1sec.mysql.db109-shard7-g5.4417.Com_help":                             "graphite015-g5:0=a",
		"10min.sar.disk_stats.app-test-57164838110fc9dc.sda.wr_sec":            "graphite010-g5:0=a",
		"1min.statsd.prod.intercom.corporate.challenge.participation.count_95": "graphite-data020-g5:0=b",
		"10min.sar.disk_stats.db047-shard35-g4.sda.rd_sec":                     "graphite010-g5:0=a",
	}

	for k, v := range expected {
		node := hr.GetNode(k)
		if node.String() != v {
			t.Errorf("Hash not compatible: %s !=> %s, rather %s", k, v, node)
		}
	}
}
