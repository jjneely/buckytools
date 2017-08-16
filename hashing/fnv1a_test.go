package hashing

import (
	"testing"
)

var FNV1aHashTestNodesWithInstanceName = []Node{
	{"graphite010-g5", 2003, "5"},
	{"graphite011-g5", 2003, "1"},
	{"graphite012-g5", 2003, "4"},
	{"graphite013-g5", 2003, "3"},
	{"graphite-data019-g5", 2003, "2"},
	{"graphite-data020-g5", 2003, "6"},
	{"graphite-data021-g5", 2003, "0"},
}

var FNV1aHashTestNodes = []Node{
	{"graphite010-g5", 2003, ""},
	{"graphite011-g5", 2003, ""},
	{"graphite012-g5", 2003, ""},
	{"graphite013-g5", 2003, ""},
	{"graphite014-g5", 2003, ""},
	{"graphite015-g5", 2003, ""},
	{"graphite016-g5", 2003, ""},
	{"graphite017-g5", 2003, ""},
	{"graphite018-g5", 2003, ""},
	{"graphite-data019-g5", 2003, ""},
	{"graphite-data020-g5", 2003, ""},
	{"graphite-data021-g5", 2003, ""},
}

func makeFNV1aTestCHR() *FNV1aHashRing {
	chr := NewFNV1aHashRing()
	for _, v := range FNV1aHashTestNodes {
		chr.AddNode(v)
	}

	return chr
}

func makeFNV1aTestCHRWithInstanceName() *FNV1aHashRing {
	chr := NewFNV1aHashRing()
	for _, v := range FNV1aHashTestNodesWithInstanceName {
		chr.AddNode(v)
	}
	return chr
}

func dumpFNV1aRing(t *testing.T, chr *FNV1aHashRing) {
	for _, v := range chr.ring {
		t.Logf("%d@%s", v.position, v.node)
	}
	t.Logf("Ring length: %d", len(chr.ring))
}

/*
cluster test fnv1a_ch replication 1
  graphite010-g5:2003
  graphite011-g5:2003
  graphite012-g5:2003
  graphite013-g5:2003
  graphite014-g5:2003
  graphite015-g5:2003
  graphite016-g5:2003
  graphite017-g5:2003
  graphite018-g5:2003
  graphite-data019-g5:2003
  graphite-data020-g5:2003
  graphite-data021-g5:2003
;
*/
func TestFNV1aCHR(t *testing.T) {
	chr := makeFNV1aTestCHR()
	t.Logf(chr.String())

	dumpFNV1aRing(t, chr)
	data := map[string]string{
		"foobar": "graphite010-g5",
		"suebob.foo.honey.i.shrunk.the.kids":                                      "graphite-data021-g5",
		"5min.prod.dc06.graphite-web006-g6.kernel.net.netfilter.nf_conntrack_max": "graphite012-g5",
	}

	for key, node := range data {
		n := chr.GetNode(key)
		if n.Server != node {
			t.Errorf("Hash not compatible with carbon-c-relay: %s => %s  Should be %s",
				key, n.Server, node)
		}
	}
}

/*
cluster test fnv1a_ch replication 1
  graphite010-g5:2003=5
  graphite011-g5:2003=1
  graphite012-g5:2003=4
  graphite013-g5:2003=3
  graphite-data019-g5:2003=2
  graphite-data020-g5:2003=6
  graphite-data021-g5:2003=0
;
*/
func TestFNV1aCHRInstance(t *testing.T) {
	chr := makeFNV1aTestCHRWithInstanceName()

	dumpFNV1aRing(t, chr)
	data := map[string]string{
		"foobar": "graphite013-g5",
		"suebob.foo.honey.i.shrunk.the.kids":                                      "graphite-data021-g5",
		"5min.prod.dc06.graphite-web006-g6.kernel.net.netfilter.nf_conntrack_max": "graphite-data019-g5",
	}

	for key, node := range data {
		n := chr.GetNode(key)
		if n.Server != node {
			t.Errorf("Hash not compatible with carbon-c-relay: %s => %s  Should be %s",
				key, n.Server, node)
		}
	}
}

func TestFNV1aNodeFormat(t *testing.T) {
	n := NewNode("graphite010-g5", 1234, "a")
	if r := n.FNV1aKeyValue(); r != "a" {
		t.Error("NewNode() did not produce the correct string representation (instance)")
		t.Logf("Expected \"%s\", returned \"%s\"", "a", r)
	}

	n = NewNode("graphite010-g5", 1234, "")
	if r := n.FNV1aKeyValue(); r != "graphite010-g5:1234" {
		t.Error("NewNode() did not produce the correct string representation (server:port)")
		t.Logf("Expected \"%s\", returned \"%s\"", "a", r)
	}
}
