package hashing

import (
	"sort"
	"strings"
	"testing"
)

type nodesSlice [][]string

var jumpHashTestNodesWithInstanceName = nodesSlice{
	{"graphite010-g5", "5"},
	{"graphite011-g5", "1"},
	{"graphite012-g5", "4"},
	{"graphite013-g5", "3"},
	{"graphite-data019-g5", "2"},
	{"graphite-data020-g5", "6"},
	{"graphite-data021-g5", "0"},
}

//makes nodesSlice sortable
func (c nodesSlice) Len() int           { return len(c) }
func (c nodesSlice) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c nodesSlice) Less(i, j int) bool { return strings.Compare(c[i][1], c[j][1]) == -1 }

var jumpHashTestNodes = []string{
	"graphite010-g5",
	"graphite011-g5",
	"graphite012-g5",
	"graphite013-g5",
	"graphite014-g5",
	"graphite015-g5",
	"graphite016-g5",
	"graphite017-g5",
	"graphite018-g5",
	"graphite-data019-g5",
	"graphite-data020-g5",
	"graphite-data021-g5",
	"graphite-data022-g5",
	"graphite-data023-g5",
	"graphite-data024-g5",
	"graphite-data025-g5",
	"graphite-data026-g5",
	"graphite-data027-g5",
	"graphite-data028-g5",
	"graphite-data029-g5",
	"graphite-data030-g5",
	"graphite-data031-g5",
	"graphite-data032-g5",
	"graphite-data033-g5",
	"graphite-data034-g5",
	"graphite-data035-g5",
	"graphite-data036-g5",
	"graphite-data037-g5",
	"graphite-data038-g5",
	"graphite-data039-g5",
	"graphite-data040-g5",
	"graphite-data041-g5",
	"graphite-data042-g5",
	"graphite-data043-g5",
	"graphite-data044-g5",
	"graphite-data045-g5",
	"graphite-data046-g5",
	"graphite-data047-g5",
	"graphite-data048-g5",
	"graphite-data049-g5",
	"graphite-data050-g5",
	"graphite-data051-g5",
	"graphite-data052-g5",
	"graphite-data053-g5",
	"graphite-data054-g5",
}

func makeJumpTestCHR(r int) *JumpHashRing {
	chr := NewJumpHashRing(r)
	for _, v := range jumpHashTestNodes {
		chr.AddNode(Node{v, 0, ""})
	}

	return chr
}

func makeJumpTestCHRWithInstanceName(r int) *JumpHashRing {
	chr := NewJumpHashRing(r)
	for _, v := range jumpHashTestNodesWithInstanceName {
		chr.AddNode(Node{v[0], 0, v[1]})
	}
	return chr
}

func TestFNV1a32(t *testing.T) {
	data := map[string]uint32{
		"252113b5-a6c4-4661-8007-a583d6aa7130": 0xbe564b20,
		"0aac2773-f766-478e-867f-ba4cca94e524": 0x15a526a8,
		"6fdcba3a-dc5b-419b-a2bc-b4670b8e85fd": 0xbf5bd512,
	}

	for s, hash := range data {
		if Fnv1a32([]byte(s)) != hash {
			t.Errorf("FNV1a32 implementation does not match reference %s => %x, expected %x",
				s, Fnv1a32([]byte(s)), hash)
		}
	}
}

func TestFNV1a64(t *testing.T) {
	data := map[string]uint64{
		"foobar":  0x85944171f73967e8,
		"changed": 0xdf38879af614707b,
		"5min.prod.dc06.graphite-web006-g6.kernel.net.netfilter.nf_conntrack_max": 0xdb7b58ef171eee9f,
	}

	for s, hash := range data {
		if Fnv1a64([]byte(s)) != hash {
			t.Errorf("FNV1a64 implementation does not match reference %s => %x, expected %x",
				s, Fnv1a64([]byte(s)), hash)
		}
	}
}

func TestOutOfRange(t *testing.T) {
	chr := makeJumpTestCHR(1)
	for _, v := range jumpHashTestNodes {
		k := Fnv1a64([]byte(v))
		i := Jump(k, len(chr.ring))
		if i < 0 || i >= len(chr.ring) {
			t.Errorf("Jump hash returned out of bounds bucket %s => %d", v, i)
		}
	}
}

func TestJumpCHR(t *testing.T) {
	chr := makeJumpTestCHR(1)
	t.Logf(chr.String())

	data := map[string]string{
		"foobar": "graphite-data043-g5",
		"suebob.foo.honey.i.shrunk.the.kids":                                      "graphite-data048-g5",
		"5min.prod.dc06.graphite-web006-g6.kernel.net.netfilter.nf_conntrack_max": "graphite014-g5",
	}

	for key, node := range data {
		n := chr.GetNode(key)
		if n.Server != node {
			t.Errorf("Hash not compatible with carbon-c-relay: %s => %s  Should be %s",
				key, n.Server, node)
		}
	}
}

func TestJumpCHRInstanceOrder(t *testing.T) {
	chr := makeJumpTestCHRWithInstanceName(1)
	t.Logf(chr.String())
	//Order the slice of nodes by instance name
	oNodes := make(nodesSlice, len(jumpHashTestNodesWithInstanceName))
	copy(oNodes, jumpHashTestNodesWithInstanceName)
	sort.Sort(oNodes)
	//Check that the nodes in the ring are correctly ordered by instance name
	for index, n := range chr.ring {
		if n.Server != oNodes[index][0] {
			t.Errorf("Wrong order in jump hash ring: expected to find: '%s', found %s", n.Server, oNodes[index][0])
		}
	}
}

func TestJumpCHRNoInstanceOrder(t *testing.T) {
	chr := makeJumpTestCHR(1)

	for i, n := range chr.ring {
		if n.Server != jumpHashTestNodes[i] {
			t.Errorf("Wrong order in jump hash ring with no instances: expected %s and found %s", jumpHashTestNodes[i], n.Server)
		}
	}
}
