package hashing

import (
	"testing"
)

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
		chr.AddNode(Node{v, ""})
	}

	return chr
}

func TestFNV1a64(t *testing.T) {
	data := map[string]uint64{
		"foobar":  0x85944171f73967e8,
		"changed": 0xdf38879af614707b,
		"5min.prod.dc06.graphite-web006-g6.kernel.net.netfilter.nf_conntrack_max": 0xdb7b58ef171eee9f,
	}

	for s, hash := range data {
		if Fnv1a64([]byte(s)) != hash {
			t.Errorf("FNV1a64 implementation does not match reference %s => %x",
				s, Fnv1a64([]byte(s)))
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
