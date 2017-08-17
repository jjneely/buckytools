package main

import (
	"fmt"
	"log"
	"net"
)

import "github.com/jjneely/buckytools/hashing"

type ClusterConfig struct {
	// Port is the port remote buckyd daemons listen on
	Port string

	// Servers is a list of all bucky server hostnames.  This does not
	// include port information
	Servers []string

	// Hash is a HashRing interface that implements the hashring algorithm
	// that the cluster is using
	Hash hashing.HashRing

	// Healthy is true if the cluster configuration represents a Healthy
	// cluster
	Healthy bool
}

// Cluster is the working and cached cluster configuration
var Cluster *ClusterConfig

func (c *ClusterConfig) HostPorts() []string {
	if c == nil {
		return nil
	}
	ret := make([]string, 0)
	for _, v := range c.Servers {
		ret = append(ret, fmt.Sprintf("%s:%s", v, c.Port))
	}
	return ret
}

// GetClusterConfig returns either the cached ClusterConfig object or
// builds it if needed.  The initial HOST:PORT of the buckyd daemon
// must be given.
func GetClusterConfig(hostport string) (*ClusterConfig, error) {
	if Cluster != nil {
		return Cluster, nil
	}

	master, err := GetSingleHashRing(hostport)
	if err != nil {
		log.Printf("Abort: Cannot communicate with initial buckyd daemon.")
		return nil, err
	}

	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		log.Printf("Abort: Invalid host:port representation: %s", hostport)
		return nil, err
	}

	Cluster = new(ClusterConfig)
	Cluster.Port = port
	Cluster.Servers = make([]string, 0)
	switch master.Algo {
	case "carbon":
		Cluster.Hash = hashing.NewCarbonHashRing()
	case "fnv1a":
		Cluster.Hash = hashing.NewFNV1aHashRing()
	case "jump_fnv1a":
		Cluster.Hash = hashing.NewJumpHashRing(master.Replicas)
	default:
		log.Printf("Unknown consistent hash algorithm: %s", master.Algo)
		return nil, fmt.Errorf("Unknown consistent hash algorithm: %s", master.Algo)
	}

	for _, v := range master.Nodes {
		Cluster.Hash.AddNode(v)
		Cluster.Servers = append(Cluster.Servers, v.Server)
	}

	members := make([]*hashing.JSONRingType, 0)
	for _, srv := range Cluster.Servers {
		if srv == master.Name {
			// Don't query the initial daemon again
			continue
		}
		host := fmt.Sprintf("%s:%s", srv, Cluster.Port)
		member, err := GetSingleHashRing(host)
		if err != nil {
			log.Printf("Cluster unhealthy: %s: %s", host, err)
		}
		members = append(members, member)
	}

	Cluster.Healthy = isHealthy(master, members)
	return Cluster, nil
}

// isHealthy will return true if the cluster ring data represents
// a healthy cluster.  The master is the initial buckyd daemon we
// built the list from.  The ring is a slice of ring objects from each
// server in the cluster except the initial buckyd daemon.
func isHealthy(master *hashing.JSONRingType, ring []*hashing.JSONRingType) bool {
	// XXX: Take replicas into account
	// The initial buckyd daemon isn't in the ring, so we need to add 1
	// to the length.
	if len(master.Nodes) != len(ring)+1 {
		return false
	}

	// We compare each ring to the first one
	for _, v := range ring {
		// Order, host:instance pair, must be the same.  You configured
		// your cluster with a CM tool, right?
		if master.Algo != v.Algo {
			return false
		}
		if len(v.Nodes) != len(master.Nodes) {
			return false
		}
		for i, v := range v.Nodes {
			if !hashing.NodeCmp(master.Nodes[i], v) {
				return false
			}
		}
	}

	return true
}
