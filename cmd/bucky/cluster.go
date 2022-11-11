package main

import (
	"fmt"
	"log"
	"net"

	"github.com/go-graphite/buckytools/hashing"
)

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
func GetClusterConfig(masterHostport string) (*ClusterConfig, error) {
	if Cluster != nil {
		return Cluster, nil
	}

	var err error
	Cluster, err = newCluster(masterHostport)
	return Cluster, err
}

func newCluster(masterHostport string) (*ClusterConfig, error) {
	master, err := GetSingleHashRing(masterHostport)
	if err != nil {
		log.Printf("Abort: Cannot communicate with initial buckyd daemon.")
		return nil, err
	}

	_, port, err := net.SplitHostPort(masterHostport)
	if err != nil {
		log.Printf("Abort: Invalid host:port representation: %s", masterHostport)
		return nil, err
	}

	cluster := new(ClusterConfig)
	cluster.Port = port
	cluster.Servers = make([]string, 0)
	switch master.Algo {
	case "carbon":
		cluster.Hash = hashing.NewCarbonHashRing()
	case "fnv1a":
		cluster.Hash = hashing.NewFNV1aHashRing()
	case "jump_fnv1a":
		cluster.Hash = hashing.NewJumpHashRing(master.Replicas)
	default:
		log.Printf("Unknown consistent hash algorithm: %s", master.Algo)
		return nil, fmt.Errorf("Unknown consistent hash algorithm: %s", master.Algo)
	}

	for _, v := range master.Nodes {
		cluster.Hash.AddNode(v)
		cluster.Servers = append(cluster.Servers, v.Server)
	}

	members := make([]*hashing.JSONRingType, 0)
	for _, srv := range cluster.Servers {
		if srv == master.Name {
			// Don't query the initial daemon again
			continue
		}
		host := fmt.Sprintf("%s:%s", srv, cluster.Port)
		member, err := GetSingleHashRing(host)
		if err != nil {
			log.Printf("Cluster unhealthy: %s: %s", host, err)
		}
		members = append(members, member)
	}

	cluster.Healthy = isHealthy(master, members)
	return cluster, nil
}

// isHealthy will return true if the cluster ring data represents
// a healthy cluster.  The master is the initial buckyd daemon we
// built the list from.  The ring is a slice of ring objects from each
// server in the cluster except the initial buckyd daemon.
func isHealthy(master *hashing.JSONRingType, ring []*hashing.JSONRingType) bool {
	var masterInRing bool
	for _, member := range ring {
		if member.Name == master.Name {
			masterInRing = true
			break
		}
	}

	if masterInRing {
		log.Printf("master is part of ring, so master expects %d nodes and we expect ring %d", len(master.Nodes), len(ring))
	}

	// XXX: Take replicas into account
	if !masterInRing && len(master.Nodes) != len(ring)+1 ||
		masterInRing && len(master.Nodes) != len(ring) {
		log.Printf("wrong number of nodes compared to expectation; cluster is inconsistent")
		return false
	}

	// We compare each ring to the first one
	for _, v := range ring {
		// Order, host:instance pair, must be the same.  You configured
		// your cluster with a CM tool, right?
		if master.Algo != v.Algo {
			log.Printf("member %s: algo %s does not match master algo %s", v.Name, v.Algo, master.Algo)
			return false
		}
		if len(v.Nodes) != len(master.Nodes) {
			log.Printf("member %s: node count %d does not match master node count %d", v.Name, len(v.Nodes), len(master.Nodes))
			return false
		}
		for i, vv := range v.Nodes {
			if !hashing.NodeCmp(master.Nodes[i], vv) {
				log.Printf("member %s: node %d %s does not match master node %d %s", v.Name, i, vv.Server, i, master.Nodes[i].Server)
				return false
			}
		}
	}

	return true
}
