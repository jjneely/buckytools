package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

import . "github.com/jjneely/buckytools"

func init() {
	usage := "[options]"
	short := "List out servers in the Graphite cluster."
	long := `Dump to STDOUT the servers that make up the consistent hash ring
of the Graphite cluster.  You will need to supply a HOST:PORT to locate one of the
buckyd daemons running on the Graphite cluster.  This command exists with an error
if a host in the cluster doesn't respond or has a different hash ring configuration
than the other members.  Using -s for a single host check tests if the given host
is alive.`

	c := NewCommand(serversCommand, "servers", usage, short, long)
	SetupHostname(c)
	SetupSingle(c)
	SetupJSON(c)
}

// getSingleHashRing connects to the given host and returns a slice of
// strings containing the host's configured hashring.  An error value is
// set if we could not retrieve the hashring information.
func GetSingleHashRing(hostport string) (*JSONRingType, error) {
	url := fmt.Sprintf("http://%s/hashring", hostport)
	server := GetHTTP()

	resp, err := server.Get(url)
	if err != nil {
		log.Printf("Error retrieving URL: %s", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Abort: /hashring API called returned: %s", resp.Status)
		return nil, fmt.Errorf("/hashring API called returned: %s", resp.Status)
	}

	ring := new(JSONRingType)
	blob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %s", err)
		return nil, err
	}
	err = json.Unmarshal(blob, &ring)
	if err != nil {
		log.Printf("Abort: %s", err)
		return nil, err
	}

	return ring, nil
}

func fetchRingWorker(host string, c chan *JSONRingType) {
	hostport := fmt.Sprintf("%s:%s", host, GetBuckyPort())
	ring, err := GetSingleHashRing(hostport)
	if err == nil {
		c <- ring
	} else {
		// Host in cluster is unhealthy
		ring = new(JSONRingType)
		ring.Name = host
		ring.Nodes = nil
		c <- ring
	}
}

// GetClusterRing returns a slice of JSONRingType structs.  One entry for
// each node in the Graphite cluster.
func GetClusterRing() ([]*JSONRingType, error) {
	master, err := GetSingleHashRing(HostPort)
	if err != nil {
		log.Printf("Abort: Cannot communicate with initial buckyd daemon.")
		return nil, err
	}

	hash := make(map[string]bool)
	for _, v := range master.Nodes {
		// split host/instance pair
		fields := strings.Split(v, ":")
		hash[fields[0]] = true
	}

	if !hash[master.Name] {
		log.Printf("Cluster inconsistent: Initial buckyd daemon not in hashring.")
		log.Printf("Hashring: %s", master.String())
		return nil, fmt.Errorf("Initial buckyd daemon not in hashring.")
	}

	comm := make(chan *JSONRingType, 10)
	// XXX: This queries the initial daemon twice.  Good?/Bad?
	for k := range hash {
		//log.Printf("Querying %s for hashring status.", k)
		go fetchRingWorker(k, comm)
	}

	results := make([]*JSONRingType, 0)
	for range hash {
		// read from comm the same number of times we write to it
		results = append(results, <-comm)
	}

	return results, nil
}

// IsHealthy will return true if the cluster ring data from GetClusterRing()
// represents a healthy and consistent cluster
func IsHealthy(ring []*JSONRingType) bool {
	// We compare each ring to the first one, checking for nil rings
	for i, v := range ring {
		if v.Nodes == nil {
			return false
		}
		// Order, host:instance pair, must be the same.  You configured
		// your cluster with a CM tool, right?
		if i > 0 {
			if len(v.Nodes) != len(ring[0].Nodes) {
				return false
			}
			for j, _ := range v.Nodes {
				if v.Nodes[j] != ring[0].Nodes[j] {
					return false
				}
			}
		}
	}

	return true
}

// GetRings returns a slice of JSONRingTypes and honors SingleHost
func GetRings() []*JSONRingType {
	var rings []*JSONRingType
	var err error

	if SingleHost {
		ring, err := GetSingleHashRing(HostPort)
		if err != nil {
			return nil
		}
		rings = make([]*JSONRingType, 0)
		rings = append(rings, ring)
	} else {
		rings, err = GetClusterRing()
		if err != nil {
			return nil
		}
	}

	return rings
}

// GetAllBuckyd returns a []string of all known buckyd daemons by checking
// the consistent hash rings.  Each string is in the format of HOST:PORT
func GetAllBuckyd() []string {
	rings := GetRings()
	if rings == nil {
		return nil
	}
	if !IsHealthy(rings) {
		log.Printf("Cluster is inconsistent. Use the servers command to investigate.")
		return nil
	}

	results := make([]string, 0)
	for _, r := range rings {
		results = append(results, fmt.Sprintf("%s:%s", r.Name, GetBuckyPort()))
	}
	return results
}

// serversCommand runs this subcommand.
func serversCommand(c Command) int {
	rings := GetRings()
	if rings == nil {
		return 1
	}

	if JSONOutput {
		blob, err := json.Marshal(rings)
		if err != nil {
			log.Printf("%s", err)
		} else {
			os.Stdout.Write(blob)
			os.Stdout.Write([]byte("\n"))
		}
	} else {
		for _, v := range rings {
			fmt.Printf("Host %s reports the following hash ring:\n", v.Name)
			for _, node := range v.Nodes {
				fmt.Printf("\t%s\n", node)
			}
		}
	}
	if !IsHealthy(rings) {
		log.Printf("Cluster is inconsistent.")
		return 1
	}

	return 0
}
