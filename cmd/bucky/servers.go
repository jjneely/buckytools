package main

import (
	"fmt"
	"log"
)

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
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)
}

// serversCommand runs this subcommand.
func serversCommand(c Command) int {
	_, err := GetClusterConfig(HostPort)
	if err != nil {
		log.Print(err)
		return 1
	}

	fmt.Printf("Buckd daemons are using port: %s\n", Cluster.Port)
	fmt.Printf("Hashing algorithm: %v\n", Cluster.Hash)
	fmt.Printf("Number of replicas: %d\n", Cluster.Hash.Replicas())
	fmt.Printf("Found these servers:\n")

	for _, v := range Cluster.Servers {
		fmt.Printf("\t%s\n", v)
	}
	fmt.Printf("\nIs cluster healthy: %v\n", Cluster.Healthy)
	if !Cluster.Healthy {
		log.Printf("Cluster is inconsistent.")
		return 1
	}

	return 0
}
