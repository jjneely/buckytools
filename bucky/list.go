package main

import (
	"encoding/json"
	"fmt"
	//"io/ioutil"
	"log"
	"os"
)

// import . "github.com/jjneely/buckytools"

var listRegexMode bool

func init() {
	usage := "[options]"
	short := "List out matching metrics."
	long := `Dump to STDOUT text or JSON of matching metrics.  Without any
arguments / options this will list every metric in the cluster.

The default mode is to work with lists.  The arguments are a series of
one or more metric key names and the results will contain these key names
if they are present on the cluster/host.  If the first argument is a "-"
then read a JSON array from STDIN as our list of metrics.

Use -r to enable regular expression mode.  The first argument is a
regular expression.  If metrics names match they will be included in the
output.`

	c := NewCommand(listCommand, "list", usage, short, long)
	SetupHostname(c)
	SetupSingle(c)
	SetupJSON(c)

	c.Flag.BoolVar(&listRegexMode, "r", false,
		"Filter by a regular expression.")
}

func listAllMetrics(servers []string) []string {
	return []string{"foo", "bar"}
}

func listRegexMetrics(servers []string, regex string) []string {
	return listAllMetrics(servers)
}

func listSliceMetrics(servers []string, metrics []string) []string {
	return listAllMetrics(servers)
}

func listJSONMetrics(servers []string) []string {
	return listAllMetrics(servers)
}

// listCommand runs this subcommand.
func listCommand(c Command) int {
	servers := GetAllBuckyd()
	if servers == nil {
		return 1
	}

	var list []string
	if c.Flag.NArg() == 0 {
		list = listAllMetrics(servers)
	} else if listRegexMode && c.Flag.NArg() > 0 {
		list = listRegexMetrics(servers, c.Flag.Arg(0))
	} else if c.Flag.Arg(0) != "-" {
		list = listSliceMetrics(servers, c.Flag.Args())
	} else {
		list = listJSONMetrics(servers)
	}

	if JSONOutput {
		blob, err := json.Marshal(list)
		if err != nil {
			log.Printf("%s", err)
		} else {
			os.Stdout.Write(blob)
			os.Stdout.Write([]byte("\n"))
		}
	} else {
		for _, v := range list {
			fmt.Printf("%s\n", v)
		}
	}

	return 0
}
