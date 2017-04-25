package main

import (
	"flag"
	"fmt"
	//"log"
	"os"
	"sort"
	"strings"
)

import . "github.com/jjneely/buckytools"

// We use STDIN and STDOUT as much as possible for handing lists, and
// other data.  Status, errors, and other data not related to pushing
// metrics around is handled via the log interface on STDERR.

type Command struct {
	// Name is the subcommand name
	Name string

	// Run the command.  The slice argv holds the command line
	// args after the sub-command.  Sub-commands must return their
	// OS return code.
	Run func(c Command) int

	// Usage is the one line usage
	Usage string

	// Short is a one line help description
	Short string

	// Long help description
	Long string

	// Flag holds the FlagSet that the subcommand has setup for itself
	Flag *flag.FlagSet
}

type CommandList []Command

// All the registered commands
var commands CommandList = make(CommandList, 0)

// Commands are sorted for output sanity...
func (c CommandList) Len() int {
	return len(c)
}

func (c CommandList) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c CommandList) Less(i, j int) bool {
	return c[i].Name < c[j].Name
}

// NewCommand is called by the init() function in sub-command files to
// register themselves at startup.  After registering itself, the sub-
// command code may then setup flags.
func NewCommand(run func(c Command) int, name, usage, short, long string) Command {
	c := Command{}
	c.Name = name
	c.Usage = usage
	c.Short = short
	c.Long = long
	c.Run = run

	c.Flag = flag.NewFlagSet(name, flag.ExitOnError)
	commands = append(commands, c)
	return c
}

func usage() {
	t := []string{
		"%s <sub-command> [options]\n",
		"Copyright 2015 - 2017 42 Lines, Inc\n",
		"Original Author: Jack Neely <jjneely@42lines.net>\n",
		"Version: %s\n\n",
		"\tBucky is a CLI designed to work with large consistent hashing\n",
		"\tGraphite clusters that have the buckyd daemon installed.  Sub-\n",
		"\tcommands will allow you to perform high level operations such\n",
		"\tas backups and restores of specific metrics, backfilling, and\n",
		"\teven rebalancing.\n\n",
		"\tUse the \"help\" sub-command for available commands.\n\n",
	}

	fmt.Fprintf(os.Stderr, strings.Join(t, ""), os.Args[0], Version)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	sort.Sort(commands)
	for _, c := range commands {
		if c.Name == os.Args[1] {
			c.Flag.Parse(os.Args[2:])
			os.Exit(c.Run(c))
		}
	}

	usage()
}
