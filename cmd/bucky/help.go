package main

import (
	"fmt"
	"os"
)

import . "github.com/jjneely/buckytools"

func init() {
	usage := "[sub-command]"
	short := "Provide help for each sub-command."
	long := `Provide help and details for running each bucky sub-command.
Specify a sub-command as an argument for detailed help information for that
command.`

	NewCommand(helpCommand, "help", usage, short, long)
	NewCommand(helpCommand, "--help", usage, short, long)
	NewCommand(helpCommand, "-h", usage, short, long)
}

// shortHelp displays a table of contents of the installed sub-commands.
func shortHelp() {
	usage()

	for _, c := range commands {
		fmt.Printf("%s %s\n", c.Name, c.Usage)
		fmt.Printf("\t%s\n", c.Short)
	}
}

// longHelp needs the Command struct for the command to print the detailed
// help texts for.
func longHelp(c Command) {
	fmt.Printf("Buckytools Help.\tSubcommand: %s\tVersion: %s\n\n",
		c.Name, Version)
	fmt.Printf("Usage: %s %s %s\n\n", os.Args[0], c.Name, c.Usage)

	// XXX: We should only print out flag information if flags are defined
	c.Flag.PrintDefaults()

	fmt.Printf("%s\n", c.Long)
}

func helpCommand(cmd Command) int {
	if cmd.Flag.NArg() > 0 {
		for _, c := range commands {
			if c.Name == cmd.Flag.Arg(0) {
				longHelp(c)
				return 0
			}
		}
		fmt.Printf("Unknown sub-command.\n")
		os.Exit(1)
	} else {
		shortHelp()
	}

	return 0
}
