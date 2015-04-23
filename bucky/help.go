package main

import (
	"fmt"
)

func init() {
	usage := "[sub-command]"
	short := "Provide help for each sub-command."
	long := "Provide help and details for running each bucky sub-command."

	NewCommand(helpCommand, "help", usage, short, long)
	NewCommand(helpCommand, "--help", usage, short, long)
	NewCommand(helpCommand, "-h", usage, short, long)
}

func helpCommand(cmd Command) int {
	fmt.Printf("Bucky is a CLI for managing consistent hashing Graphite clusters.\n")
	fmt.Printf("\n")

	for _, c := range commands {
		fmt.Printf("%s %s\n", c.Name, c.Usage)
		fmt.Printf("\t%s\n", c.Short)
	}

	return 0
}
