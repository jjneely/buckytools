package main

// Lifted from: https://gist.github.com/albrow/5882501
// Modified from its original

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

// askForConfirmation uses Scanln to parse user input. A user must type in "yes" or "no" and
// then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user. Typically, you should use fmt to print out a question
// before calling askForConfirmation. E.g. fmt.Println("WARNING: Are you sure? (yes/no)")
func askForConfirmation(prompt string) bool {
	// Stdin may be redirected, read from console directly
	console, _ := os.Open("/dev/tty")
	reader := bufio.NewReader(console)

	if prompt != "" {
		fmt.Printf("%s ", prompt)
	}
	for {
		response, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading confirmation, assuming no: %s", err)
			return false
		}
		response = strings.TrimSpace(response)
		okayResponses := []string{"y", "Y", "yes", "Yes", "YES"}
		nokayResponses := []string{"n", "N", "no", "No", "NO"}

		if containsString(okayResponses, response) {
			return true
		} else if containsString(nokayResponses, response) {
			return false
		} else {
			fmt.Printf("Please type \"Yes\" or \"No\" and then press enter: ")
		}
	}
}

// You might want to put the following two functions in a separate utility package.

// posString returns the first index of element in slice.
// If slice does not contain element, returns -1.
func posString(slice []string, element string) int {
	for index, elem := range slice {
		if elem == element {
			return index
		}
	}
	return -1
}

// containsString returns true iff slice contains element
func containsString(slice []string, element string) bool {
	return !(posString(slice, element) == -1)
}
