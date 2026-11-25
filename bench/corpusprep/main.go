// Command corpusprep downloads public agent-trajectory datasets and extracts
// role-separated text records for the trace generator.
package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	root := filepath.Join("..", "..", ".data") // repository root when run from bench/corpusprep
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: corpusprep fetch|extract [data-dir]")
		os.Exit(2)
	}
	if len(os.Args) > 2 {
		root = os.Args[2]
	}
	var err error
	switch os.Args[1] {
	case "fetch":
		err = fetch(filepath.Join(root, "corpus-raw"))
	case "extract":
		err = extract(filepath.Join(root, "corpus-raw"), filepath.Join(root, "corpus"))
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "corpusprep:", err)
		os.Exit(1)
	}
}
