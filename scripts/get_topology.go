package main

import "archive/zip"
import "path/filepath"
import "fmt"
import "os"
import "strings"

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please provide the input directory")
		os.Exit(1)
	}

	dirpath := os.Args[1]

	pattern := fmt.Sprintf(filepath.Join(fmt.Sprintf("%v", dirpath), "*.zip"))

	zipfiles, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println("Error in filepath.Glob:", err)
		os.Exit(1)
	}

	indexerNodes := make(map[string]bool)
	queryNodes := make(map[string]bool)
	kvNodes := make(map[string]bool)
	otherNodes := make(map[string]bool)

	for _, zipfile := range zipfiles {
		fmt.Println("Procesing zipfile:", zipfile)
		otherNodes[zipfile] = true

		freader, err := zip.OpenReader(zipfile)
		if err != nil {
			fmt.Println("Error in zip.OpenReader:", err)
			os.Exit(1)
		}

		for _, f := range freader.Reader.File {
			if strings.Contains(f.FileHeader.Name, "query_pprof.log") {
				queryNodes[zipfile] = true
				delete(otherNodes, zipfile)
			}

			if strings.Contains(f.FileHeader.Name, "projector_pprof.log") {
				kvNodes[zipfile] = true
				delete(otherNodes, zipfile)
			}

			if strings.Contains(f.FileHeader.Name, "indexer_pprof.log") {
				indexerNodes[zipfile] = true
				delete(otherNodes, zipfile)
			}
		}
	}

	fmt.Println("Indexer Nodes:")
	for f, _ := range indexerNodes {
		fmt.Printf("%v ", f)
	}

	fmt.Println("")


	fmt.Println("Query Nodes:")
	for f, _ := range queryNodes {
		fmt.Printf("%v ", f)
	}

	fmt.Println("")


	fmt.Println("KV Nodes:")
	for f, _ := range kvNodes {
		fmt.Printf("%v ", f)
	}

	fmt.Println("")

	fmt.Println("Other Nodes:")
	for f, _ := range otherNodes {
		fmt.Printf("%v ", f)
	}

	fmt.Println("")
}

