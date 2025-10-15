package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/bson"
)

func makeBSON() {
	inFile := flag.String("in", "input.json", "Input JSON file")
	outFile := flag.String("out", "output.bson", "Output BSON file")
	flag.Parse()

	jsonBytes, err := os.ReadFile(*inFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read input file: %v\n", err)
		os.Exit(1)
	}

	var obj any
	if err := json.Unmarshal(jsonBytes, &obj); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid JSON: %v\n", err)
		os.Exit(1)
	}

	bsonBytes, err := bson.Marshal(obj)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode BSON: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(*outFile, bsonBytes, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write BSON file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("BSON file created: %s\n", *outFile)
}

func main() {
	makeBSON()
}
