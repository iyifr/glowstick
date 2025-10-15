package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/valyala/fasthttp"
	"go.mongodb.org/mongo-driver/bson"
)

func helloHandler(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("Hello world")
}

func bsonHandler(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) != "POST" {
		ctx.Error("Only POST allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	bsonBytes := ctx.PostBody()

	var raw bson.Raw = bsonBytes

	bsonErr := raw.Validate()

	if bsonErr != nil {
		ctx.Error("Bad BSON", fasthttp.StatusBadRequest)
	}

	var elements, elemErr = raw.Elements()

	if elemErr != nil {
		fmt.Println("Element error")
	}

	// Map through elements and print value
	for _, elem := range elements {
		key := elem.Key()
		val := elem.Value()
		valType := elem.Value().Type
		fmt.Printf("\nKey: %s\nValue: %v\nValue Type:%s\n", key, val, valType)

		if valType == bson.TypeArray {
			fmt.Println("Value is an array")
			naturalArr, err := val.Array().Elements()

			if err != nil {
				fmt.Printf("Error decoding array: %v\n", err)
			} else {
				for i, element := range naturalArr {
					fmt.Printf("  [%d]: %v\n", i, element)
				}
			}
		}
	}

	var doc interface{}
	err := bson.Unmarshal(bsonBytes, &doc)
	if err != nil {
		ctx.Error("Failed to decode BSON", fasthttp.StatusBadRequest)
		return
	}

	jsonBytes, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		ctx.Error("Failed to encode JSON", fasthttp.StatusInternalServerError)
		return
	}
	err = os.WriteFile("output.json", jsonBytes, 0644)
	if err != nil {
		ctx.Error("Failed to write file", fasthttp.StatusInternalServerError)
		return
	}

	ctx.WriteString("BSON saved as JSON to output.json")
}
