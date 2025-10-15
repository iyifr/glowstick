package main

import (
	"fmt"

	wt "huedb/wiredtiger"
)

func main() {
	if err := wt.RunExample(); err != nil {
		fmt.Println("example error:", err)
	}
}
