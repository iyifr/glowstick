package main

import (
	"fmt"

	wt "glowstickdb/pkgs/wiredtiger"
)

func main() {
	if err := wt.RunExample(); err != nil {
		fmt.Println("example error:", err)
	}
}
