//go:build !cgo

package main

func main() {
	println("cgo is disabled: enable cgo and install Faiss/WiredTiger headers to run this check.")
}
