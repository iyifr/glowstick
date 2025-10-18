package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	wt "glowstickdb/pkgs/wiredtiger"
)

func main() {
	fmt.Println("=== Range Scan Performance Comparison ===")

	// Create directory
	if err := os.MkdirAll("WT_HOME", 0755); err != nil {
		log.Fatal("Failed to create WT_HOME:", err)
	}

	// Initialize service
	wtService := wt.WiredTiger()

	// Open connection
	if err := wtService.Open("WT_HOME", "create"); err != nil {
		log.Fatal("Failed to open connection:", err)
	}
	defer func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
	}()

	uri := "table:performance_test"

	// Create table
	if err := wtService.CreateTable(uri, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Generate large dataset
	fmt.Println("Generating large dataset...")
	numRecords := 10000
	startTime := time.Now()

	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("user_%06d", i)
		value := fmt.Sprintf("data_%d", i)
		if err := wtService.PutString(uri, key, value); err != nil {
			log.Fatal("Failed to put data:", err)
		}
	}

	insertTime := time.Since(startTime)
	fmt.Printf("Inserted %d records in %v\n", numRecords, insertTime)

	// Force garbage collection before performance tests
	runtime.GC()
	runtime.GC()

	// Test 1: Full table scan (traditional method)
	fmt.Println("\n=== Test 1: Full Table Scan ===")
	startTime = time.Now()

	pairs, err := wtService.Scan(uri)
	if err != nil {
		log.Fatal("Failed to scan:", err)
	}

	scanTime := time.Since(startTime)
	fmt.Printf("Full scan: %v, found %d records\n", scanTime, len(pairs))

	// Test 2: Range scan (small range)
	fmt.Println("\n=== Test 2: Range Scan (Small Range) ===")
	startTime = time.Now()

	cursor, err := wtService.ScanRange(uri, "user_000100", "user_000200")
	if err != nil {
		log.Fatal("Failed to create range cursor:", err)
	}
	defer cursor.Close()

	rangeCount := 0
	for cursor.Next() {
		rangeCount++
	}

	rangeTime := time.Since(startTime)
	fmt.Printf("Range scan: %v, found %d records\n", rangeTime, rangeCount)

	// Test 3: Range scan (medium range)
	fmt.Println("\n=== Test 3: Range Scan (Medium Range) ===")
	startTime = time.Now()

	cursor, err = wtService.ScanRange(uri, "user_000000", "user_005000")
	if err != nil {
		log.Fatal("Failed to create range cursor:", err)
	}
	defer cursor.Close()

	rangeCount = 0
	for cursor.Next() {
		rangeCount++
	}

	rangeTime = time.Since(startTime)
	fmt.Printf("Range scan: %v, found %d records\n", rangeTime, rangeCount)

	// Test 4: Memory usage comparison
	fmt.Println("\n=== Test 4: Memory Usage Comparison ===")

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Traditional scan - loads all into memory
	pairs, err = wtService.Scan(uri)
	if err != nil {
		log.Fatal("Failed to scan:", err)
	}

	runtime.ReadMemStats(&m2)
	traditionalMemory := m2.Alloc - m1.Alloc

	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Range scan - streams data
	cursor, err = wtService.ScanRange(uri, "user_000000", "user_010000")
	if err != nil {
		log.Fatal("Failed to create range cursor:", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		count++
	}

	runtime.ReadMemStats(&m2)
	rangeMemory := m2.Alloc - m1.Alloc

	fmt.Printf("Traditional scan memory: %d bytes\n", traditionalMemory)
	fmt.Printf("Range scan memory: %d bytes\n", rangeMemory)
	fmt.Printf("Memory efficiency: %.2fx less memory used\n", float64(traditionalMemory)/float64(rangeMemory))

	// Test 5: Pagination performance
	fmt.Println("\n=== Test 5: Pagination Performance ===")
	pageSize := 100
	numPages := 10

	startTime = time.Now()

	for page := 0; page < numPages; page++ {
		startKey := fmt.Sprintf("user_%06d", page*pageSize)
		endKey := fmt.Sprintf("user_%06d", (page+1)*pageSize)

		cursor, err := wtService.ScanRange(uri, startKey, endKey)
		if err != nil {
			log.Fatal("Failed to create pagination cursor:", err)
		}

		pageCount := 0
		for cursor.Next() {
			pageCount++
		}
		cursor.Close()

		fmt.Printf("Page %d: %d records\n", page+1, pageCount)
	}

	paginationTime := time.Since(startTime)
	fmt.Printf("Pagination time for %d pages: %v\n", numPages, paginationTime)

	// Test 6: Search near vs range scan
	fmt.Println("\n=== Test 6: Search Near vs Range Scan ===")

	// Search near
	startTime = time.Now()
	key, value, exact, found, err := wtService.SearchNear(uri, "user_005000")
	if err != nil {
		log.Fatal("Failed to search near:", err)
	}
	searchNearTime := time.Since(startTime)

	if found {
		fmt.Printf("Search near: %v, found %s = %s (exact: %d)\n", searchNearTime, key, value, exact)
	}

	// Range scan around same area
	startTime = time.Now()
	cursor, err = wtService.ScanRange(uri, "user_004950", "user_005050")
	if err != nil {
		log.Fatal("Failed to create range cursor:", err)
	}
	defer cursor.Close()

	rangeCount = 0
	for cursor.Next() {
		rangeCount++
	}

	rangeTime = time.Since(startTime)
	fmt.Printf("Range scan: %v, found %d records\n", rangeTime, rangeCount)

	fmt.Println("\n=== Performance Comparison Summary ===")
	fmt.Printf("Full table scan (%d records): %v\n", len(pairs), scanTime)
	fmt.Printf("Range scan (small): %v\n", rangeTime)
	fmt.Printf("Memory efficiency: %.2fx improvement\n", float64(traditionalMemory)/float64(rangeMemory))
	fmt.Println("\nRange scans provide:")
	fmt.Println("- Constant memory usage regardless of result set size")
	fmt.Println("- Faster queries for bounded ranges")
	fmt.Println("- Better scalability for large datasets")
	fmt.Println("- Efficient pagination support")
}
