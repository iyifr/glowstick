package wiredtiger

import (
	"fmt"
	"os"
)

func RunExample() error {
	fmt.Println("=== WiredTiger Example ===")

	// Create directory with error handling
	fmt.Println("Creating WT_HOME directory...")
	if err := os.MkdirAll("WT_HOME", 0755); err != nil {
		return fmt.Errorf("failed to create WT_HOME: %w", err)
	}

	// Initialize service
	fmt.Println("Initializing WiredTiger service...")
	wt := WiredTiger()

	// Open connection with debug output
	fmt.Println("Opening WiredTiger connection...")
	if err := wt.Open("WT_HOME", "create"); err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	fmt.Println("✓ Connection opened successfully")
	defer func() {
		fmt.Println("Closing connection...")
		if err := wt.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		} else {
			fmt.Println("✓ Connection closed")
		}
	}()

	uri := "table:example"

	// Create table
	fmt.Printf("Creating table: %s...\n", uri)
	if err := wt.CreateTable(uri, "key_format=S,value_format=S"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Println("✓ Table created")

	// Insert data
	fmt.Println("\nInserting data...")
	entries := map[string]string{
		"alpha":   "one",
		"beta":    "two",
		"gamma":   "three",
		"delta":   "four",
		"epsilon": "five",
	}

	for k, v := range entries {
		if err := wt.PutString(uri, k, v); err != nil {
			return fmt.Errorf("failed to put %s: %w", k, err)
		}
		fmt.Printf("  Put: %s = %s\n", k, v)
	}
	fmt.Println("✓ Data inserted")

	// Get specific values
	fmt.Println("\nRetrieving specific values...")
	for _, key := range []string{"alpha", "gamma", "nonexistent"} {
		if v, ok, err := wt.GetString(uri, key); err != nil {
			return fmt.Errorf("failed to get %s: %w", key, err)
		} else if ok {
			fmt.Printf("  %s: %s\n", key, v)
		} else {
			fmt.Printf("  %s: (not found)\n", key)
		}
	}

	// Check existence
	fmt.Println("\nChecking key existence...")
	for _, key := range []string{"beta", "missing"} {
		if exists, err := wt.Exists(uri, key); err != nil {
			return fmt.Errorf("failed to check existence of %s: %w", key, err)
		} else {
			fmt.Printf("  %s exists: %v\n", key, exists)
		}
	}

	// Search near
	fmt.Println("\nTesting search_near...")
	if k, v, exact, ok, err := wt.SearchNear(uri, "charlie"); err != nil {
		return fmt.Errorf("failed to search_near: %w", err)
	} else if ok {
		exactStr := map[int]string{-1: "less than", 0: "exact match", 1: "greater than"}[exact]
		fmt.Printf("  Searched for 'charlie', found: %s = %s (%s)\n", k, v, exactStr)
	}

	// Scan all records
	fmt.Println("\nScanning all records...")
	if rows, err := wt.Scan(uri); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	} else {
		fmt.Printf("  Found %d records:\n", len(rows))
		for i, r := range rows {
			fmt.Printf("    [%d] %s -> %s\n", i+1, r.Key, r.Value)
		}
	}

	// Delete records
	fmt.Println("\nDeleting records...")
	toDelete := []string{"alpha", "beta"}
	for _, key := range toDelete {
		if err := wt.DeleteString(uri, key); err != nil {
			return fmt.Errorf("failed to delete %s: %w", key, err)
		}
		fmt.Printf("  Deleted: %s\n", key)
	}
	fmt.Println("✓ Records deleted")

	// Verify deletion
	fmt.Println("\nVerifying deletion...")
	if rows, err := wt.Scan(uri); err != nil {
		return fmt.Errorf("failed to scan after deletion: %w", err)
	} else {
		fmt.Printf("  Remaining records: %d\n", len(rows))
		for _, r := range rows {
			fmt.Printf("    %s -> %s\n", r.Key, r.Value)
		}
	}

	fmt.Println("\n=== Example completed successfully ===")
	return nil
}

func RunBinarExample() error {
	fmt.Println("=== WiredTiger Example ===")

	// Create directory with error handling
	fmt.Println("Creating WT_HOME directory...")
	if err := os.MkdirAll("WT_HOME", 0755); err != nil {
		return fmt.Errorf("failed to create WT_HOME: %w", err)
	}

	// Initialize service
	fmt.Println("Initializing WiredTiger service...")
	wt := WiredTiger()

	// Open connection with debug output
	fmt.Println("Opening WiredTiger connection...")
	if err := wt.Open("WT_HOME", "create"); err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	fmt.Println("✓ Connection opened successfully")
	defer func() {
		fmt.Println("Closing connection...")
		if err := wt.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		} else {
			fmt.Println("✓ Connection closed")
		}
	}()

	uri := "table:binary_example"

	// Create table
	fmt.Printf("Creating table: %s...\n", uri)
	if err := wt.CreateTable(uri, "key_format=S,value_format=S"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Println("✓ Table created")

	// Insert data
	fmt.Println("\nInserting data...")
	entries := map[string]string{
		"alpha":   "one",
		"beta":    "two",
		"gamma":   "three",
		"delta":   "four",
		"epsilon": "five",
	}

	for k, v := range entries {
		if err := wt.PutString(uri, k, v); err != nil {
			return fmt.Errorf("failed to put %s: %w", k, err)
		}
		fmt.Printf("  Put: %s = %s\n", k, v)
	}
	fmt.Println("✓ Data inserted")

	// Get specific values
	fmt.Println("\nRetrieving specific values...")
	for _, key := range []string{"alpha", "gamma", "nonexistent"} {
		if v, ok, err := wt.GetString(uri, key); err != nil {
			return fmt.Errorf("failed to get %s: %w", key, err)
		} else if ok {
			fmt.Printf("  %s: %s\n", key, v)
		} else {
			fmt.Printf("  %s: (not found)\n", key)
		}
	}

	// Check existence
	fmt.Println("\nChecking key existence...")
	for _, key := range []string{"beta", "missing"} {
		if exists, err := wt.Exists(uri, key); err != nil {
			return fmt.Errorf("failed to check existence of %s: %w", key, err)
		} else {
			fmt.Printf("  %s exists: %v\n", key, exists)
		}
	}

	// Search near
	fmt.Println("\nTesting search_near...")
	if k, v, exact, ok, err := wt.SearchNear(uri, "charlie"); err != nil {
		return fmt.Errorf("failed to search_near: %w", err)
	} else if ok {
		exactStr := map[int]string{-1: "less than", 0: "exact match", 1: "greater than"}[exact]
		fmt.Printf("  Searched for 'charlie', found: %s = %s (%s)\n", k, v, exactStr)
	}

	// Scan all records
	fmt.Println("\nScanning all records...")
	if rows, err := wt.Scan(uri); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	} else {
		fmt.Printf("  Found %d records:\n", len(rows))
		for i, r := range rows {
			fmt.Printf("    [%d] %s -> %s\n", i+1, r.Key, r.Value)
		}
	}

	// Delete records
	fmt.Println("\nDeleting records...")
	toDelete := []string{"alpha", "beta"}
	for _, key := range toDelete {
		if err := wt.DeleteString(uri, key); err != nil {
			return fmt.Errorf("failed to delete %s: %w", key, err)
		}
		fmt.Printf("  Deleted: %s\n", key)
	}
	fmt.Println("✓ Records deleted")

	// Verify deletion
	fmt.Println("\nVerifying deletion...")
	if rows, err := wt.Scan(uri); err != nil {
		return fmt.Errorf("failed to scan after deletion: %w", err)
	} else {
		fmt.Printf("  Remaining records: %d\n", len(rows))
		for _, r := range rows {
			fmt.Printf("    %s -> %s\n", r.Key, r.Value)
		}
	}

	fmt.Println("\n=== Example completed successfully ===")
	return nil

}
