package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	wt "glowstickdb/pkgs/wiredtiger"
)

// User represents a user document
type User struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	Age       int       `json:"age"`
	CreatedAt time.Time `json:"created_at"`
	Status    string    `json:"status"`
}

// Order represents an order document
type Order struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// LogEntry represents a log entry
type LogEntry struct {
	ID        string    `json:"id"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
	Service   string    `json:"service"`
}

func main() {
	fmt.Println("=== BSON Query Patterns Example ===")

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

	// Create tables
	usersTable := "table:users"
	ordersTable := "table:orders"
	logsTable := "table:logs"

	if err := wtService.CreateTable(usersTable, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create users table:", err)
	}
	if err := wtService.CreateTable(ordersTable, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create orders table:", err)
	}
	if err := wtService.CreateTable(logsTable, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create logs table:", err)
	}

	// Example 1: Time-series queries (logs)
	fmt.Println("\n=== Example 1: Time-Series Queries ===")
	fmt.Println("Inserting log entries...")

	now := time.Now()
	logEntries := []LogEntry{
		{ID: "log_001", Level: "INFO", Message: "User login", Timestamp: now.Add(-2 * time.Hour), Service: "auth"},
		{ID: "log_002", Level: "ERROR", Message: "Database timeout", Timestamp: now.Add(-1 * time.Hour), Service: "db"},
		{ID: "log_003", Level: "WARN", Message: "High memory usage", Timestamp: now.Add(-30 * time.Minute), Service: "monitor"},
		{ID: "log_004", Level: "INFO", Message: "API request", Timestamp: now.Add(-15 * time.Minute), Service: "api"},
		{ID: "log_005", Level: "ERROR", Message: "Payment failed", Timestamp: now.Add(-5 * time.Minute), Service: "payment"},
	}

	for _, log := range logEntries {
		key := encodeTimestamp(log.Timestamp) + "_" + log.ID
		value, _ := json.Marshal(log)
		if err := wtService.PutString(logsTable, key, string(value)); err != nil {
			fmt.Print("Failed to put log:", err)
		}
		fmt.Printf("  %s -> %s\n", key, log.Message)
	}

	// Query logs from last hour
	fmt.Println("\nQuerying logs from last hour:")
	startTime := now.Add(-1 * time.Hour)
	endTime := now

	startKey := encodeTimestamp(startTime)
	endKey := encodeTimestamp(endTime)

	cursor, err := wtService.ScanRange(logsTable, startKey, endKey)
	if err != nil {
		log.Fatal("Failed to create logs cursor:", err)
	}
	defer cursor.Close()

	for cursor.Next() {
		_, logData, err := cursor.CurrentString()
		if err != nil {
			log.Fatal("Failed to get log data:", err)
		}

		var logEntry LogEntry
		if err := json.Unmarshal([]byte(logData), &logEntry); err != nil {
			continue
		}

		fmt.Printf("  %s [%s] %s - %s\n",
			logEntry.Timestamp.Format("15:04:05"),
			logEntry.Level,
			logEntry.Message,
			logEntry.Service)
	}

	// Example 2: User management with compound queries
	fmt.Println("\n=== Example 2: User Management ===")
	fmt.Println("Inserting users...")

	users := []User{
		{ID: "user_001", Email: "alice@example.com", Name: "Alice", Age: 25, CreatedAt: now.Add(-30 * 24 * time.Hour), Status: "active"},
		{ID: "user_002", Email: "bob@example.com", Name: "Bob", Age: 30, CreatedAt: now.Add(-25 * 24 * time.Hour), Status: "active"},
		{ID: "user_003", Email: "charlie@example.com", Name: "Charlie", Age: 35, CreatedAt: now.Add(-20 * 24 * time.Hour), Status: "inactive"},
		{ID: "user_004", Email: "diana@example.com", Name: "Diana", Age: 28, CreatedAt: now.Add(-15 * 24 * time.Hour), Status: "active"},
		{ID: "user_005", Email: "eve@example.com", Name: "Eve", Age: 32, CreatedAt: now.Add(-10 * 24 * time.Hour), Status: "pending"},
	}

	for _, user := range users {
		// Store by email for email-based queries
		emailKey := "email_" + user.Email
		userData, _ := json.Marshal(user)
		if err := wtService.PutString(usersTable, emailKey, string(userData)); err != nil {
			log.Fatal("Failed to put user:", err)
		}

		// Store by creation date for time-based queries
		dateKey := "date_" + encodeTimestamp(user.CreatedAt) + "_" + user.ID
		if err := wtService.PutString(usersTable, dateKey, string(userData)); err != nil {
			log.Fatal("Failed to put user by date:", err)
		}

		fmt.Printf("  %s (%s, age %d)\n", user.Name, user.Email, user.Age)
	}

	// Query users by email range
	fmt.Println("\nQuerying users with emails from 'a' to 'c':")
	cursor, err = wtService.ScanRange(usersTable, "email_a", "email_c")
	if err != nil {
		log.Fatal("Failed to create email cursor:", err)
	}
	defer cursor.Close()

	for cursor.Next() {
		key, userData, err := cursor.CurrentString()
		if err != nil {
			log.Fatal("Failed to get user data:", err)
		}

		if key[:6] == "email_" {
			var user User
			if err := json.Unmarshal([]byte(userData), &user); err != nil {
				continue
			}
			fmt.Printf("  %s (%s) - %s\n", user.Name, user.Email, user.Status)
		}
	}

	// Example 3: Order analytics
	fmt.Println("\n=== Example 3: Order Analytics ===")
	fmt.Println("Inserting orders...")

	orders := []Order{
		{ID: "order_001", UserID: "user_001", Amount: 99.99, Status: "completed", CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "order_002", UserID: "user_002", Amount: 149.50, Status: "completed", CreatedAt: now.Add(-1 * time.Hour)},
		{ID: "order_003", UserID: "user_001", Amount: 75.00, Status: "pending", CreatedAt: now.Add(-30 * time.Minute)},
		{ID: "order_004", UserID: "user_003", Amount: 200.00, Status: "completed", CreatedAt: now.Add(-15 * time.Minute)},
		{ID: "order_005", UserID: "user_004", Amount: 50.25, Status: "failed", CreatedAt: now.Add(-5 * time.Minute)},
	}

	for _, order := range orders {
		key := encodeTimestamp(order.CreatedAt) + "_" + order.ID
		orderData, _ := json.Marshal(order)
		if err := wtService.PutString(ordersTable, key, string(orderData)); err != nil {
			log.Fatal("Failed to put order:", err)
		}
		fmt.Printf("  %s - $%.2f (%s)\n", order.ID, order.Amount, order.Status)
	}

	// Calculate revenue for last hour
	fmt.Println("\nCalculating revenue for last hour:")
	startTime = now.Add(-1 * time.Hour)
	endTime = now

	startKey = encodeTimestamp(startTime)
	endKey = encodeTimestamp(endTime)

	cursor, err = wtService.ScanRange(ordersTable, startKey, endKey)
	if err != nil {
		log.Fatal("Failed to create orders cursor:", err)
	}
	defer cursor.Close()

	totalRevenue := 0.0
	completedOrders := 0

	for cursor.Next() {
		_, orderData, err := cursor.CurrentString()
		if err != nil {
			log.Fatal("Failed to get order data:", err)
		}

		var order Order
		if err := json.Unmarshal([]byte(orderData), &order); err != nil {
			continue
		}

		if order.Status == "completed" {
			totalRevenue += order.Amount
			completedOrders++
		}
	}

	fmt.Printf("  Completed orders: %d\n", completedOrders)
	fmt.Printf("  Total revenue: $%.2f\n", totalRevenue)

	// Example 4: Pagination with BSON data
	fmt.Println("\n=== Example 4: Pagination ===")
	fmt.Println("Paginating users (page size 2):")

	pageSize := 2
	lastKey := ""
	pageNum := 1

	for {
		var endKey string
		if lastKey == "" {
			endKey = "email_z" // High sentinel for first page
		} else {
			endKey = "email_z"
		}

		cursor, err := wtService.ScanRange(usersTable, lastKey, endKey)
		if err != nil {
			log.Fatal("Failed to create pagination cursor:", err)
		}

		fmt.Printf("\nPage %d:\n", pageNum)
		pageItems := 0
		var nextLastKey string

		for cursor.Next() && pageItems < pageSize {
			key, userData, err := cursor.CurrentString()
			if err != nil {
				log.Fatal("Failed to get user data:", err)
			}

			if key[:6] == "email_" {
				var user User
				if err := json.Unmarshal([]byte(userData), &user); err != nil {
					continue
				}
				fmt.Printf("  %s (%s)\n", user.Name, user.Email)
				nextLastKey = key
				pageItems++
			}
		}
		cursor.Close()

		if pageItems == 0 {
			break // No more items
		}

		lastKey = nextLastKey
		pageNum++

		if pageItems < pageSize {
			break // Last page
		}
	}

	fmt.Println("\n=== BSON Query Patterns Example Completed ===")
}

// encodeTimestamp encodes a timestamp for lexicographic ordering
func encodeTimestamp(t time.Time) string {
	return fmt.Sprintf("%020d", t.UnixNano())
}
