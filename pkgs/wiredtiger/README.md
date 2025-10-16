# WiredTiger CGO Wrapper Documentation

A Go wrapper for WiredTiger database using CGO, providing both string and binary key-value operations optimized for document storage.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Best Practices](#best-practices)

---

## Installation

### Prerequisites

- Go 1.16+
- WiredTiger 11.3.1+ installed via Homebrew or compiled from source
- CGO enabled (`CGO_ENABLED=1`)

### macOS Installation

```bash
# Install WiredTiger via Homebrew
brew install wiredtiger

# Verify installation
ls -la /usr/local/lib/libwiredtiger*
```

### Linux Installation

```bash
# Build from source
git clone https://github.com/wiredtiger/wiredtiger.git
cd wiredtiger
./autogen.sh
./configure --enable-shared
make
sudo make install
```

### Build Your Project

```bash
CGO_ENABLED=1 go build
```

---

## Quick Start

### Basic Usage

```go
package main

import (
	"fmt"
	"log"
	"wiredtiger"
)

func main() {
	// Create service
	svc := wiredtiger.NewService()
	defer svc.Close()

	// Open database
	if err := svc.Open("./mydata", "create,cache_size=100M"); err != nil {
		log.Fatal(err)
	}

	// Create a table
	if err := svc.CreateTable("table:users", "key_format=S,value_format=S"); err != nil {
		log.Fatal(err)
	}

	// Insert data
	if err := svc.PutString("table:users", "user:123", "Alice"); err != nil {
		log.Fatal(err)
	}

	// Retrieve data
	if value, found, err := svc.GetString("table:users", "user:123"); err != nil {
		log.Fatal(err)
	} else if found {
		fmt.Printf("Found: %s\n", value)
	}
}
```

---

## Core Concepts

### Key-Value Store

WiredTiger is a high-performance key-value storage engine. This wrapper provides:

- **String operations**: For text keys and values
- **Binary operations**: For ObjectIDs, timestamps, serialized data (BSON, JSON, etc.)

### Tables

Each table is a separate key-value collection with its own configuration:

```
table:users       <- String-based user data
collection:posts  <- Binary storage for documents
index:email       <- Fast lookup index
```

### Key Formats

- `key_format=S`: String keys
- `key_format=u`: Binary (unsigned byte array) keys

### Value Formats

- `value_format=S`: String values
- `value_format=u`: Binary values

### Configuration String

Controls table behavior:

```
key_format=S,value_format=S,block_compressor=snappy,split_pct=90
```

---

## API Reference

### Connection Management

#### `NewService() Service`

Creates a new WiredTiger service instance.

```go
svc := wiredtiger.NewService()
```

#### `Open(home string, config string) error`

Opens a database connection.

```go
err := svc.Open("./data", "create,cache_size=2GB,log=(enabled=true)")
```

#### `Close() error`

Closes the database connection.

```go
defer svc.Close()
```

#### `CreateTable(name string, config string) error`

Creates a new table with specified configuration.

```go
// String table
svc.CreateTable("table:config", "key_format=S,value_format=S")

// Binary table optimized for documents
svc.CreateTable("collection:users", "key_format=u,value_format=u,block_compressor=snappy")
```

---

### String Operations

#### `PutString(table, key, value string) error`

Inserts or updates a string key-value pair.

```go
err := svc.PutString("table:users", "alice", "Alice Johnson")
```

#### `GetString(table, key string) (string, bool, error)`

Retrieves a value by string key.

```go
value, found, err := svc.GetString("table:users", "alice")
if found {
    fmt.Println("Value:", value)
}
```

#### `DeleteString(table, key string) error`

Deletes a key-value pair.

```go
err := svc.DeleteString("table:users", "alice")
```

#### `Exists(table, key string) (bool, error)`

Checks if a key exists.

```go
exists, err := svc.Exists("table:users", "alice")
```

#### `Scan(table string) ([]KeyValuePair, error)`

Scans all key-value pairs in a table.

```go
pairs, err := svc.Scan("table:users")
for _, pair := range pairs {
    fmt.Printf("%s: %s\n", pair.Key, pair.Value)
}
```

#### `SearchNear(table, key string) (string, string, int, bool, error)`

Finds a key near the search key (useful for range queries).

```go
key, value, exact, found, err := svc.SearchNear("table:users", "alice")
// exact: -1 (less), 0 (exact), 1 (greater)
```

---

### Binary Operations

#### `PutBinary(table string, key, value []byte) error`

Inserts binary key-value pair.

```go
key := []byte{0x01, 0x02, 0x03}
value := []byte{0xAA, 0xBB, 0xCC}
err := svc.PutBinary("collection:data", key, value)
```

#### `GetBinary(table string, key []byte) ([]byte, bool, error)`

Retrieves binary value.

```go
data, found, err := svc.GetBinary("collection:data", key)
```

#### `DeleteBinary(table string, key []byte) error`

Deletes a binary key-value pair.

```go
err := svc.DeleteBinary("collection:data", key)
```

#### `ExistsBinary(table string, key []byte) (bool, error)`

Checks if binary key exists.

```go
exists, err := svc.ExistsBinary("collection:data", key)
```

#### `ScanBinary(table string) ([]BinaryKeyValuePair, error)`

Scans all binary key-value pairs.

```go
pairs, err := svc.ScanBinary("collection:data")
for _, pair := range pairs {
    fmt.Printf("Key: %x, Value: %x\n", pair.Key, pair.Value)
}
```

#### `SearchNearBinary(table string, key []byte) ([]byte, []byte, int, bool, error)`

Finds binary key near search key.

```go
key, value, exact, found, err := svc.SearchNearBinary("collection:data", probeKey)
```

---

### Convenience Functions

#### `PutBinaryWithStringKey(table, stringKey string, value []byte) error`

Converts string key to binary and inserts.

```go
svc.PutBinaryWithStringKey("table:config", "app:version", []byte{0x01, 0x00})
```

#### `GetBinaryWithStringKey(table, stringKey string) ([]byte, bool, error)`

Retrieves binary value using string key.

```go
value, found, err := svc.GetBinaryWithStringKey("table:config", "app:version")
```

#### `DeleteBinaryWithStringKey(table, stringKey string) error`

Deletes using string key.

```go
err := svc.DeleteBinaryWithStringKey("table:config", "app:version")
```

---

## Examples

### Example 1: Simple Key-Value Store

```go
package main

import (
	"fmt"
	"log"
	"wiredtiger"
)

func main() {
	svc := wiredtiger.NewService()
	defer svc.Close()

	// Open database
	if err := svc.Open("./store", "create,cache_size=100M"); err != nil {
		log.Fatal(err)
	}

	// Create configuration table
	if err := svc.CreateTable("table:config", "key_format=S,value_format=S"); err != nil {
		log.Fatal(err)
	}

	// Store configuration
	configs := map[string]string{
		"app:name":    "MyApp",
		"app:version": "1.0.0",
		"db:host":     "localhost",
		"db:port":     "27017",
	}

	for key, value := range configs {
		if err := svc.PutString("table:config", key, value); err != nil {
			log.Fatal(err)
		}
	}

	// Retrieve and display
	fmt.Println("Configuration:")
	pairs, _ := svc.Scan("table:config")
	for _, pair := range pairs {
		fmt.Printf("  %s = %s\n", pair.Key, pair.Value)
	}
}
```

### Example 2: Document Database Pattern

```go
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"wiredtiger"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func main() {
	svc := wiredtiger.NewService()
	defer svc.Close()

	if err := svc.Open("./docdb", "create,cache_size=500M"); err != nil {
		log.Fatal(err)
	}

	// Create collection (documents)
	if err := svc.CreateTable("collection:users",
		"key_format=u,value_format=u,block_compressor=snappy"); err != nil {
		log.Fatal(err)
	}

	// Create index (email -> user_id)
	if err := svc.CreateTable("index:users:email",
		"key_format=u,value_format=u,block_compressor=zstd"); err != nil {
		log.Fatal(err)
	}

	// Insert documents
	users := []User{
		{Name: "Alice", Email: "alice@example.com", Age: 30},
		{Name: "Bob", Email: "bob@example.com", Age: 25},
		{Name: "Charlie", Email: "charlie@example.com", Age: 35},
	}

	for i, user := range users {
		// Create user ID (binary)
		userID := make([]byte, 4)
		binary.BigEndian.PutUint32(userID, uint32(i+1))

		// Serialize user to JSON
		data, _ := json.Marshal(user)

		// Store in collection
		if err := svc.PutBinary("collection:users", userID, data); err != nil {
			log.Fatal(err)
		}

		// Create index entry: email -> user_id
		emailIndexKey := append([]byte(user.Email), userID...)
		if err := svc.PutBinary("index:users:email", emailIndexKey, userID); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("✓ Inserted user %d: %s\n", i+1, user.Name)
	}

	// Query by email
	fmt.Println("\nQuerying by email (alice@example.com):")
	searchKey := append([]byte("alice@example.com"), make([]byte, 4)...)

	// Find first matching entry
	pairs, _ := svc.ScanBinary("index:users:email")
	for _, pair := range pairs {
		if len(pair.Key) >= len("alice@example.com") {
			if string(pair.Key[:len("alice@example.com")]) == "alice@example.com" {
				// Found index entry, pair.Value is the user_id
				userData, _, _ := svc.GetBinary("collection:users", pair.Value)
				var user User
				json.Unmarshal(userData, &user)
				fmt.Printf("  Found: %+v\n", user)
				break
			}
		}
	}
}
```

### Example 3: Time-Series Data

```go
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"
	"wiredtiger"
)

func main() {
	svc := wiredtiger.NewService()
	defer svc.Close()

	if err := svc.Open("./timeseries", "create,cache_size=200M"); err != nil {
		log.Fatal(err)
	}

	// Create time-series table with binary timestamps as keys
	if err := svc.CreateTable("table:metrics",
		"key_format=u,value_format=u,prefix_compression=true"); err != nil {
		log.Fatal(err)
	}

	// Insert metrics (timestamp -> value)
	baseTime := time.Now().Add(-1 * time.Hour)

	fmt.Println("Inserting time-series data:")
	for i := 0; i < 10; i++ {
		t := baseTime.Add(time.Duration(i*6) * time.Minute)

		// Encode timestamp as binary (big-endian uint64 for proper sorting)
		tsKey := make([]byte, 8)
		binary.BigEndian.PutUint64(tsKey, uint64(t.UnixNano()))

		// Value: CPU usage percentage
		value := make([]byte, 4)
		cpuUsage := uint32(50 + i*5) // 50-95%
		binary.BigEndian.PutUint32(value, cpuUsage)

		if err := svc.PutBinary("table:metrics", tsKey, value); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %s: %d%%\n", t.Format("15:04:05"), cpuUsage)
	}

	// Range query: metrics from 30 minutes ago
	fmt.Println("\nQuerying metrics from last 30 minutes:")
	rangeStart := time.Now().Add(-30 * time.Minute)
	rangeKey := make([]byte, 8)
	binary.BigEndian.PutUint64(rangeKey, uint64(rangeStart.UnixNano()))

	// Get all metrics (in real app, implement proper range scan)
	pairs, _ := svc.ScanBinary("table:metrics")
	for _, pair := range pairs {
		if len(pair.Key) == 8 && len(pair.Value) == 4 {
			ts := int64(binary.BigEndian.Uint64(pair.Key))
			value := binary.BigEndian.Uint32(pair.Value)
			t := time.Unix(0, ts)
			if t.After(rangeStart) {
				fmt.Printf("  %s: %d%%\n", t.Format("15:04:05"), value)
			}
		}
	}
}
```

### Example 4: Cache Layer

```go
package main

import (
	"fmt"
	"log"
	"wiredtiger"
)

type CacheLayer struct {
	svc wiredtiger.Service
}

func NewCacheLayer() (*CacheLayer, error) {
	svc := wiredtiger.NewService()
	if err := svc.Open("./cache", "create,cache_size=1GB"); err != nil {
		return nil, err
	}

	if err := svc.CreateTable("table:cache",
		"key_format=S,value_format=S,block_compressor=snappy"); err != nil {
		return nil, err
	}

	return &CacheLayer{svc: svc}, nil
}

func (c *CacheLayer) Set(key, value string) error {
	return c.svc.PutString("table:cache", key, value)
}

func (c *CacheLayer) Get(key string) (string, bool, error) {
	return c.svc.GetString("table:cache", key)
}

func (c *CacheLayer) Delete(key string) error {
	return c.svc.DeleteString("table:cache", key)
}

func (c *CacheLayer) Close() error {
	return c.svc.Close()
}

func main() {
	cache, err := NewCacheLayer()
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close()

	// Use cache
	cache.Set("user:123:name", "Alice")
	cache.Set("user:123:email", "alice@example.com")
	cache.Set("api:rate_limit:ip:192.168.1.1", "45/60")

	// Retrieve
	if name, found, _ := cache.Get("user:123:name"); found {
		fmt.Printf("Name: %s\n", name)
	}

	if rateLimit, found, _ := cache.Get("api:rate_limit:ip:192.168.1.1"); found {
		fmt.Printf("Rate Limit: %s\n", rateLimit)
	}

	// Delete
	cache.Delete("user:123:name")
	if _, found, _ := cache.Get("user:123:name"); !found {
		fmt.Println("✓ Key deleted successfully")
	}
}
```

---

## Best Practices

### 1. Configuration Selection

**For Small Documents/Strings:**

```go
"key_format=S,value_format=S"
```

**For Documents (BSON/JSON):**

```go
"key_format=u,value_format=u,block_compressor=snappy,internal_page_max=16KB"
```

**For Indexes:**

```go
"key_format=u,value_format=u,block_compressor=zstd,prefix_compression=true"
```

**For High Write Load:**

```go
"block_compressor=snappy,split_pct=95,leaf_page_max=32KB"
```

**For High Read Load:**

```go
"block_compressor=zstd,prefix_compression=true,cache_size=8GB"
```

### 2. Memory Management

- Set `cache_size` to 50-60% of available RAM
- Enable compression (`snappy` for speed, `zstd` for ratio)
- Monitor cache hit ratio with statistics

### 3. Durability

```go
// Journaling enabled
svc.Open("./data", "create,log=(enabled=true,compressor=snappy)")

// Checkpointing strategy
"checkpoint=(wait=60,log_size=2GB)"
```

### 4. Key Design

**Good:**

- ObjectID format: `[timestamp][random][counter]` - sorts by time
- Timestamp keys: big-endian `uint64` for range queries
- Composite: `field1|field2|id` for compound indexes

**Avoid:**

- UUID v4 as key (unordered, cache-unfriendly)
- Large keys (increases memory pressure)
- Random key generation without structure

### 5. Error Handling

```go
if err := svc.PutString("table:data", key, value); err != nil {
	// WiredTiger returns specific error codes
	// Check error string for details
	log.Printf("Put failed: %v", err)
	return err
}
```

### 6. Resource Cleanup

Always defer Close():

```go
svc := wiredtiger.NewService()
if err := svc.Open("./data", "create"); err != nil {
	log.Fatal(err)
}
defer svc.Close()

// Safe even if Open failed
```

---

## Performance Tips

1. **Batch Operations**: Batch multiple inserts for better throughput
2. **Compression**: Use `snappy` for hot data, `zstd` for cold/indexes
3. **Page Size**: Smaller pages (8KB) for indexes, larger (16-32KB) for documents
4. **Splitting**: Tune `split_pct` based on workload (90 for balanced, 95 for writes)
5. **Range Queries**: Use binary-encoded timestamps for efficient scans
6. **Prefix Compression**: Enable for indexes with common prefixes

---

## Troubleshooting

### "wiredtiger_open failed"

- Check database directory permissions
- Ensure WiredTiger is installed: `brew ls wiredtiger`
- Verify CGO is enabled: `CGO_ENABLED=1 go run`

### "connection not open"

- Call `Open()` before other operations
- Check if `Close()` was called prematurely

### Memory pressure

- Increase `cache_size` in connection config
- Reduce document size
- Use compression

### Slow queries

- Create appropriate indexes
- Use binary keys for range queries
- Check cache hit ratio

---

## See Also

- [WiredTiger Official Docs](http://source.wiredtiger.com/)
- [Binary Operations Example](#example-2-document-database-pattern)
- [Time-Series Example](#example-3-time-series-data)
