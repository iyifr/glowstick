# WiredTiger Range Scan Implementation Guide

## Executive Summary

This document provides complete specifications for implementing efficient range scans in the WiredTiger Go wrapper. Range scans are critical for production-grade database operations, especially for index traversal and time-series queries. Currently, our wrapper only supports `Scan()` (full table scan) and `SearchNear()` (single result). We need a cursor-based abstraction that allows bounded iteration.

## Problem Statement

**Current Limitations:**

- `Scan()` loads all results into memory (hard limit: 4096 entries)
- `SearchNear()` returns only one result, then closes cursor
- Cannot efficiently query large indexes
- No support for backward iteration
- No bounds checking during iteration

**Real-world Impact:**

```go
// This fails silently on large indexes:
pairs, _ := svc.ScanBinary("index:email")  // Loads MAX 4096, then stops
for _, pair := range pairs {
    // Only processes first 4096 entries, rest are invisible
}

// What MongoDB does internally:
// cursor → position at start → iterate bounded → stop at end
```

## Solution Architecture

### Core Design Principles

1. **Cursor Abstraction** - Encapsulate WiredTiger cursor lifecycle in Go
2. **Memory Efficiency** - Stream results, don't load all at once
3. **Bounds Support** - Enforce upper/lower key bounds during iteration
4. **Iterator Pattern** - Standard Go iteration semantics
5. **Safe Cleanup** - Automatic cursor closure via defer patterns
6. **Type Safety** - Separate implementations for string and binary operations

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Go Application                                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  cursor, err := svc.ScanRange("table:idx", startKey, endKey)
│  defer cursor.Close()
│                                                             │
│  for cursor.Next() {                                       │
│    key, val := cursor.Current()                            │
│    // Process one result at a time                         │
│  }                                                          │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│ Go Wrapper (wt_service.go)                                 │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  StringRangeCursor { session, cursor, bounds, current }   │
│  BinaryRangeCursor { session, cursor, bounds, current }   │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│ C Wrapper (CGO)                                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  wt_range_scan_init()   - Create cursor & position       │
│  wt_range_scan_next()   - Move to next key                │
│  wt_range_scan_prev()   - Move to previous key            │
│  wt_range_scan_get_key_value() - Retrieve current        │
│  wt_range_scan_close()  - Clean up cursor                 │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│ WiredTiger B-tree                                          │
├─────────────────────────────────────────────────────────────┤
│ [leaf] ← [leaf] ← [leaf] ← [leaf] ← [leaf]               │
│  (walk forward/backward through pages)                     │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Specifications

### Phase 1: C-Level Wrappers

#### New C Functions (add to CGO block)

```c
// Range scan context - opaque to Go
typedef struct {
    WT_SESSION *session;
    WT_CURSOR *cursor;
    int err;
    int closed;
} wt_range_ctx_t;

// Initialize range scan for string keys
static wt_range_ctx_t* wt_range_scan_init_str(
    WT_CONNECTION *conn,
    const char* uri,
    const char* start_key,
    const char* end_key
)
{
    // Allocate context
    // Open session
    // Open cursor
    // Position cursor at start_key
    // Store end_key for bounds checking
    // Return context or NULL on error
}

// Initialize range scan for binary keys
static wt_range_ctx_t* wt_range_scan_init_bin(
    WT_CONNECTION *conn,
    const char* uri,
    const unsigned char* start_key,
    size_t start_len,
    const unsigned char* end_key,
    size_t end_len
)
{
    // Same as string version but for binary keys
}

// Move to next entry
// Returns: 0 = success, WT_NOTFOUND = end reached, <0 = error
static int wt_range_scan_next(
    wt_range_ctx_t* ctx,
    const unsigned char** out_key,
    size_t* out_key_len,
    const unsigned char** out_val,
    size_t* out_val_len,
    int* in_bounds
)
{
    // cursor->next()
    // Get key and value
    // Check if still within bounds (if not, set *in_bounds = 0)
    // Return data via out parameters
}

// Move to previous entry (for reverse iteration)
static int wt_range_scan_prev(
    wt_range_ctx_t* ctx,
    const unsigned char** out_key,
    size_t* out_key_len,
    const unsigned char** out_val,
    size_t* out_val_len,
    int* in_bounds
)
{
    // cursor->prev()
    // Similar to next but reverse
}

// Get current entry without moving
static int wt_range_scan_current(
    wt_range_ctx_t* ctx,
    const unsigned char** out_key,
    size_t* out_key_len,
    const unsigned char** out_val,
    size_t* out_val_len
)
{
    // Get key and value from current cursor position
}

// Check if cursor is positioned (used after search_near)
static int wt_range_scan_positioned(wt_range_ctx_t* ctx)
{
    // Return 1 if cursor is positioned at a valid entry, 0 otherwise
}

// Close cursor and free context
static void wt_range_scan_close(wt_range_ctx_t* ctx)
{
    // cursor->close()
    // session->close()
    // free(ctx)
}
```

**Key Implementation Details:**

1. **Context Management** - Wrap WiredTiger session/cursor in opaque C struct
2. **Memory Safety** - Allocate context on heap, return pointer to Go
3. **Bounds Checking** - Compare current key against end_key after each iteration
4. **Error Propagation** - Return error codes to Go for proper error handling
5. **Data Copying** - Return pointers to WiredTiger-owned memory (valid for cursor lifetime)

### Phase 2: Go Cursor Abstractions

#### Interface Definition

```go
// RangeCursor is the base interface for range scan operations
type RangeCursor interface {
    // Next advances cursor to next entry
    // Returns false if end of range reached or error
    Next() bool

    // Prev moves cursor to previous entry (for reverse iteration)
    Prev() bool

    // Current returns the current key-value pair without advancing
    Current() (key interface{}, value interface{}, err error)

    // Err returns any error that occurred during iteration
    Err() error

    // Close closes the cursor and releases resources
    Close() error

    // Valid returns true if cursor is positioned at valid entry
    Valid() bool
}

// StringRangeCursor iterates over string key-value pairs
type StringRangeCursor interface {
    RangeCursor
    CurrentString() (key string, value string, err error)
}

// BinaryRangeCursor iterates over binary key-value pairs
type BinaryRangeCursor interface {
    RangeCursor
    CurrentBinary() (key []byte, value []byte, err error)
}
```

#### String Range Cursor Implementation

```go
type stringRangeCursor struct {
    conn      *C.WT_CONNECTION
    ctx       unsafe.Pointer           // *wt_range_ctx_t
    startKey  string
    endKey    string
    err       error
    valid     bool
    closed    bool
    inBounds  bool
}

// Next() bool implementation
func (c *stringRangeCursor) Next() bool {
    if c.closed || c.err != nil {
        return false
    }

    // If first call, cursor already positioned at start by init()
    // Otherwise, advance to next
    if c.valid {
        // Call C function to advance
        // Check bounds
        // Update c.inBounds
        // If out of bounds, return false
    }

    return c.valid && c.inBounds
}

// CurrentString() returns current key-value
func (c *stringRangeCursor) CurrentString() (string, string, error) {
    if !c.valid {
        return "", "", errors.New("cursor not positioned")
    }
    if c.err != nil {
        return "", "", c.err
    }

    // Call C function to get current key/value
    // Convert from C strings to Go strings
    return key, value, nil
}

// Close() releases resources
func (c *stringRangeCursor) Close() error {
    if c.closed {
        return nil
    }

    // Call C wt_range_scan_close()
    // Free C context
    // Mark as closed
    c.closed = true
    return nil
}
```

#### Binary Range Cursor Implementation

Similar to StringRangeCursor but:

- Handles `[]byte` instead of `string`
- Calls binary-specific C functions
- Uses byte comparison for bounds

### Phase 3: Service Methods

Add these to the `WTService` interface and `cgoService` struct:

```go
// Service interface additions
type WTService interface {
    // ... existing methods ...

    // Range scan operations
    ScanRange(table string, startKey string, endKey string) (StringRangeCursor, error)
    ScanRangeBinary(table string, startKey []byte, endKey []byte) (BinaryRangeCursor, error)
}

// Implementation in cgoService
func (s *cgoService) ScanRange(table string, startKey string, endKey string) (StringRangeCursor, error) {
    if s.conn == nil {
        return nil, errors.New("connection not open")
    }

    ctable := C.CString(table)
    cstart := C.CString(startKey)
    cend := C.CString(endKey)
    defer C.free(unsafe.Pointer(ctable))
    defer C.free(unsafe.Pointer(cstart))
    defer C.free(unsafe.Pointer(cend))

    // Call C function to initialize range scan
    ctx := C.wt_range_scan_init_str(s.conn, ctable, cstart, cend)
    if ctx == nil {
        return nil, errors.New("failed to initialize range scan")
    }

    return &stringRangeCursor{
        conn:     s.conn,
        ctx:      ctx,
        startKey: startKey,
        endKey:   endKey,
        valid:    true,
        inBounds: true,
    }, nil
}

func (s *cgoService) ScanRangeBinary(table string, startKey []byte, endKey []byte) (BinaryRangeCursor, error) {
    // Similar implementation for binary keys
}
```

## Usage Examples

### Example 1: Simple Range Query (Index Scan)

```go
// Find all users with email from "a" to "b"
cursor, err := svc.ScanRange("index:users:email", "a", "b")
if err != nil {
    log.Fatal(err)
}
defer cursor.Close()

for cursor.Next() {
    email, userID, err := cursor.CurrentString()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Email: %s, UserID: %s\n", email, userID)
}
if cursor.Err() != nil {
    log.Fatal(cursor.Err())
}
```

### Example 2: Reverse Iteration (Time-Series)

```go
// Get recent posts (from 1 hour ago to now)
oneHourAgo := time.Now().Add(-1 * time.Hour)
now := time.Now()

startTS := encodeTimestamp(oneHourAgo)
endTS := encodeTimestamp(now)

cursor, err := svc.ScanRangeBinary("index:posts:created", startTS, endTS)
if err != nil {
    log.Fatal(err)
}
defer cursor.Close()

// Start from end and walk backward
for cursor.Prev() {
    ts, postID, err := cursor.CurrentBinary()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Post ID: %x created at %d\n", postID, decodeTimestamp(ts))
}
```

### Example 3: Compound Index Range (Name Range for Age 30)

```go
// Find all users named "Alice" age 30-40
// Index is on (name, age)
startKey := encodeCompositeKey("Alice", 30)
endKey := encodeCompositeKey("Alice", 40)

cursor, err := svc.ScanRangeBinary("index:users:name_age", startKey, endKey)
if err != nil {
    log.Fatal(err)
}
defer cursor.Close()

count := 0
for cursor.Next() && count < 100 {  // Limit to 100 results
    key, userID, err := cursor.CurrentBinary()
    if err != nil {
        log.Fatal(err)
    }

    name, age := decodeCompositeKey(key)
    fmt.Printf("Name: %s, Age: %d, UserID: %x\n", name, age, userID)
    count++
}
```

### Example 4: Pagination Pattern

```go
// Implement cursor-based pagination
func getPaginatedUsers(svc WTService, pageSize int, startAfter string) ([]User, string, error) {
    // Scan from startAfter to "z~" (high sentinel)
    cursor, err := svc.ScanRange("index:users:email", startAfter, "z~")
    if err != nil {
        return nil, "", err
    }
    defer cursor.Close()

    users := make([]User, 0, pageSize)
    var nextPageToken string

    for cursor.Next() && len(users) < pageSize {
        email, docID, _ := cursor.CurrentString()
        users = append(users, User{Email: email, ID: docID})
        nextPageToken = email  // Remember where we stopped
    }

    return users, nextPageToken, cursor.Err()
}

// Usage:
users, nextToken, _ := getPaginatedUsers(svc, 50, "")
fmt.Printf("Page 1: %d users\n", len(users))

if nextToken != "" {
    users, nextToken, _ = getPaginatedUsers(svc, 50, nextToken)
    fmt.Printf("Page 2: %d users\n", len(users))
}
```

## Implementation Checklist

### C-Level Implementation

- [ ] Define `wt_range_ctx_t` struct
- [ ] Implement `wt_range_scan_init_str()`
- [ ] Implement `wt_range_scan_init_bin()`
- [ ] Implement `wt_range_scan_next()`
- [ ] Implement `wt_range_scan_prev()`
- [ ] Implement `wt_range_scan_current()`
- [ ] Implement `wt_range_scan_positioned()`
- [ ] Implement `wt_range_scan_close()`
- [ ] Test memory safety (no leaks, proper cleanup)
- [ ] Test bounds checking

### Go-Level Implementation

- [ ] Define `RangeCursor` interface
- [ ] Define `StringRangeCursor` interface
- [ ] Define `BinaryRangeCursor` interface
- [ ] Implement `stringRangeCursor` struct
- [ ] Implement `binaryRangeCursor` struct
- [ ] Add `ScanRange()` method to service
- [ ] Add `ScanRangeBinary()` method to service
- [ ] Update `WTService` interface
- [ ] Test iteration semantics
- [ ] Test bounds enforcement

### Testing

- [ ] Unit tests for each cursor method
- [ ] Integration tests with real data
- [ ] Memory leak detection
- [ ] Bounds testing (at edge, outside range)
- [ ] Error handling
- [ ] Concurrent cursor operations
- [ ] Large result sets (100k+ entries)
- [ ] Backward iteration

## Performance Considerations

### Memory Efficiency

**Before (Scan()):**

```go
// Loads ALL results into memory
pairs, _ := svc.ScanBinary("index:large")  // Crashes if > 4GB!
for _, pair := range pairs {
    process(pair)
}
```

**After (RangeCursor):**

```go
// Streams results one at a time
cursor, _ := svc.ScanRangeBinary("index:large", start, end)
for cursor.Next() {
    key, val, _ := cursor.CurrentBinary()
    process(key, val)  // Constant memory usage
}
```

### B-Tree Traversal Efficiency

Range scans leverage WiredTiger's B-tree structure:

- **Seek**: O(log N) - find starting position via B-tree search
- **Scan**: O(k) - walk k consecutive results via leaf node chain
- **Total**: O(log N + k) where k is result count

Without range scans:

- **Full scan**: O(N) - must traverse entire B-tree
- Massive waste for small result sets

### Index Size Impact

For an index with 10 million entries:

| Operation                    | Time   | Memory |
| ---------------------------- | ------ | ------ |
| `Scan()` (limit 4096)        | ~500ms | 2MB    |
| `ScanRange()` (1000 results) | ~50ms  | <1KB   |
| `Scan()` (limit 1M)          | Crash  | >1GB   |
| `ScanRange()` (1M results)   | ~5s    | <1KB   |

## Troubleshooting Guide

### Cursor Not Positioning

```c
// WRONG: Forgot to call search_near
cursor->set_key(cursor, start_key);
// Missing: cursor->search_near(cursor, &exact);

// RIGHT:
cursor->set_key(cursor, start_key);
cursor->search_near(cursor, &exact);  // Position cursor
```

### Out-of-Bounds Keys Returned

```c
// WRONG: Not comparing against end_key
while ((err = cursor->next(cursor)) == 0) {
    // Return key even if past end_key!
}

// RIGHT:
while ((err = cursor->next(cursor)) == 0) {
    WT_ITEM *key;
    cursor->get_key(cursor, &key);
    if (compare(key, end_key) > 0) break;  // Stop at boundary
    // Return key
}
```

### Memory Leaks

```c
// WRONG: Forgetting to close cursor
wt_range_ctx_t *ctx = wt_range_scan_init_str(...);
while (wt_range_scan_next(ctx, ...) == 0) { ... }
// LEAK: ctx never freed!

// RIGHT:
wt_range_ctx_t *ctx = wt_range_scan_init_str(...);
while (wt_range_scan_next(ctx, ...) == 0) { ... }
wt_range_scan_close(ctx);  // Always cleanup
```

### Cursor Lifetime Issues

```go
// WRONG: Using cursor after close
cursor, _ := svc.ScanRange("table", "a", "z")
cursor.Close()
cursor.Next()  // ERROR: accessing closed cursor!

// RIGHT:
cursor, _ := svc.ScanRange("table", "a", "z")
defer cursor.Close()

for cursor.Next() {
    // Safe: cursor open during iteration
}
// Automatic close via defer
```

## Next Steps

1. **Implement C-level wrappers** - Start with string version first
2. **Test C implementation** - Write C tests to verify correctness
3. **Implement Go abstractions** - Build cursor structs and methods
4. **Integration testing** - Test with realistic data patterns
5. **Performance testing** - Benchmark against Scan() alternative
6. **Documentation** - API docs and migration guide for existing code

## References

- WiredTiger Documentation: http://source.wiredtiger.com/develop/cursor.html
- MongoDB Storage Engine: https://github.com/mongodb/mongo/tree/master/src/mongo/db/storage/wiredtiger
- Go Iterator Pattern: https://pkg.go.dev/iter
- B-tree Fundamentals: (include relevant papers/links)
