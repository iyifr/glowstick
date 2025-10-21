# WiredTiger Go Service Layer

Provides a minimal, high-performance Go interface to the WiredTiger storage engine via CGO. Exposes both string and binary key-value tables, optimized for embeddable document/database workloads requiring scalable, transactional tables.

This abstraction is intentionally generic and low-level: it enables fast, type-agnostic persistence for services or libraries that need modern key-value or document storage. It does not implement query languages, indexing, or domain-specific logic—these belong at higher application layers.

## Service Methods

All APIs are on the `WTService` interface.

**Connection & Table:**

- `Open(home string, config string) error` — Open/create a database at a directory.
- `Close() error` — Close the current database connection.
- `CreateTable(name string, config string) error` — Create a table (string or binary keys/values, configurable).

**String Key/Value Operations:**

- `PutString(table, key, value string) error` — Set value for key.
- `GetString(table, key string) (string, bool, error)` — Retrieve key; returns value, found, err.
- `DeleteString(table, key string) error` — Remove key.
- `Exists(table, key string) (bool, error)` — Check key.
- `Scan(table string, threshold ...int) ([]KeyValuePair, error)` — Full table scan (optional limit).
- `SearchNear(table, probeKey string) (string, string, int, bool, error)` — Find nearest (or equal) key.

**Binary Key/Value Operations:**

- `PutBinary(table string, key, value []byte) error`
- `GetBinary(table string, key []byte) ([]byte, bool, error)`
- `DeleteBinary(table string, key []byte) error`
- `ExistsBinary(table string, key []byte) (bool, error)`
- `ScanBinary(table string) ([]BinaryKeyValuePair, error)`
- `SearchNearBinary(table string, probeKey []byte) ([]byte, []byte, int, bool, error)`

**Convenience (String<->Binary Mapping):**

- `PutBinaryWithStringKey(table, stringKey string, value []byte) error`
- `GetBinaryWithStringKey(table, stringKey string) ([]byte, bool, error)`
- `DeleteBinaryWithStringKey(table, stringKey string) error`

**Range/Cursor Scans:**

- `ScanRange(table, startKey, endKey string) (StringRangeCursor, error)`
- `ScanRangeBinary(table string, startKey, endKey []byte) (BinaryRangeCursor, error)`

---

See official [WiredTiger documentation](http://source.wiredtiger.com/) for details on configuration options, table types, and low-level tuning.
