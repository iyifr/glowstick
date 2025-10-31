# WiredTiger Go Layer

Provides a minimal Go interface to the WiredTiger storage engine via CGO. Exposes both string and binary key-value tables.

## Service Methods

All APIs are on the `WTService` interface.

**Connection & Table:**

- `Open(home string, config string) error` — Open/create a database at a directory.
- `Close() error` — Close the current database connection.
- `CreateTable(name string, config string) error` — Create a table (string or binary keys/values, configurable with config string).

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

**(String<->Binary Mapping):**
Store a string key with a binary value.

- `PutBinaryWithStringKey(table, stringKey string, value []byte) error`
- `GetBinaryWithStringKey(table, stringKey string) ([]byte, bool, error)`
- `DeleteBinaryWithStringKey(table, stringKey string) error`

**Range/Cursor Scans:**

- `ScanRange(table, startKey, endKey string) (StringRangeCursor, error)`
- `ScanRangeBinary(table string, startKey, endKey []byte) (BinaryRangeCursor, error)`

---
