package wiredtiger

// Service provides a minimal API for interacting with WiredTiger.
// This abstracts the underlying cgo implementation to allow testing and !cgo builds.
type Service interface {
	Open(home string, config string) error
	Close() error
	CreateTable(name string, config string) error
	PutString(table string, key string, value string) error
	GetString(table string, key string) (string, bool, error)
	DeleteString(table string, key string) error
	Exists(table string, key string) (bool, error)
	Scan(table string) ([]KeyValuePair, error)
	SearchNear(table string, probeKey string) (string, string, int, bool, error)
	PutBinary(table string, key []byte, value []byte) error
	GetBinary(table string, key []byte) ([]byte, bool, error)
	DeleteBinary(table string, key []byte) error
	ExistsBinary(table string, key []byte) (bool, error)
	ScanBinary(table string) ([]BinaryKeyValuePair, error)
	SearchNearBinary(table string, probeKey []byte) ([]byte, []byte, int, bool, error)
	PutBinaryWithStringKey(table string, stringKey string, value []byte) error
	GetBinaryWithStringKey(table string, stringKey string) ([]byte, bool, error)
	DeleteBinaryWithStringKey(table string, stringKey string) error
}

// New returns a Service implementation backed by cgo (when enabled).
func New() Service {
	return newService()
}

// KeyValuePair represents a string key/value row.
type KeyValuePair struct {
	Key   string
	Value string
}
type BinaryKeyValuePair struct {
	Key   []byte
	Value []byte
}
