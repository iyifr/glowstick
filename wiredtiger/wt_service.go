package wiredtiger

// Service provides a minimal API for interacting with WiredTiger.
// This abstracts the underlying cgo implementation to allow testing and !cgo builds.
type Service interface {
	Open(home string, config string) error
	Close() error
	CreateTable(name string, config string) error
	PutString(table string, key string, value string) error
	GetString(table string, key string) (value string, found bool, err error)
	DeleteString(table string, key string) error
	Exists(table string, key string) (bool, error)
	Scan(table string) ([]KeyValuePair, error)
	SearchNear(table string, probeKey string) (key string, value string, exact int, found bool, err error)
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
