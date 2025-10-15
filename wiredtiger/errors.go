package wiredtiger

import "errors"

func errNoCgo() error {
	return errors.New("wiredtiger: cgo disabled or headers not found; enable cgo and install WiredTiger headers")
}
