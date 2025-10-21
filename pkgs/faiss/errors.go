package faiss

import "errors"

func errNoCgo() error {
	return errors.New("FAISS functionality requires cgo build")
}
