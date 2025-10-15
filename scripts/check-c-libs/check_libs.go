//go:build cgo

package main

/*
#cgo darwin CFLAGS: -I/usr/local/include -I/opt/homebrew/include
#cgo darwin LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -Wl,-rpath,/opt/homebrew/lib
#cgo linux CFLAGS: -I/usr/local/include
#cgo linux LDFLAGS: -ldl -L/usr/local/lib -Wl,-rpath,/usr/local/lib -Wl,-rpath,/usr/lib -Wl,-rpath,/usr/lib/x86_64-linux-gnu
#include <stdlib.h>
#include <dlfcn.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func CheckLibrary(name string) bool {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	h := C.dlopen(cname, C.RTLD_LAZY)
	if h == nil {
		return false
	}
	C.dlclose(h)
	return true
}

func main() {
	faissAvailable := false
	for _, lib := range []string{"libfaiss_c.dylib", "libfaiss_c.so"} {
		if CheckLibrary(lib) {
			faissAvailable = true
			break
		}
	}
	if faissAvailable {
		fmt.Println("Faiss library is available.")
	} else {
		fmt.Println("Faiss library is not available.")
	}

	wiredTigerAvailable := false
	for _, lib := range []string{"libwiredtiger.dylib", "libwiredtiger.so"} {
		if CheckLibrary(lib) {
			wiredTigerAvailable = true
			break
		}
	}
	if wiredTigerAvailable {
		fmt.Println("WiredTiger library is available.")
	} else {
		fmt.Println("WiredTiger library is not available.")
	}
}
