//go:build cgo

package wiredtiger

/*
#cgo darwin CFLAGS: -I/usr/local/include
#cgo darwin LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -lwiredtiger
#cgo linux CFLAGS: -I/usr/local/include
#cgo linux LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -Wl,-rpath,/usr/lib -Wl,-rpath,/usr/lib/x86_64-linux-gnu -lwiredtiger
#include <stdlib.h>
#include <string.h>
#include <wiredtiger.h>

static int wt_open_wrap(const char* home, const char* config, WT_CONNECTION **conn_out) {
	if (!home || !config || !conn_out) return -1;
	return wiredtiger_open(home, NULL, config, conn_out);
}

static int wt_close_wrap(WT_CONNECTION *conn) {
	if (!conn) return -1;
	return conn->close(conn, NULL);
}

static int wt_create_wrap(WT_CONNECTION *conn, const char* name, const char* config) {
	if (!conn || !name || !config) return -1;
	WT_SESSION *session = NULL;
	int err = conn->open_session(conn, NULL, NULL, &session);
	if (err != 0) return err;
	if (!session) return -1;
	err = session->create(session, name, config);
	int cerr = session->close(session, NULL);
	return err != 0 ? err : cerr;
}

static int wt_put_str(WT_CONNECTION *conn, const char* uri, const char* key, const char* val) {
	if (!conn || !uri || !key || !val) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, val);
    err = cursor->insert(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_get_str(WT_CONNECTION *conn, const char* uri, const char* key, const char **outVal) {
	if (!conn || !uri || !key || !outVal) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->search(cursor);
    if (err != 0) { cursor->close(cursor); session->close(session, NULL); return err; }
    const char *val;
    err = cursor->get_value(cursor, &val);
    if (err == 0) { *outVal = val; }
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_del_str(WT_CONNECTION *conn, const char* uri, const char* key) {
	if (!conn || !uri || !key) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->remove(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_exists_str(WT_CONNECTION *conn, const char* uri, const char* key, int *found) {
	if (!conn || !uri || !key || !found) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    *found = 0;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->search(cursor);
    if (err == 0) *found = 1;
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0 && err != WT_NOTFOUND) return err;
    if (cerr != 0) return cerr;
    return serr;
}

typedef struct {
    char **keys;
    char **vals;
    int len;
} wt_vec_t;

static void wt_vec_free(wt_vec_t vec) {
    if (vec.keys) {
        for (int i = 0; i < vec.len; i++) if (vec.keys[i]) free(vec.keys[i]);
        free(vec.keys);
    }
    if (vec.vals) {
        for (int i = 0; i < vec.len; i++) if (vec.vals[i]) free(vec.vals[i]);
        free(vec.vals);
    }
}

static int wt_scan_collect(WT_CONNECTION *conn, const char* uri, int max, wt_vec_t *out) {
	if (!conn || !uri || !out) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = 0;
    memset(out, 0, sizeof(*out));
    out->keys = (char**)calloc((size_t)max, sizeof(char*));
    out->vals = (char**)calloc((size_t)max, sizeof(char*));
    if (!out->keys || !out->vals) { err = -1; goto done; }
    err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) goto done;
    if (!session) { err = -1; goto done; }
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) goto done;
    if (!cursor) { err = -1; goto done; }
    const char *k; const char *v;
    while ((err = cursor->next(cursor)) == 0) {
        if (cursor->get_key(cursor, &k) != 0) { err = -1; break; }
        if (cursor->get_value(cursor, &v) != 0) { err = -1; break; }
        if (out->len >= max) break;
        out->keys[out->len] = strdup(k);
        out->vals[out->len] = strdup(v);
        if (!out->keys[out->len] || !out->vals[out->len]) { err = -1; break; }
        out->len++;
    }
    if (err == WT_NOTFOUND) err = 0;
done:
    if (cursor) cursor->close(cursor);
    if (session) session->close(session, NULL);
    return err;
}

static int wt_search_near_str(WT_CONNECTION *conn, const char* uri, const char* key,
                              const char **outKey, const char **outVal, int *exact) {
	if (!conn || !uri || !key || !outKey || !outVal || !exact) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->search_near(cursor, exact);
    if (err == 0) {
        const char *k; const char *v;
        if (cursor->get_key(cursor, &k) == 0 && cursor->get_value(cursor, &v) == 0) {
            *outKey = k; *outVal = v;
        }
    }
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (cerr != 0) return cerr;
    return err != 0 ? err : serr;
}
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

type cgoService struct {
	conn *C.WT_CONNECTION
}

func newService() Service { return &cgoService{} }

func (s *cgoService) Open(home string, config string) error {
	chome := C.CString(home)
	cconfig := C.CString(config)
	defer C.free(unsafe.Pointer(chome))
	defer C.free(unsafe.Pointer(cconfig))
	var conn *C.WT_CONNECTION
	err := C.wt_open_wrap(chome, cconfig, &conn)
	if err != 0 {
		return fmt.Errorf("wiredtiger_open failed with error code %d", int(err))
	}
	if conn == nil {
		return errors.New("wiredtiger_open returned nil connection")
	}
	s.conn = conn
	return nil
}

func (s *cgoService) Close() error {
	if s.conn == nil {
		return nil
	}
	err := C.wt_close_wrap(s.conn)
	s.conn = nil
	if err != 0 {
		return fmt.Errorf("wiredtiger close failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) CreateTable(name string, config string) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	fmt.Printf("DEBUG: s.conn pointer = %p\n", s.conn)
	cname := C.CString(name)
	cconfig := C.CString(config)
	defer C.free(unsafe.Pointer(cname))
	defer C.free(unsafe.Pointer(cconfig))
	fmt.Printf("DEBUG: About to call wt_create_wrap with conn=%p, name=%s, config=%s\n", s.conn, name, config)
	err := C.wt_create_wrap(s.conn, cname, cconfig)
	fmt.Printf("DEBUG: wt_create_wrap returned %d\n", int(err))
	if err != 0 {
		return fmt.Errorf("wiredtiger create failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) PutString(table string, key string, value string) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	cval := C.CString(value)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cval))
	err := C.wt_put_str(s.conn, curi, ckey, cval)
	if err != 0 {
		return fmt.Errorf("wiredtiger put failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) GetString(table string, key string) (string, bool, error) {
	if s.conn == nil {
		return "", false, errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	var cval *C.char
	err := C.wt_get_str(s.conn, curi, ckey, &cval)
	if err != 0 {
		return "", false, nil
	}
	return C.GoString(cval), true, nil
}

func (s *cgoService) SearchNear(table string, probeKey string) (string, string, int, bool, error) {
	if s.conn == nil {
		return "", "", 0, false, errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(probeKey)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	var outKey *C.char
	var outVal *C.char
	var exact C.int
	err := C.wt_search_near_str(s.conn, curi, ckey, &outKey, &outVal, &exact)
	if err != 0 {
		return "", "", 0, false, fmt.Errorf("wiredtiger search_near failed with error code %d", int(err))
	}
	return C.GoString(outKey), C.GoString(outVal), int(exact), true, nil
}

func (s *cgoService) DeleteString(table string, key string) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	err := C.wt_del_str(s.conn, curi, ckey)
	if err != 0 {
		return fmt.Errorf("wiredtiger delete failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) Exists(table string, key string) (bool, error) {
	if s.conn == nil {
		return false, errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	var found C.int
	err := C.wt_exists_str(s.conn, curi, ckey, &found)
	if err != 0 && err != C.int(-31804) { // tolerate WT_NOTFOUND if headers not available
		return false, fmt.Errorf("wiredtiger exists failed with error code %d", int(err))
	}
	return found == 1, nil
}

func (s *cgoService) Scan(table string) ([]KeyValuePair, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))
	var vec C.wt_vec_t
	err := C.wt_scan_collect(s.conn, curi, C.int(4096), &vec)
	if err != 0 {
		return nil, fmt.Errorf("wiredtiger scan failed with error code %d", int(err))
	}
	n := int(vec.len)
	out := make([]KeyValuePair, 0, n)
	// Convert C arrays of char* to Go strings
	keys := (*[1 << 28]*C.char)(unsafe.Pointer(vec.keys))[:n:n]
	vals := (*[1 << 28]*C.char)(unsafe.Pointer(vec.vals))[:n:n]
	for i := 0; i < n; i++ {
		out = append(out, KeyValuePair{Key: C.GoString(keys[i]), Value: C.GoString(vals[i])})
	}
	C.wt_vec_free(vec)
	return out, nil
}
