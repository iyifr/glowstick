//go:build cgo

package wiredtiger

/*
#cgo CFLAGS: -Wno-incompatible-pointer-types-discards-qualifiers
#cgo darwin CFLAGS: -I/usr/local/include
#cgo darwin LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -lwiredtiger
#cgo linux CFLAGS: -I/usr/local/include
#cgo linux LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -Wl,-rpath,/usr/lib -Wl,-rpath,/usr/lib/x86_64-linux-gnu -lwiredtiger
#include <stdlib.h>
#include <string.h>
#include <wiredtiger.h>

// ============================================================================
// CONNECTION OPERATIONS
// ============================================================================

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

// ============================================================================
// STRING KEY/VALUE OPERATIONS
// ============================================================================

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

// ============================================================================
// BINARY KEY/VALUE OPERATIONS
// ============================================================================

static int wt_put_bin(WT_CONNECTION *conn, const char* uri,
                      const unsigned char* key, size_t key_len,
                      const unsigned char* val, size_t val_len) {
	if (!conn || !uri || !key || !val) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    WT_ITEM val_item;
    val_item.data = (void*)val;
    val_item.size = val_len;
    cursor->set_value(cursor, &val_item);

    err = cursor->insert(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_get_bin(WT_CONNECTION *conn, const char* uri,
                      const unsigned char* key, size_t key_len,
                      WT_ITEM *outVal) {
	if (!conn || !uri || !key || !outVal) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->search(cursor);
    if (err != 0) { cursor->close(cursor); session->close(session, NULL); return err; }

    WT_ITEM *val;
    err = cursor->get_value(cursor, &val);
    if (err == 0) {
        outVal->data = malloc(val->size);
        if (!outVal->data) {
            cursor->close(cursor);
            session->close(session, NULL);
            return -1;
        }
       memcpy(outVal->data, (void*)val->data, val->size);
	   outVal->size = val->size;
    }

    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_del_bin(WT_CONNECTION *conn, const char* uri,
                      const unsigned char* key, size_t key_len) {
	if (!conn || !uri || !key) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->remove(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_exists_bin(WT_CONNECTION *conn, const char* uri,
                         const unsigned char* key, size_t key_len,
                         int *found) {
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

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->search(cursor);
    if (err == 0) *found = 1;

    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0 && err != WT_NOTFOUND) return err;
    if (cerr != 0) return cerr;
    return serr;
}

// ============================================================================
// SCAN OPERATIONS
// ============================================================================

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

typedef struct {
    unsigned char **keys;
    size_t *key_lens;
    unsigned char **vals;
    size_t *val_lens;
    int len;
} wt_bin_vec_t;

static void wt_bin_vec_free(wt_bin_vec_t vec) {
    if (vec.keys) {
        for (int i = 0; i < vec.len; i++) if (vec.keys[i]) free(vec.keys[i]);
        free(vec.keys);
    }
    if (vec.vals) {
        for (int i = 0; i < vec.len; i++) if (vec.vals[i]) free(vec.vals[i]);
        free(vec.vals);
    }
    if (vec.key_lens) free(vec.key_lens);
    if (vec.val_lens) free(vec.val_lens);
}

static int wt_scan_bin_collect(WT_CONNECTION *conn, const char* uri, int max, wt_bin_vec_t *out) {
	if (!conn || !uri || !out) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = 0;
    memset(out, 0, sizeof(*out));

    out->keys = (unsigned char**)calloc((size_t)max, sizeof(unsigned char*));
    out->vals = (unsigned char**)calloc((size_t)max, sizeof(unsigned char*));
    out->key_lens = (size_t*)calloc((size_t)max, sizeof(size_t));
    out->val_lens = (size_t*)calloc((size_t)max, sizeof(size_t));

    if (!out->keys || !out->vals || !out->key_lens || !out->val_lens) {
        err = -1;
        goto done;
    }

    err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) goto done;
    if (!session) { err = -1; goto done; }

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) goto done;
    if (!cursor) { err = -1; goto done; }

    WT_ITEM *k; WT_ITEM *v;
    while ((err = cursor->next(cursor)) == 0) {
        if (cursor->get_key(cursor, &k) != 0) { err = -1; break; }
        if (cursor->get_value(cursor, &v) != 0) { err = -1; break; }
        if (out->len >= max) break;

        out->keys[out->len] = (unsigned char*)malloc(k->size);
        if (!out->keys[out->len]) { err = -1; break; }
        memcpy(out->keys[out->len], k->data, k->size);
        out->key_lens[out->len] = k->size;

        out->vals[out->len] = (unsigned char*)malloc(v->size);
        if (!out->vals[out->len]) { err = -1; break; }
        memcpy(out->vals[out->len], v->data, v->size);
        out->val_lens[out->len] = v->size;

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

static int wt_search_near_bin(WT_CONNECTION *conn, const char* uri,
                              const unsigned char* key, size_t key_len,
                              WT_ITEM *outKey, WT_ITEM *outVal, int *exact) {
	if (!conn || !uri || !key || !outKey || !outVal || !exact) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->search_near(cursor, exact);
    if (err == 0) {
        WT_ITEM *k; WT_ITEM *v;
        if (cursor->get_key(cursor, &k) == 0 && cursor->get_value(cursor, &v) == 0) {
            outKey->data = malloc(k->size);
            if (outKey->data) {
                memcpy(outKey->data, k->data, k->size);
                outKey->size = k->size;
            }
            outVal->data = malloc(v->size);
            if (outVal->data) {
                memcpy(outVal->data, v->data, v->size);
                outVal->size = v->size;
            }
        }
    }

    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (cerr != 0) return cerr;
    return err != 0 ? err : serr;
}

// ============================================================================
// RANGE SCAN OPERATIONS (string keys)
// ============================================================================

typedef struct {
    WT_SESSION *session;
    WT_CURSOR *cursor;
    char *end_key;   // malloc'd, for bounds
    int   err;
    int   closed;
    int   valid;     // 1 if positioned at valid entry
    int   in_bounds; // 1 if still in user range
} wt_range_ctx_t;

static wt_range_ctx_t* wt_range_scan_init_str(WT_CONNECTION *conn, const char* uri, const char* start_key, const char* end_key) {
    wt_range_ctx_t *ctx = malloc(sizeof(wt_range_ctx_t));
    if (!ctx) return NULL;
    memset(ctx, 0, sizeof(*ctx));
    ctx->err = 0; ctx->closed = 0; ctx->valid = 0; ctx->in_bounds = 1;
    if (!conn || !uri || !start_key || !end_key) { free(ctx); return NULL; }
    ctx->session = NULL;
    ctx->cursor = NULL;
    ctx->end_key = strdup(end_key);
    // Open session
    int err = conn->open_session(conn, NULL, NULL, &ctx->session);
    if (err != 0 || !ctx->session) { free(ctx->end_key); free(ctx); return NULL; }
    // Open cursor
    err = ctx->session->open_cursor(ctx->session, uri, NULL, NULL, &ctx->cursor);
    if (err != 0 || !ctx->cursor) {
        ctx->session->close(ctx->session, NULL);
        free(ctx->end_key); free(ctx); return NULL;
    }
    // Position at start_key (with search_near pattern)
    ctx->cursor->set_key(ctx->cursor, start_key);
    int exact = 0;
    err = ctx->cursor->search_near(ctx->cursor, &exact);

    if (err != 0 && err != WT_NOTFOUND) {
        // Actual error
        ctx->cursor->close(ctx->cursor);
        ctx->session->close(ctx->session, NULL);
        free(ctx->end_key); free(ctx); return NULL;
    }

    // If exact < 0, we're before the start key
    // Call next() to position at the first key >= start_key
    if (exact < 0) {
        err = ctx->cursor->next(ctx->cursor);
        if (err != 0 && err != WT_NOTFOUND) {
            ctx->cursor->close(ctx->cursor);
            ctx->session->close(ctx->session, NULL);
            free(ctx->end_key); free(ctx); return NULL;
        }
    }

    // Now cursor is positioned at:
    // - exact match of start_key, OR
    // - first key > start_key (if no exact match)
    // Check bounds: current < end_key?
    const char *curr;
    if (ctx->cursor->get_key(ctx->cursor, &curr) == 0) {
        if (strcmp(curr, ctx->end_key) >= 0) {
            ctx->in_bounds = 0;
            ctx->valid = 0;
        } else {
            ctx->in_bounds = 1;
            ctx->valid = 1;
        }
    } else {
        ctx->in_bounds = 0;
        ctx->valid = 0;
    }
    return ctx;
}

static int wt_range_scan_next(wt_range_ctx_t* ctx, const char **out_key, const char **out_val, int* in_bounds) {
    if (!ctx || ctx->closed) return -1;
    int err = ctx->cursor->next(ctx->cursor);
    if (err != 0) { ctx->valid = 0; ctx->in_bounds = 0; *in_bounds = 0; return err; }
    const char *key = NULL; const char* val = NULL;
    ctx->cursor->get_key(ctx->cursor, &key);
    ctx->cursor->get_value(ctx->cursor, &val);
    // Check bounds: key < end_key
    if (strcmp(key, ctx->end_key) >= 0) {
        ctx->in_bounds = 0;
        ctx->valid = 0;
        *in_bounds = 0;
        return 1; // not an error, just OOB
    }
    ctx->in_bounds = 1; ctx->valid = 1;
    *out_key = key;
    *out_val = val;
    *in_bounds = 1;
    return 0;
}

// Retrieve current key/val (no iteration/movement)
static int wt_range_scan_current(wt_range_ctx_t* ctx, const char **out_key, const char **out_val) {
    if (!ctx || ctx->closed || !ctx->valid) return -1;
    return ctx->cursor->get_key(ctx->cursor, out_key) == 0 && ctx->cursor->get_value(ctx->cursor, out_val) == 0 ? 0 : -1;
}

// Close/finalize & free ctx and internal resources
static void wt_range_scan_close(wt_range_ctx_t* ctx) {
    if (!ctx || ctx->closed) return;
    if (ctx->cursor) ctx->cursor->close(ctx->cursor);
    if (ctx->session) ctx->session->close(ctx->session, NULL);
    if (ctx->end_key) free(ctx->end_key);
    ctx->closed = 1;
    free(ctx);
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

func WiredTigerService() WTService { return &cgoService{} }

// ============================================================================
// CONNECTION OPERATIONS
// ============================================================================

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
	cname := C.CString(name)
	cconfig := C.CString(config)
	defer C.free(unsafe.Pointer(cname))
	defer C.free(unsafe.Pointer(cconfig))
	err := C.wt_create_wrap(s.conn, cname, cconfig)
	if err != 0 {
		return fmt.Errorf("wiredtiger create failed with error code %d", int(err))
	}
	return nil
}

// ============================================================================
// STRING KEY/VALUE OPERATIONS (existing)
// ============================================================================

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
	if err != 0 && err != C.int(-31804) {
		return false, fmt.Errorf("wiredtiger exists failed with error code %d", int(err))
	}
	return found == 1, nil
}

// Scan returns up to a given threshold (default 4096 if threshold is 0 or less) rows from the table.
// If threshold is provided as >0, use it. Otherwise, default to 4096.
func (s *cgoService) Scan(table string, threshold ...int) ([]KeyValuePair, error) {
	limit := 4096
	if len(threshold) > 0 && threshold[0] > 0 {
		limit = threshold[0]
	}
	return s.ScanBatch(table, 0, limit)
}

// ScanBatch enables batched scanning of results.
// Offset is currently not implemented due to WiredTiger API limitations and is for API compatibility.
// Limit sets the maximum number of results returned.
func (s *cgoService) ScanBatch(table string, offset int, limit int) ([]KeyValuePair, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}
	if limit <= 0 {
		return []KeyValuePair{}, nil
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))
	var vec C.wt_vec_t
	err := C.wt_scan_collect(s.conn, curi, C.int(limit), &vec)
	if err != 0 {
		return nil, fmt.Errorf("wiredtiger scan failed with error code %d", int(err))
	}
	n := int(vec.len)
	out := make([]KeyValuePair, 0, n)
	keys := (*[1 << 28]*C.char)(unsafe.Pointer(vec.keys))[:n:n]
	vals := (*[1 << 28]*C.char)(unsafe.Pointer(vec.vals))[:n:n]
	for i := 0; i < n; i++ {
		out = append(out, KeyValuePair{Key: C.GoString(keys[i]), Value: C.GoString(vals[i])})
	}
	C.wt_vec_free(vec)
	return out, nil
}

func (s *cgoService) ExistsBinary(table string, key []byte) (bool, error) {
	if s.conn == nil {
		return false, errors.New("connection not open")
	}
	if len(key) == 0 {
		return false, errors.New("key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var found C.int
	err := C.wt_exists_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)), &found)
	if err != 0 && err != C.int(-31804) {
		return false, fmt.Errorf("wiredtiger binary exists failed with error code %d", int(err))
	}
	return found == 1, nil
}

func (s *cgoService) ScanBinary(table string) ([]BinaryKeyValuePair, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var vec C.wt_bin_vec_t
	err := C.wt_scan_bin_collect(s.conn, curi, C.int(4096), &vec)
	if err != 0 {
		return nil, fmt.Errorf("wiredtiger binary scan failed with error code %d", int(err))
	}

	n := int(vec.len)
	out := make([]BinaryKeyValuePair, 0, n)

	// Convert C arrays to Go slices
	keys := (*[1 << 28]*C.uchar)(unsafe.Pointer(vec.keys))[:n:n]
	vals := (*[1 << 28]*C.uchar)(unsafe.Pointer(vec.vals))[:n:n]
	keyLens := (*[1 << 28]C.size_t)(unsafe.Pointer(vec.key_lens))[:n:n]
	valLens := (*[1 << 28]C.size_t)(unsafe.Pointer(vec.val_lens))[:n:n]

	for i := 0; i < n; i++ {
		keyBytes := C.GoBytes(unsafe.Pointer(keys[i]), C.int(keyLens[i]))
		valBytes := C.GoBytes(unsafe.Pointer(vals[i]), C.int(valLens[i]))
		out = append(out, BinaryKeyValuePair{Key: keyBytes, Value: valBytes})
	}

	C.wt_bin_vec_free(vec)
	return out, nil
}

func (s *cgoService) SearchNearBinary(table string, probeKey []byte) ([]byte, []byte, int, bool, error) {
	if s.conn == nil {
		return nil, nil, 0, false, errors.New("connection not open")
	}
	if len(probeKey) == 0 {
		return nil, nil, 0, false, errors.New("probe key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var outKey C.WT_ITEM
	var outVal C.WT_ITEM
	var exact C.int

	err := C.wt_search_near_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&probeKey[0])), C.size_t(len(probeKey)),
		&outKey, &outVal, &exact)
	if err != 0 {
		return nil, nil, 0, false, fmt.Errorf("wiredtiger binary search_near failed with error code %d", int(err))
	}

	// Copy C data to Go slices
	keyBytes := C.GoBytes(unsafe.Pointer(outKey.data), C.int(outKey.size))
	valBytes := C.GoBytes(unsafe.Pointer(outVal.data), C.int(outVal.size))
	C.free(outKey.data)
	C.free(outVal.data)

	return keyBytes, valBytes, int(exact), true, nil
}

// ============================================================================
// STRING OPERATIONS (existing)
// ============================================================================

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

// ============================================================================
// BINARY KEY/VALUE OPERATIONS
// ============================================================================

func (s *cgoService) PutBinary(table string, key []byte, value []byte) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	if len(key) == 0 || len(value) == 0 {
		return errors.New("key and value cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	err := C.wt_put_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.uchar)(unsafe.Pointer(&value[0])), C.size_t(len(value)))
	if err != 0 {
		return fmt.Errorf("wiredtiger binary put failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) GetBinary(table string, key []byte) ([]byte, bool, error) {
	if s.conn == nil {
		return nil, false, errors.New("connection not open")
	}
	if len(key) == 0 {
		return nil, false, errors.New("key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var outVal C.WT_ITEM
	err := C.wt_get_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)), &outVal)
	if err != 0 {
		return nil, false, nil
	}

	// Copy C data to Go slice
	result := C.GoBytes(unsafe.Pointer(outVal.data), C.int(outVal.size))
	C.free(outVal.data)

	return result, true, nil
}

func (s *cgoService) DeleteBinary(table string, key []byte) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	err := C.wt_del_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	if err != 0 {
		return fmt.Errorf("wiredtiger binary delete failed with error code %d", int(err))
	}
	return nil
}

// ============================================================================
// CONVENIENCE HELPER FUNCTIONS
// ============================================================================

// PutBinaryWithStringKey is a convenience function for composite keys
// Useful for index operations where key is composite of field values + doc ID
func (s *cgoService) PutBinaryWithStringKey(table string, stringKey string, value []byte) error {
	keyBytes := []byte(stringKey)
	return s.PutBinary(table, keyBytes, value)
}

// GetBinaryWithStringKey is a convenience function
// Retrieves binary value using a string key
func (s *cgoService) GetBinaryWithStringKey(table string, stringKey string) ([]byte, bool, error) {
	keyBytes := []byte(stringKey)
	return s.GetBinary(table, keyBytes)
}

// DeleteBinaryWithStringKey is a convenience function
// Deletes a key-value pair using a string key
func (s *cgoService) DeleteBinaryWithStringKey(table string, stringKey string) error {
	keyBytes := []byte(stringKey)
	return s.DeleteBinary(table, keyBytes)
}

// ============================================================================
// RANGE SCAN OPERATIONS
// ============================================================================

type stringRangeCursor struct {
	ctx       *C.wt_range_ctx_t
	err       error
	closed    bool
	valid     bool
	inBounds  bool
	firstCall bool
	currKey   string
	currVal   string
}

func (c *stringRangeCursor) Next() bool {
	if c.closed || c.err != nil || c.ctx == nil {
		c.valid = false
		return false
	}

	// On first call, cursor is already positioned at the first valid result
	if c.firstCall {
		c.firstCall = false
		// Get current position without advancing
		var ck, cv *C.char
		errCode := C.wt_range_scan_current(c.ctx, &ck, &cv)
		if errCode != 0 {
			c.err = errors.New("range scan current failed")
			c.valid = false
			return false
		}
		c.currKey = C.GoString(ck)
		c.currVal = C.GoString(cv)
		c.valid = true
		return true
	}

	// Subsequent calls: advance to next position
	var ck, cv *C.char
	var inBounds C.int
	errCode := C.wt_range_scan_next(c.ctx, &ck, &cv, &inBounds)
	if errCode != 0 && errCode != 1 { // 1 = not error, just OOB
		c.err = errors.New("range scan next failed")
		c.valid = false
		return false
	}
	if inBounds == 0 {
		c.valid = false
		return false
	}
	c.currKey = C.GoString(ck)
	c.currVal = C.GoString(cv)
	c.valid = true
	return true
}

func (c *stringRangeCursor) CurrentString() (string, string, error) {
	if c.closed || c.ctx == nil || !c.valid {
		return "", "", errors.New("cursor not positioned")
	}
	var ck, cv *C.char
	errCode := C.wt_range_scan_current(c.ctx, &ck, &cv)
	if errCode != 0 {
		return "", "", errors.New("range scan current failed")
	}
	return C.GoString(ck), C.GoString(cv), nil
}

func (c *stringRangeCursor) Err() error { return c.err }
func (c *stringRangeCursor) Close() error {
	if c.closed || c.ctx == nil {
		return nil
	}
	C.wt_range_scan_close(c.ctx)
	c.closed = true
	return nil
}
func (c *stringRangeCursor) Valid() bool { return c.valid }

// ScanRange creates a cursor for iterating over string keys in the range [startKey, endKey)
func (s *cgoService) ScanRange(table, startKey, endKey string) (StringRangeCursor, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}
	ctable := C.CString(table)
	cstart := C.CString(startKey)
	cend := C.CString(endKey)
	defer C.free(unsafe.Pointer(ctable))
	defer C.free(unsafe.Pointer(cstart))
	defer C.free(unsafe.Pointer(cend))
	ctx := C.wt_range_scan_init_str(s.conn, ctable, cstart, cend)
	if ctx == nil {
		return nil, errors.New("failed to initialize string range scan")
	}
	out := &stringRangeCursor{ctx: ctx, valid: true, inBounds: true, firstCall: true}
	return out, nil
}
