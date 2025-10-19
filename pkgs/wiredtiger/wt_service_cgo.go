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
    int   err;
    int   valid;     // 1 if positioned at valid entry
    int   in_range; // 1 if still in user range
    char *end_key;   // malloc'd, for bounds
} wt_range_ctx_t;

static wt_range_ctx_t* wt_range_scan_init_str(WT_CONNECTION *conn, const char* uri, const char* start_key, const char* end_key) {
    wt_range_ctx_t *ctx = malloc(sizeof(wt_range_ctx_t));
    if (!ctx) return NULL;
    memset(ctx, 0, sizeof(*ctx));
    ctx->err = 0; ctx->valid = 0; ctx->in_range = 1;
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
    printf("%d\n", exact);
    switch (exact) {
    case -1: {
        // search_near landed before start_key; advance to first key > start_key
        printf("range_scan_init_str: search_near result -1, calling reset+next to first real key...\n");
        int err_reset = ctx->cursor->reset(ctx->cursor);
        if (err_reset != 0) {
            printf("range_scan_init_str: reset failed, err=%d\n", err_reset);
            ctx->cursor->close(ctx->cursor);
            ctx->session->close(ctx->session, NULL);
            free(ctx->end_key); free(ctx);
            return NULL;
        }
        // After reset, must call next() and check bounds
        int err_next = ctx->cursor->next(ctx->cursor);
        if (err_next != 0) {
            printf("range_scan_init_str: next after reset failed, no valid key (err=%d)\n", err_next);
            ctx->in_range = 0;
            ctx->valid = 0;
            // Still return ctx so caller sees empty range, not NULL/fatal
            return ctx;
        }
        // Check bounds: key < end_key?
        const char *curr = NULL;
        if (ctx->cursor->get_key(ctx->cursor, &curr) == 0) {
            if (strcmp(curr, ctx->end_key) >= 0) {
                ctx->in_range = 0;
                ctx->valid = 0;
            } else {
                ctx->in_range = 1;
                ctx->valid = 1;
            }
        } else {
            ctx->in_range = 0;
            ctx->valid = 0;
        }
        printf("range_scan_init_str: after reset+next, valid=%d, in_range=%d\n", ctx->valid, ctx->in_range);
        return ctx;
    }

    case 0:
    case 1: {

        // Now cursor is positioned at:
        // - exact match of start_key, OR
        // - first key > start_key (if no exact match)
        // Check bounds: current < end_key?
        const char *curr;
        if (ctx->cursor->get_key(ctx->cursor, &curr) == 0) {
            if (strcmp(curr, ctx->end_key) >= 0) {
                ctx->in_range = 0;
                ctx->valid = 0;
            } else {
                ctx->in_range = 1;
                ctx->valid = 1;
            }
        } else {
            // Set BOOLS to false
            ctx->in_range = 0;
            ctx->valid = 0;
        }
        return ctx;
    }
    }
    ctx->cursor->close(ctx->cursor);
    ctx->session->close(ctx->session, NULL);
    free(ctx->end_key);
    free(ctx);
    return NULL;
}


static int wt_range_scan_next(wt_range_ctx_t* ctx, const char **out_key, const char **out_val, int* in_range) {
    if (!ctx) return -1;
    int err = ctx->cursor->next(ctx->cursor);
    if (err != 0) { ctx->valid = 0; ctx->in_range = 0; *in_range = 0; return err; }
    const char *key = NULL; const char* val = NULL;
    ctx->cursor->get_key(ctx->cursor, &key);
    ctx->cursor->get_value(ctx->cursor, &val);
    // Check bounds: key < end_key
    if (strcmp(key, ctx->end_key) >= 0) {
        ctx->in_range = 0;
        ctx->valid = 0;
        *in_range = 0;
        return 1; // not an error, just OOB
    }
    ctx->in_range = 1; ctx->valid = 1;
    *out_key = key;
    *out_val = val;
    *in_range = 1;
    return 0;
}

// Retrieve current key/val (no iteration/movement)
static int wt_range_scan_current(wt_range_ctx_t* ctx, const char **out_key, const char **out_val) {
    if (!ctx  || !ctx->valid) return -1;
    return ctx->cursor->get_key(ctx->cursor, out_key) == 0 && ctx->cursor->get_value(ctx->cursor, out_val) == 0 ? 0 : -1;
}

// Close/finalize & free ctx and internal resources
static void wt_range_scan_close(wt_range_ctx_t* ctx) {
    if (!ctx) return;
    if (ctx->cursor) ctx->cursor->close(ctx->cursor);
    if (ctx->session) ctx->session->close(ctx->session, NULL);
    if (ctx->end_key) free(ctx->end_key);
    free(ctx);
}

// ============================================================================
// BATCH RANGE SCAN OPERATIONS (string keys)
// ============================================================================

// Batch buffer structure for efficient data transfer
typedef struct {
    char *data;        // Packed key-value data
    size_t capacity;  // Total allocated capacity
    size_t length;    // Current data length
    size_t count;     // Number of records in buffer
} wt_batch_buf_t;

// Initialize batch buffer with initial capacity
static wt_batch_buf_t* wt_batch_buf_init(size_t initial_capacity) {
    wt_batch_buf_t *buf = malloc(sizeof(wt_batch_buf_t));
    if (!buf) return NULL;

    buf->data = malloc(initial_capacity);
    if (!buf->data) {
        free(buf);
        return NULL;
    }

    buf->capacity = initial_capacity;
    buf->length = 0;
    buf->count = 0;
    return buf;
}

// Ensure buffer has enough capacity for additional data
static int wt_batch_buf_ensure_capacity(wt_batch_buf_t *buf, size_t needed) {
    if (!buf) return -1;

    if (buf->length + needed <= buf->capacity) {
        return 0; // Already has enough capacity
    }

    // Double capacity strategy for amortized O(1) growth
    size_t new_capacity = buf->capacity * 2;
    while (new_capacity < buf->length + needed) {
        new_capacity *= 2;
    }

    char *new_data = realloc(buf->data, new_capacity);
    if (!new_data) return -1;

    buf->data = new_data;
    buf->capacity = new_capacity;
    return 0;
}

// Append a key-value pair to the batch buffer
// Format: [key_len (4 bytes)][key_data][val_len (4 bytes)][val_data]
static int wt_batch_buf_append_kv(wt_batch_buf_t *buf, const char *key, const char *val) {
    if (!buf || !key || !val) return -1;

    size_t key_len = strlen(key);
    size_t val_len = strlen(val);
    size_t total_needed = 4 + key_len + 4 + val_len; // lengths + data

    if (wt_batch_buf_ensure_capacity(buf, total_needed) != 0) {
        return -1;
    }

    char *ptr = buf->data + buf->length;

    // Write key length (little-endian uint32)
    uint32_t key_len_le = (uint32_t)key_len;
    memcpy(ptr, &key_len_le, 4);
    ptr += 4;

    // Write key data
    memcpy(ptr, key, key_len);
    ptr += key_len;

    // Write value length (little-endian uint32)
    uint32_t val_len_le = (uint32_t)val_len;
    memcpy(ptr, &val_len_le, 4);
    ptr += 4;

    // Write value data
    memcpy(ptr, val, val_len);
    ptr += val_len;

    buf->length = ptr - buf->data;
    buf->count++;
    return 0;
}

// Free batch buffer and all associated memory
static void wt_batch_buf_free(wt_batch_buf_t *buf) {
    if (!buf) return;
    if (buf->data) free(buf->data);
    free(buf);
}

// High-performance batch range scan implementation
// Fetches up to max_records key-value pairs in a single operation
static int wt_range_scan_next_batch(wt_range_ctx_t* ctx, int max_records,
                                   char **out_buf, int *out_buf_len, int *out_count) {
    if (!ctx || !out_buf || !out_buf_len || !out_count) {
        return -1;
    }

    // Initialize output parameters
    *out_buf = NULL;
    *out_buf_len = 0;
    *out_count = 0;

    // If cursor is not valid or out of range, return empty batch
    if (!ctx->valid || !ctx->in_range) {
        return 0; // Success, but empty batch
    }

    // Initialize batch buffer with reasonable initial capacity
    // Estimate: average 50 chars per key + 100 chars per value = ~150 bytes per record
    size_t estimated_size = max_records * 150;
    wt_batch_buf_t *batch_buf = wt_batch_buf_init(estimated_size);
    if (!batch_buf) {
        return -1; // Memory allocation failed
    }

    int records_fetched = 0;
    int err = 0;

    // Fetch records in batch
    for (int i = 0; i < max_records; i++) {
        const char *key = NULL;
        const char *val = NULL;

        // Get current key-value pair
        err = ctx->cursor->get_key(ctx->cursor, &key);
        if (err != 0) break;

        err = ctx->cursor->get_value(ctx->cursor, &val);
        if (err != 0) break;

        // Check bounds: key < end_key
        if (strcmp(key, ctx->end_key) >= 0) {
            ctx->in_range = 0;
            ctx->valid = 0;
            break; // Out of range, but not an error
        }

        // Append to batch buffer
        if (wt_batch_buf_append_kv(batch_buf, key, val) != 0) {
            err = -1;
            break;
        }

        records_fetched++;

        // Advance to next record
        err = ctx->cursor->next(ctx->cursor);
        if (err != 0) {
            // If we got some records before hitting an error, that's still success
            if (records_fetched > 0) {
                err = 0; // Treat as success with partial batch
            }
            break;
        }
    }

    // Set output parameters
    if (records_fetched > 0) {
        *out_buf = batch_buf->data;
        *out_buf_len = (int)batch_buf->length;
        *out_count = records_fetched;

        // Transfer ownership of data buffer to caller
        // Don't free batch_buf->data here - it will be freed by wt_free_batch_buf
        batch_buf->data = NULL; // Prevent double-free
    }

    // Always free the batch buffer struct
    wt_batch_buf_free(batch_buf);

    return err;
}

// Free a batch buffer returned by wt_range_scan_next_batch
static void wt_free_batch_buf(char *buf) {
    if (buf) {
        free(buf);
    }
}

*/
import "C"
import (
	"encoding/binary"
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
	firstCall bool

	// batchBuffer holds a batch of key-value pairs from the C layer.
	// The format is [key1_len (u32)][key1_data][val1_len (u32)][val1_data]...
	batchBuffer []byte
	// readOffset is the current reading position in batchBuffer.
	readOffset int

	// Current key and value parsed from the batch.
	currKey string
	currVal string
}

func (c *stringRangeCursor) Next() bool {
	if c.closed || c.err != nil || c.ctx == nil {
		c.valid = false
		return false
	}

	// If the buffer is fully read, fetch the next batch.
	if c.readOffset >= len(c.batchBuffer) {
		if err := c.fetchNextBatch(); err != nil {
			c.err = err
			c.valid = false
			return false
		}
		// If the new batch is empty, we're done.
		if len(c.batchBuffer) == 0 {
			c.valid = false
			return false
		}
	}

	// Read the next key-value pair from the batch buffer.
	// Each entry is length-prefixed.
	if err := c.readNextKV(); err != nil {
		c.err = err
		c.valid = false
		return false
	}

	c.valid = true
	return true
}

// fetchNextBatch retrieves the next chunk of key-value pairs from C.
func (c *stringRangeCursor) fetchNextBatch() error {
	const batchSize = 1000 // Number of records to fetch per batch.
	var cBuf *C.char
	var cBufLen C.int
	var numFetched C.int

	errCode := C.wt_range_scan_next_batch(c.ctx, batchSize, &cBuf, &cBufLen, &numFetched)
	if errCode != 0 {
		return errors.New("range scan fetchNextBatch failed")
	}
	defer C.wt_free_batch_buf(cBuf) // Free the C buffer after copying.

	if numFetched == 0 {
		c.batchBuffer = nil
		c.readOffset = 0
		return nil
	}

	// Copy the data from C memory to a Go-managed byte slice.
	c.batchBuffer = C.GoBytes(unsafe.Pointer(cBuf), cBufLen)
	c.readOffset = 0
	return nil
}

// readNextKV parses the next key and value from the batchBuffer.
func (c *stringRangeCursor) readNextKV() error {
	buf := c.batchBuffer
	offset := c.readOffset

	// Ensure there's enough data for key length.
	if len(buf)-offset < 4 {
		return errors.New("incomplete batch: could not read key length")
	}
	keyLen := int(binary.LittleEndian.Uint32(buf[offset:]))
	offset += 4

	// Ensure there's enough data for the key.
	if len(buf)-offset < keyLen {
		return errors.New("incomplete batch: could not read key")
	}
	c.currKey = string(buf[offset : offset+keyLen])
	offset += keyLen

	// Ensure there's enough data for value length.
	if len(buf)-offset < 4 {
		return errors.New("incomplete batch: could not read value length")
	}
	valLen := int(binary.LittleEndian.Uint32(buf[offset:]))
	offset += 4

	// Ensure there's enough data for the value.
	if len(buf)-offset < valLen {
		return errors.New("incomplete batch: could not read value")
	}
	c.currVal = string(buf[offset : offset+valLen])
	offset += valLen

	c.readOffset = offset
	return nil
}

func (c *stringRangeCursor) CurrentString() (string, string, error) {
	if !c.valid {
		return "", "", errors.New("cursor not positioned on a valid record")
	}
	if c.err != nil {
		return "", "", c.err
	}
	return c.currKey, c.currVal, nil
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
	out := &stringRangeCursor{
		ctx:         ctx,
		valid:       true,
		firstCall:   true,
		batchBuffer: nil,
		readOffset:  0,
		currKey:     "",
		currVal:     "",
	}
	return out, nil
}
