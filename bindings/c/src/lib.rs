//! C language bindings for MooFile.
//!
//! Compiles to `libmoofile.so` / `libmoofile.dylib` / `moofile.dll`.
//!
//! All public functions follow the same pattern:
//!   - Return 0 on success, non-zero on error.
//!   - When an error occurs, `*err_out` is set to an allocated C string
//!     that the caller must free with `moofile_free_string`.
//!   - Handles are opaque pointers.
//!
//! Documents and filters are passed as JSON strings.  The Rust side
//! parses them with serde_json and converts to BSON internally.

use std::ffi::{CStr, CString};
use std::panic::{self, AssertUnwindSafe};
use std::ptr;

use bson::{Bson, Document};
use moofile::Collection as RustCollection;

// ---------------------------------------------------------------------------
// Error handling helpers
// ---------------------------------------------------------------------------

/// Set `*err_out` to a C-string copy of `msg`.  The caller must free with
/// `moofile_free_string`.
unsafe fn set_error(err_out: *mut *mut i8, msg: &str) {
    if !err_out.is_null() {
        let c_msg = CString::new(msg).unwrap_or(CString::new("unknown error").unwrap());
        unsafe { *err_out = c_msg.into_raw().cast(); }
    }
}

/// Clear the error output (set to NULL).
unsafe fn clear_error(err_out: *mut *mut i8) {
    if !err_out.is_null() {
        unsafe { *err_out = ptr::null_mut(); }
    }
}

/// Catch a Rust panic, convert to error string. Returns -1 on panic/error.
fn catch_panic<F>(err_out: *mut *mut i8, f: F) -> i32
where
    F: FnOnce() -> Result<i32, String> + std::panic::UnwindSafe,
{
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(code)) => code,
        Ok(Err(msg)) => {
            unsafe { set_error(err_out, &msg); }
            -1
        }
        Err(panic_info) => {
            let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "Rust panic (unknown)".to_string()
            };
            unsafe { set_error(err_out, &msg); }
            -1
        }
    }
}

/// Macro to wrap a C-exported function body with panic catching.
macro_rules! c_try {
    ($err_out:expr, $body:expr) => {{
        let err = $err_out;
        match panic::catch_unwind(AssertUnwindSafe(|| -> Result<i32, String> { $body })) {
            Ok(Ok(code)) => code,
            Ok(Err(msg)) => {
                unsafe { set_error(err, &msg); }
                -1
            }
            Err(panic_info) => {
                let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Rust panic (unknown)".to_string()
                };
                unsafe { set_error(err, &msg); }
                -1
            }
        }
    }};
}

// ---------------------------------------------------------------------------
// String conversion helpers
// ---------------------------------------------------------------------------

unsafe fn c_str_to_str<'a>(s: *const i8) -> Result<&'a str, String> {
    if s.is_null() {
        return Err("null pointer".into());
    }
    unsafe { CStr::from_ptr(s.cast()) }
        .to_str()
        .map_err(|e| format!("invalid UTF-8: {e}"))
}

/// Return a C-string allocated via CString (caller frees via moofile_free_string).
fn to_c_string(s: String) -> *mut i8 {
    CString::new(s).unwrap_or_default().into_raw().cast()
}

// ---------------------------------------------------------------------------
// JSON <-> BSON conversion
// ---------------------------------------------------------------------------

fn json_to_doc(json_str: &str) -> Result<Document, String> {
    let val: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| format!("JSON parse error: {e}"))?;
    json_value_to_bson_document(val)
}

fn json_value_to_bson_document(val: serde_json::Value) -> Result<Document, String> {
    match val {
        serde_json::Value::Object(map) => {
            let mut doc = Document::new();
            for (k, v) in map {
                doc.insert(k, json_value_to_bson(v)?);
            }
            Ok(doc)
        }
        _ => Err("expected JSON object at top level".into()),
    }
}

fn json_value_to_bson(val: serde_json::Value) -> Result<Bson, String> {
    Ok(match val {
        serde_json::Value::Null => Bson::Null,
        serde_json::Value::Bool(b) => Bson::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Int64(i)
                }
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                return Err(format!("invalid number: {n}"));
            }
        }
        serde_json::Value::String(s) => Bson::String(s),
        serde_json::Value::Array(arr) => {
            let mut bson_arr = Vec::with_capacity(arr.len());
            for v in arr {
                bson_arr.push(json_value_to_bson(v)?);
            }
            Bson::Array(bson_arr)
        }
        serde_json::Value::Object(map) => {
            let mut doc = Document::new();
            for (k, v) in map {
                doc.insert(k, json_value_to_bson(v)?);
            }
            Bson::Document(doc)
        }
    })
}

fn doc_to_json(doc: &Document) -> String {
    let val = bson_document_to_json_value(doc);
    serde_json::to_string(&val).unwrap_or_else(|_| "{}".to_string())
}

fn bson_document_to_json_value(doc: &Document) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (k, v) in doc.iter() {
        map.insert(k.to_string(), bson_to_json_value(v));
    }
    serde_json::Value::Object(map)
}

fn bson_to_json_value(bson: &Bson) -> serde_json::Value {
    match bson {
        Bson::Null => serde_json::Value::Null,
        Bson::Boolean(b) => serde_json::Value::Bool(*b),
        Bson::Int32(i) => serde_json::Value::Number((*i).into()),
        Bson::Int64(i) => serde_json::Value::Number((*i).into()),
        Bson::Double(f) => {
            serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from_f64(0.0).unwrap()))
        }
        Bson::String(s) => serde_json::Value::String(s.clone()),
        Bson::Array(arr) => {
            let mut json_arr = Vec::with_capacity(arr.len());
            for v in arr {
                json_arr.push(bson_to_json_value(v));
            }
            serde_json::Value::Array(json_arr)
        }
        Bson::Document(d) => bson_document_to_json_value(d),
        Bson::DateTime(dt) => serde_json::json!(dt.timestamp_millis()),
        Bson::ObjectId(oid) => serde_json::Value::String(oid.to_hex()),
        Bson::Binary(bin) => serde_json::Value::String(base64_encode(&bin.bytes)),
        Bson::RegularExpression(re) => serde_json::Value::String(format!("/{}/{}", re.pattern, re.options)),
        Bson::JavaScriptCode(code) => serde_json::Value::String(code.clone()),
        Bson::JavaScriptCodeWithScope(js) => serde_json::Value::String(js.code.clone()),
        Bson::Timestamp(ts) => serde_json::Value::String(format!("Timestamp({}, {})", ts.time, ts.increment)),
        Bson::Decimal128(d) => serde_json::Value::String(d.to_string()),
        Bson::Symbol(s) => serde_json::Value::String(s.clone()),
        Bson::Undefined => serde_json::Value::Null,
        Bson::DbPointer(_) => serde_json::Value::String("[DBPointer]".into()),
        Bson::MaxKey => serde_json::Value::String("MaxKey".into()),
        Bson::MinKey => serde_json::Value::String("MinKey".into()),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------

pub struct MooFileCollection {
    inner: RustCollection,
}

pub struct MooFileCursor {
    docs: Vec<Document>,
    index: usize,
}

pub struct MooFileSearchCursor {
    results: Vec<(Document, f32)>,
    index: usize,
}

// ---------------------------------------------------------------------------
// Collection lifecycle
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_open(
    path: *const i8,
    config_json: *const i8,
    err_out: *mut *mut i8,
) -> *mut MooFileCollection {
    unsafe { clear_error(err_out); }

    let path_str = match unsafe { c_str_to_str(path) } {
        Ok(s) => s,
        Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); }
    };

    let config: serde_json::Value = if config_json.is_null() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        match unsafe { c_str_to_str(config_json) } {
            Ok(s) => match serde_json::from_str(s) {
                Ok(v) => v,
                Err(e) => { unsafe { set_error(err_out, &format!("config JSON parse error: {e}")); } return ptr::null_mut(); }
            },
            Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); }
        }
    };

    let mut builder = RustCollection::builder(path_str);

    if let Some(indexes) = config.get("indexes").and_then(|v| v.as_array()) {
        let fields: Vec<&str> = indexes.iter().filter_map(|v| v.as_str()).collect();
        builder = builder.indexes(&fields);
    }

    if let Some(vi) = config.get("vector_indexes").and_then(|v| v.as_object()) {
        for (field, dim) in vi {
            if let Some(d) = dim.as_u64() {
                builder = builder.vector_index(field.as_str(), d as usize);
            }
        }
    }

    if let Some(ti) = config.get("text_indexes").and_then(|v| v.as_array()) {
        for field in ti {
            if let Some(f) = field.as_str() {
                builder = builder.text_index(f);
            }
        }
    }

    if config.get("readonly").and_then(|v| v.as_bool()).unwrap_or(false) {
        builder = builder.readonly();
    }

    if let Some(dur) = config.get("durability").and_then(|v| v.as_str()) {
        let d = match dur {
            "none" => moofile::Durability::None,
            "os" => moofile::Durability::Os,
            "fsync" => moofile::Durability::Fsync,
            other => { unsafe { set_error(err_out, &format!("invalid durability '{other}': must be 'none', 'os', or 'fsync'")); } return ptr::null_mut(); }
        };
        builder = builder.durability(d);
    }

    // Parse auto_embed config
    if let Some(ae) = config.get("auto_embed").and_then(|v| v.as_object()) {
        for (source_field, cfg_val) in ae {
            use moofile::AutoEmbedConfig;
            let mut ae_config = AutoEmbedConfig::default();
            if let Some(obj) = cfg_val.as_object() {
                if let Some(model) = obj.get("model").and_then(|v| v.as_str()) {
                    ae_config.model = model.to_string();
                } else {
                    unsafe { set_error(err_out, &format!("auto_embed[{}]: 'model' is required", source_field)); }
                    return ptr::null_mut();
                }
                if let Some(target) = obj.get("target").and_then(|v| v.as_str()) {
                    ae_config.target_field = target.to_string();
                }
                if let Some(dims) = obj.get("dims").and_then(|v| v.as_u64()) {
                    ae_config.dims = dims as usize;
                }
                if let Some(prec) = obj.get("precision").and_then(|v| v.as_str()) {
                    ae_config.precision = match prec {
                        "f32" => moofile::EmbeddingPrecision::F32,
                        "int8" => moofile::EmbeddingPrecision::Int8,
                        "uint8" => moofile::EmbeddingPrecision::Uint8,
                        "binary" => moofile::EmbeddingPrecision::Binary,
                        other => { unsafe { set_error(err_out, &format!("auto_embed[{}]: unknown precision '{other}'", source_field)); }
                            return ptr::null_mut(); }
                    };
                }
                if let Some(norm) = obj.get("normalize").and_then(|v| v.as_bool()) {
                    ae_config.normalize = norm;
                }
                if let Some(qp) = obj.get("query_prefix").and_then(|v| v.as_str()) {
                    ae_config.query_prefix = qp.to_string();
                }
                if let Some(dp) = obj.get("doc_prefix").and_then(|v| v.as_str()) {
                    ae_config.doc_prefix = dp.to_string();
                }
            }
            builder = builder.auto_embed(source_field.as_str(), ae_config);
        }
    }

    // Parse model_cache_dir
    if let Some(mcd) = config.get("model_cache_dir").and_then(|v| v.as_str()) {
        builder = builder.model_cache_dir(mcd);
    }

    match builder.open() {
        Ok(inner) => Box::into_raw(Box::new(MooFileCollection { inner })),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

#[no_mangle]
pub extern "C" fn moofile_close(handle: *mut MooFileCollection, err_out: *mut *mut i8) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { Box::from_raw(handle) };
        coll.inner.close().map_err(|e| e.to_string())?;
        Ok(0)
    })
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_insert(
    handle: *mut MooFileCollection,
    doc_json: *const i8,
    err_out: *mut *mut i8,
) -> *mut i8 {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let doc_str = match unsafe { c_str_to_str(doc_json) } {
        Ok(s) => s,
        Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); }
    };
    let doc = match json_to_doc(doc_str) {
        Ok(d) => d,
        Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); }
    };

    match coll.inner.insert(doc) {
        Ok(inserted) => to_c_string(doc_to_json(&inserted)),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

#[no_mangle]
pub extern "C" fn moofile_insert_many(
    handle: *mut MooFileCollection,
    docs_json: *const i8,
    err_out: *mut *mut i8,
) -> *mut i8 {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let json_str = match unsafe { c_str_to_str(docs_json) } {
        Ok(s) => s,
        Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); }
    };

    let arr: Vec<serde_json::Value> = match serde_json::from_str(json_str) {
        Ok(a) => a,
        Err(e) => { unsafe { set_error(err_out, &format!("JSON parse error: {e}")); } return ptr::null_mut(); }
    };

    let mut docs = Vec::with_capacity(arr.len());
    for val in arr {
        match json_value_to_bson_document(val) {
            Ok(d) => docs.push(d),
            Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); }
        }
    }

    match coll.inner.insert_many(docs) {
        Ok(inserted) => {
            let mut out_arr = Vec::with_capacity(inserted.len());
            for doc in &inserted {
                out_arr.push(bson_document_to_json_value(doc));
            }
            let json = serde_json::to_string(&serde_json::Value::Array(out_arr))
                .unwrap_or_else(|_| "[]".to_string());
            to_c_string(json)
        }
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_find(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    err_out: *mut *mut i8,
) -> *mut MooFileCursor {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let filter_str = if filter_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(filter_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };
    let filter = match json_to_doc(filter_str) { Ok(d) => d, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    match coll.inner.find(filter).and_then(|q| q.to_list()) {
        Ok(docs) => Box::into_raw(Box::new(MooFileCursor { docs, index: 0 })),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

/// Parse the `options_json` blob accepted by `moofile_find_ex` and apply it to
/// a `Query`.  Unknown keys are rejected rather than ignored: a typo in
/// `"limit"` would otherwise silently return the whole collection.
fn apply_find_options(
    mut q: moofile::Query,
    options_json: &str,
) -> Result<moofile::Query, String> {
    let val: serde_json::Value = serde_json::from_str(options_json)
        .map_err(|e| format!("options JSON parse error: {e}"))?;
    let obj = match val {
        serde_json::Value::Object(o) => o,
        serde_json::Value::Null => return Ok(q),
        _ => return Err("options must be a JSON object".into()),
    };

    for key in obj.keys() {
        match key.as_str() {
            "sort" | "skip" | "limit" | "group" | "agg" => {}
            other => return Err(format!("unknown find option '{other}'")),
        }
    }

    // "sort": "field" | {"field": "name", "desc": bool}
    if let Some(sort) = obj.get("sort") {
        match sort {
            serde_json::Value::String(f) => q = q.sort(f.clone(), false),
            serde_json::Value::Object(o) => {
                let field = o
                    .get("field")
                    .and_then(|v| v.as_str())
                    .ok_or("sort.field must be a string")?;
                let desc = o.get("desc").and_then(|v| v.as_bool()).unwrap_or(false);
                q = q.sort(field.to_string(), desc);
            }
            serde_json::Value::Null => {}
            _ => return Err("sort must be a string or an object".into()),
        }
    }

    if let Some(skip) = obj.get("skip") {
        if !skip.is_null() {
            let n = skip.as_u64().ok_or("skip must be a non-negative integer")?;
            q = q.skip(n as usize);
        }
    }

    if let Some(limit) = obj.get("limit") {
        if !limit.is_null() {
            let n = limit.as_u64().ok_or("limit must be a non-negative integer")?;
            q = q.limit(n as usize);
        }
    }

    if let Some(group) = obj.get("group") {
        match group {
            serde_json::Value::String(f) => q = q.group(f.clone()),
            serde_json::Value::Null => {}
            _ => return Err("group must be a string".into()),
        }
    }

    // "agg": [{"func": "sum", "field": "amount"}, {"func": "count"}]
    if let Some(agg) = obj.get("agg") {
        if !agg.is_null() {
            let arr = agg.as_array().ok_or("agg must be an array")?;
            let mut funcs = Vec::with_capacity(arr.len());
            for item in arr {
                let o = item.as_object().ok_or("each agg entry must be an object")?;
                let func = o
                    .get("func")
                    .and_then(|v| v.as_str())
                    .ok_or("agg.func must be a string")?;
                // Every function except `count` operates on a named field.
                let field = || -> Result<String, String> {
                    o.get("field")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .ok_or_else(|| format!("agg func '{func}' requires a 'field'"))
                };
                funcs.push(match func {
                    "count" => moofile::AggFunc::Count,
                    "sum" => moofile::AggFunc::Sum(field()?),
                    "mean" | "avg" => moofile::AggFunc::Mean(field()?),
                    "min" => moofile::AggFunc::Min(field()?),
                    "max" => moofile::AggFunc::Max(field()?),
                    "collect" => moofile::AggFunc::Collect(field()?),
                    "first" => moofile::AggFunc::First(field()?),
                    "last" => moofile::AggFunc::Last(field()?),
                    other => return Err(format!("unknown agg func '{other}'")),
                });
            }
            q = q.agg(funcs);
        }
    }

    Ok(q)
}

/// Find with the full query builder: sort, skip, limit, group, agg.
///
/// `options_json` is a JSON object; see `apply_find_options` for the schema.
#[no_mangle]
pub extern "C" fn moofile_find_ex(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    options_json: *const i8,
    err_out: *mut *mut i8,
) -> *mut MooFileCursor {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let filter_str = if filter_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(filter_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };
    let filter = match json_to_doc(filter_str) { Ok(d) => d, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let opts_str = if options_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(options_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };

    let query = match coll.inner.find(filter) {
        Ok(q) => q,
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } return ptr::null_mut(); }
    };
    let query = match apply_find_options(query, opts_str) {
        Ok(q) => q,
        Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); }
    };

    match query.to_list() {
        Ok(docs) => Box::into_raw(Box::new(MooFileCursor { docs, index: 0 })),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

#[no_mangle]
pub extern "C" fn moofile_find_one(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    err_out: *mut *mut i8,
) -> *mut i8 {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let filter_str = if filter_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(filter_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };
    let filter = match json_to_doc(filter_str) { Ok(d) => d, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    match coll.inner.find_one(filter) {
        Ok(Some(doc)) => to_c_string(doc_to_json(&doc)),
        Ok(None) => ptr::null_mut(),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

#[no_mangle]
pub extern "C" fn moofile_count(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    err_out: *mut *mut i8,
) -> i64 {
    let result = catch_panic(err_out, || -> Result<i32, String> {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        let filter_str = if filter_json.is_null() { "{}" } else { unsafe { c_str_to_str(filter_json)? } };
        let filter = json_to_doc(filter_str)?;
        let count = coll.inner.count(filter).map_err(|e| e.to_string())?;
        Ok(count as i32)
    });
    if result < 0 { -1 } else { result as i64 }
}

#[no_mangle]
pub extern "C" fn moofile_exists(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    err_out: *mut *mut i8,
) -> i32 {
    catch_panic(err_out, || -> Result<i32, String> {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        let filter_str = if filter_json.is_null() { "{}" } else { unsafe { c_str_to_str(filter_json)? } };
        let filter = json_to_doc(filter_str)?;
        let exists = coll.inner.exists(filter).map_err(|e| e.to_string())?;
        Ok(if exists { 1 } else { 0 })
    })
}

// ---------------------------------------------------------------------------
// Cursor iteration
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_cursor_next(
    cursor: *mut MooFileCursor,
    err_out: *mut *mut i8,
) -> *mut i8 {
    unsafe { clear_error(err_out); }
    if cursor.is_null() { unsafe { set_error(err_out, "cursor is null"); } return ptr::null_mut(); }
    let c = unsafe { &mut *cursor };
    if c.index >= c.docs.len() { return ptr::null_mut(); }
    let doc = &c.docs[c.index];
    c.index += 1;
    to_c_string(doc_to_json(doc))
}

#[no_mangle]
pub extern "C" fn moofile_cursor_free(cursor: *mut MooFileCursor) {
    if !cursor.is_null() { unsafe { drop(Box::from_raw(cursor)); } }
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_update_one(
    handle: *mut MooFileCollection,
    where_json: *const i8,
    update_json: *const i8,
    err_out: *mut *mut i8,
) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        let where_str = unsafe { c_str_to_str(where_json)? };
        let where_doc = json_to_doc(where_str)?;

        let update_str = if update_json.is_null() { "{}" } else { unsafe { c_str_to_str(update_json)? } };
        let update_val: serde_json::Value = serde_json::from_str(update_str).map_err(|e| format!("update JSON parse error: {e}"))?;

        let set = update_val.get("set").map(|v| json_value_to_bson_document(v.clone())).transpose()?;
        let unset = update_val.get("unset")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect::<Vec<_>>());
        let inc = update_val.get("inc").map(|v| json_value_to_bson_document(v.clone())).transpose()?;

        let ok = coll.inner.update_one(where_doc, set, unset, inc).map_err(|e| e.to_string())?;
        Ok(if ok { 1 } else { 0 })
    })
}

#[no_mangle]
pub extern "C" fn moofile_update_many(
    handle: *mut MooFileCollection,
    where_json: *const i8,
    update_json: *const i8,
    err_out: *mut *mut i8,
) -> i64 {
    let result = c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        let where_str = unsafe { c_str_to_str(where_json)? };
        let where_doc = json_to_doc(where_str)?;

        let update_str = if update_json.is_null() { "{}" } else { unsafe { c_str_to_str(update_json)? } };
        let update_val: serde_json::Value = serde_json::from_str(update_str).map_err(|e| format!("update JSON parse error: {e}"))?;

        let set = update_val.get("set").map(|v| json_value_to_bson_document(v.clone())).transpose()?;
        let unset = update_val.get("unset")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect::<Vec<_>>());
        let inc = update_val.get("inc").map(|v| json_value_to_bson_document(v.clone())).transpose()?;

        let count = coll.inner.update_many(where_doc, set, unset, inc).map_err(|e| e.to_string())?;
        Ok(count as i32)
    });
    if result < 0 { -1 } else { result as i64 }
}

#[no_mangle]
pub extern "C" fn moofile_replace_one(
    handle: *mut MooFileCollection,
    where_json: *const i8,
    replacement_json: *const i8,
    err_out: *mut *mut i8,
) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        let where_str = unsafe { c_str_to_str(where_json)? };
        let where_doc = json_to_doc(where_str)?;
        let repl_str = unsafe { c_str_to_str(replacement_json)? };
        let repl_doc = json_to_doc(repl_str)?;
        let ok = coll.inner.replace_one(where_doc, repl_doc).map_err(|e| e.to_string())?;
        Ok(if ok { 1 } else { 0 })
    })
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_delete_one(
    handle: *mut MooFileCollection,
    where_json: *const i8,
    err_out: *mut *mut i8,
) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        let where_str = unsafe { c_str_to_str(where_json)? };
        let where_doc = json_to_doc(where_str)?;
        let ok = coll.inner.delete_one(where_doc).map_err(|e| e.to_string())?;
        Ok(if ok { 1 } else { 0 })
    })
}

#[no_mangle]
pub extern "C" fn moofile_delete_many(
    handle: *mut MooFileCollection,
    where_json: *const i8,
    err_out: *mut *mut i8,
) -> i64 {
    let result = c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        let where_str = unsafe { c_str_to_str(where_json)? };
        let where_doc = json_to_doc(where_str)?;
        let count = coll.inner.delete_many(where_doc).map_err(|e| e.to_string())?;
        Ok(count as i32)
    });
    if result < 0 { -1 } else { result as i64 }
}

// ---------------------------------------------------------------------------
// Vector search
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_vector_search(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    field: *const i8,
    query_vector_json: *const i8,
    limit: i32,
    err_out: *mut *mut i8,
) -> *mut MooFileSearchCursor {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let filter_str = if filter_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(filter_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };
    let filter = match json_to_doc(filter_str) { Ok(d) => d, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let field_str = match unsafe { c_str_to_str(field) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let vec_str = match unsafe { c_str_to_str(query_vector_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };
    let query_vec: Vec<f32> = match serde_json::from_str(vec_str) {
        Ok(v) => v, Err(e) => { unsafe { set_error(err_out, &format!("query vector parse error: {e}")); } return ptr::null_mut(); }
    };

    let lim = if limit <= 0 { 10 } else { limit as usize };

    match coll.inner.find(filter).and_then(|q| q.vector_search(field_str, query_vec, lim).to_list()) {
        Ok(results) => Box::into_raw(Box::new(MooFileSearchCursor { results, index: 0 })),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

// ---------------------------------------------------------------------------
// Text search
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_text_search(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    field: *const i8,
    query: *const i8,
    limit: i32,
    err_out: *mut *mut i8,
) -> *mut MooFileSearchCursor {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let filter_str = if filter_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(filter_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };
    let filter = match json_to_doc(filter_str) { Ok(d) => d, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let field_str = match unsafe { c_str_to_str(field) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };
    let query_str = match unsafe { c_str_to_str(query) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let lim = if limit <= 0 { 10 } else { limit as usize };

    match coll.inner.find(filter).and_then(|q| q.text_search(field_str, query_str, lim).to_list()) {
        Ok(results) => Box::into_raw(Box::new(MooFileSearchCursor { results, index: 0 })),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

// ---------------------------------------------------------------------------
// Hybrid search
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_hybrid_search(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    text_field: *const i8,
    vector_field: *const i8,
    query_text: *const i8,
    query_vector_json: *const i8,
    limit: i32,
    err_out: *mut *mut i8,
) -> *mut MooFileSearchCursor {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let filter_str = if filter_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(filter_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };
    let filter = match json_to_doc(filter_str) { Ok(d) => d, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let tf = match unsafe { c_str_to_str(text_field) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };
    let vf = match unsafe { c_str_to_str(vector_field) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };
    let qt = match unsafe { c_str_to_str(query_text) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let qv: Option<Vec<f32>> = if query_vector_json.is_null() {
        None
    } else {
        let vec_str = match unsafe { c_str_to_str(query_vector_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };
        match serde_json::from_str(vec_str) {
            Ok(v) => Some(v),
            Err(e) => { unsafe { set_error(err_out, &format!("query vector parse error: {e}")); } return ptr::null_mut(); }
        }
    };

    let lim = if limit <= 0 { 10 } else { limit as usize };

    match coll.inner.find(filter).and_then(|q| q.hybrid_search(tf, vf, qt, qv, lim).to_list()) {
        Ok(results) => Box::into_raw(Box::new(MooFileSearchCursor { results, index: 0 })),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

// ---------------------------------------------------------------------------
// Semantic search (autoembedding)
// ---------------------------------------------------------------------------

/// Perform semantic search — auto-embeds the query text using the configured
/// embedding model and returns vector search results.
///
/// `source_field` must have been configured with `auto_embed` at collection
/// open time.  The query text is automatically prefixed with the configured
/// `query_prefix`.
///
/// Returns a search cursor with `(doc_json, score)` pairs.
#[no_mangle]
pub extern "C" fn moofile_semantic_search(
    handle: *mut MooFileCollection,
    filter_json: *const i8,
    source_field: *const i8,
    query_text: *const i8,
    limit: i32,
    err_out: *mut *mut i8,
) -> *mut MooFileSearchCursor {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    let filter_str = if filter_json.is_null() { "{}" } else {
        match unsafe { c_str_to_str(filter_json) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } }
    };
    let filter = match json_to_doc(filter_str) { Ok(d) => d, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let source = match unsafe { c_str_to_str(source_field) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };
    let qt = match unsafe { c_str_to_str(query_text) } { Ok(s) => s, Err(e) => { unsafe { set_error(err_out, &e); } return ptr::null_mut(); } };

    let lim = if limit <= 0 { 10 } else { limit as usize };

    match coll.inner.find(filter)
        .and_then(|q| q.semantic(source, qt, lim))
        .and_then(|vq| vq.to_list())
    {
        Ok(results) => Box::into_raw(Box::new(MooFileSearchCursor { results, index: 0 })),
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

// ---------------------------------------------------------------------------
// Search cursor
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_search_cursor_next(
    cursor: *mut MooFileSearchCursor,
    err_out: *mut *mut i8,
) -> *mut i8 {
    unsafe { clear_error(err_out); }
    if cursor.is_null() { unsafe { set_error(err_out, "search cursor is null"); } return ptr::null_mut(); }
    let c = unsafe { &mut *cursor };
    if c.index >= c.results.len() { return ptr::null_mut(); }
    let (doc, score) = &c.results[c.index];
    c.index += 1;

    let doc_val = bson_document_to_json_value(doc);
    let pair = serde_json::json!([doc_val, score]);
    to_c_string(serde_json::to_string(&pair).unwrap_or_else(|_| "[]".to_string()))
}

#[no_mangle]
pub extern "C" fn moofile_search_cursor_free(cursor: *mut MooFileSearchCursor) {
    if !cursor.is_null() { unsafe { drop(Box::from_raw(cursor)); } }
}

// ---------------------------------------------------------------------------
// Batch writes
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_batch_begin(handle: *mut MooFileCollection, err_out: *mut *mut i8) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        coll.inner.batch_begin().map_err(|e| e.to_string())?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn moofile_batch_commit(handle: *mut MooFileCollection, err_out: *mut *mut i8) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        coll.inner.batch_commit().map_err(|e| e.to_string())?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn moofile_batch_rollback(handle: *mut MooFileCollection, err_out: *mut *mut i8) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        coll.inner.batch_rollback().map_err(|e| e.to_string())?;
        Ok(0)
    })
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_stats(
    handle: *mut MooFileCollection,
    err_out: *mut *mut i8,
) -> *mut i8 {
    unsafe { clear_error(err_out); }
    if handle.is_null() { unsafe { set_error(err_out, "handle is null"); } return ptr::null_mut(); }
    let coll = unsafe { &*handle };

    match coll.inner.stats() {
        Ok(stats) => {
            let out = serde_json::json!({
                "documents": stats.documents,
                "dead_records": stats.dead_records,
                "file_size_bytes": stats.file_size_bytes,
                "dead_ratio": stats.dead_ratio,
            });
            to_c_string(serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()))
        }
        Err(e) => { unsafe { set_error(err_out, &e.to_string()); } ptr::null_mut() }
    }
}

#[no_mangle]
pub extern "C" fn moofile_compact(handle: *mut MooFileCollection, err_out: *mut *mut i8) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        coll.inner.compact().map_err(|e| e.to_string())?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn moofile_sync(handle: *mut MooFileCollection, err_out: *mut *mut i8) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        coll.inner.sync().map_err(|e| e.to_string())?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn moofile_reindex(handle: *mut MooFileCollection, err_out: *mut *mut i8) -> i32 {
    c_try!(err_out, {
        if handle.is_null() { return Err("handle is null".into()); }
        let coll = unsafe { &*handle };
        coll.inner.reindex().map_err(|e| e.to_string())?;
        Ok(0)
    })
}

/// Re-embed every document carrying `source_field`, rewriting its configured
/// vector field at the current model's width.
///
/// Returns the number of documents rewritten, or -1 on error.
#[no_mangle]
pub extern "C" fn moofile_reembed(
    handle: *mut MooFileCollection,
    source_field: *const i8,
    err_out: *mut *mut i8,
) -> i64 {
    let result = catch_panic(err_out, || -> Result<i32, String> {
        if handle.is_null() { return Err("handle is null".into()); }
        if source_field.is_null() { return Err("source_field is null".into()); }
        let coll = unsafe { &*handle };
        let field = unsafe { c_str_to_str(source_field)? };
        let n = coll.inner.reembed(field).map_err(|e| e.to_string())?;
        Ok(n as i32)
    });
    if result < 0 { -1 } else { result as i64 }
}

// ---------------------------------------------------------------------------
// Memory management
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn moofile_free_string(s: *mut i8) {
    if !s.is_null() { unsafe { drop(CString::from_raw(s.cast())); } }
}
