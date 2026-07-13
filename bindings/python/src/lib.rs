//! PyO3 native binding for MooFile.
//!
//! Compiles to `_native.so` and is loaded by `moofile/__init__.py`
//! as a transparent drop-in for the pure-Python `Collection`.

use std::collections::HashMap;

use bson::{Bson, Document};
use moofile_core::Collection as RustCollection;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

// ---------------------------------------------------------------------------
// Helpers: PyObject ↔ Bson
// ---------------------------------------------------------------------------

fn py_to_bson(obj: &Bound<PyAny>) -> PyResult<Bson> {
    if obj.is_none() {
        return Ok(Bson::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Bson::Boolean(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
            return Ok(Bson::Int32(i as i32));
        }
        return Ok(Bson::Int64(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Bson::Double(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Bson::String(s));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_bson(&item)?);
        }
        return Ok(Bson::Array(arr));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut doc = Document::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            doc.insert(key, py_to_bson(&v)?);
        }
        return Ok(Bson::Document(doc));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        format!("unsupported type: {:?}", obj.get_type().name()),
    ))
}

fn py_to_document(dict: &Bound<PyDict>) -> PyResult<Document> {
    match py_to_bson(dict.as_any())? {
        Bson::Document(doc) => Ok(doc),
        _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected dict")),
    }
}

fn bson_to_py(val: &Bson, py: Python<'_>) -> PyObject {
    match val {
        Bson::Null => py.None(),
        Bson::Boolean(b) => b.to_object(py),
        Bson::Int32(i) => i.to_object(py),
        Bson::Int64(i) => i.to_object(py),
        Bson::Double(f) => f.to_object(py),
        Bson::String(s) => s.to_object(py),
        Bson::Array(arr) => {
            let list = PyList::new(py, arr.iter().map(|v| bson_to_py(v, py)));
            list.unwrap().into()
        }
        Bson::Document(doc) => {
            let dict = PyDict::new(py);
            for (k, v) in doc.iter() {
                dict.set_item(k, bson_to_py(v, py)).unwrap();
            }
            dict.into()
        }
        _ => val.to_string().to_object(py),
    }
}

fn doc_to_py(doc: &Document, py: Python<'_>) -> PyObject {
    let dict = PyDict::new(py);
    for (k, v) in doc.iter() {
        dict.set_item(k, bson_to_py(v, py)).unwrap();
    }
    dict.into()
}

/// Encode a BSON document to raw bytes for Python-side decoding (item #6).
/// Returning raw bytes avoids the slow recursive PyDict building in bson_to_py
/// and the lossy _ => val.to_string() fallback.
fn doc_to_bson_bytes(doc: &Document, py: Python<'_>) -> PyObject {
    let bytes = bson::to_vec(doc).unwrap_or_default();
    PyBytes::new(py, &bytes).into()
}

// ---------------------------------------------------------------------------
// NativeCollection
// ---------------------------------------------------------------------------

#[pyclass(name = "NativeCollection")]
struct NativeCollection {
    inner: RustCollection,
}

#[pymethods]
impl NativeCollection {
    #[new]
    #[pyo3(signature = (path, indexes=None, vector_indexes=None, text_indexes=None, readonly=false, durability="os"))]
    fn new(
        path: String,
        indexes: Option<Vec<String>>,
        vector_indexes: Option<HashMap<String, usize>>,
        text_indexes: Option<Vec<String>>,
        readonly: bool,
        durability: &str,
    ) -> PyResult<Self> {
        let dur = match durability {
            "none" => moofile_core::Durability::None,
            "os" => moofile_core::Durability::Os,
            "fsync" => moofile_core::Durability::Fsync,
            other => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("invalid durability '{}': must be 'none', 'os', or 'fsync'", other),
            )),
        };
        let mut builder = RustCollection::builder(&path).durability(dur);
        if let Some(idxs) = &indexes {
            let refs: Vec<&str> = idxs.iter().map(|s| s.as_str()).collect();
            builder = builder.indexes(&refs);
        }
        if let Some(vi) = &vector_indexes {
            for (field, dim) in vi {
                builder = builder.vector_index(field, *dim);
            }
        }
        if let Some(ti) = &text_indexes {
            for field in ti {
                builder = builder.text_index(field.as_str());
            }
        }
        if readonly {
            builder = builder.readonly();
        }
        let inner = builder
            .open()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(Self { inner })
    }

    // --- Bulk operations (fast paths that avoid per-doc round-trips) ---

    /// Insert many documents in a single Rust loop — much faster than
    /// calling `insert()` N times from Python.
    /// Returns a list of raw BSON bytes for Python-side decoding.
    fn insert_many(&self, py: Python<'_>, docs: &Bound<PyList>) -> PyResult<PyObject> {
        let mut rust_docs = Vec::with_capacity(docs.len());
        for item in docs.iter() {
            let dict = item.downcast::<PyDict>()?;
            rust_docs.push(py_to_document(dict)?);
        }
        let results = self.inner.insert_many(rust_docs).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;
        let list = PyList::new(py, results.iter().map(|d| doc_to_bson_bytes(d, py)));
        Ok(list.unwrap().into())
    }

    /// Find all matching documents and return as a Python list of dicts.
    /// Single PyO3 round-trip, no per-doc conversion overhead in Python.
    fn find(&self, py: Python<'_>, filter: Option<&Bound<PyDict>>) -> PyResult<PyObject> {
        let f = match filter {
            Some(d) => py_to_document(d)?,
            None => Document::new(),
        };
        let results = self
            .inner
            .find(f)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .to_list()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let list = PyList::new(py, results.iter().map(|d| doc_to_py(d, py)));
        Ok(list.unwrap().into())
    }

    // --- Single-document ops ---

    fn insert(&self, py: Python<'_>, doc: &Bound<PyDict>) -> PyResult<PyObject> {
        let d = py_to_document(doc)?;
        let result = self.inner.insert(d).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;
        Ok(doc_to_py(&result, py))
    }

    fn find_one(
        &self,
        py: Python<'_>,
        filter: Option<&Bound<PyDict>>,
    ) -> PyResult<PyObject> {
        let f = match filter {
            Some(d) => py_to_document(d)?,
            None => Document::new(),
        };
        let result = self.inner.find_one(f).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })?;
        match result {
            Some(doc) => Ok(doc_to_py(&doc, py)),
            None => Ok(py.None()),
        }
    }

    // --- Raw BSON passthrough (item #6) ---
    // Returns raw BSON bytes for Python-side decoding with pymongo's C decoder.
    // This avoids the slow recursive PyDict building in bson_to_py and fixes
    // the lossy _ => val.to_string() fallback that mangles datetimes, binary,
    // ObjectIds, etc.

    /// Find all matching documents and return as a list of raw BSON bytes.
    fn find_raw(&self, py: Python<'_>, filter: Option<&Bound<PyDict>>) -> PyResult<PyObject> {
        let f = match filter {
            Some(d) => py_to_document(d)?,
            None => Document::new(),
        };
        let results = self
            .inner
            .find(f)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .to_list()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let list = PyList::new(py, results.iter().map(|d| doc_to_bson_bytes(d, py)));
        Ok(list.unwrap().into())
    }

    /// Find one matching document and return as raw BSON bytes (or None).
    fn find_one_raw(
        &self,
        py: Python<'_>,
        filter: Option<&Bound<PyDict>>,
    ) -> PyResult<PyObject> {
        let f = match filter {
            Some(d) => py_to_document(d)?,
            None => Document::new(),
        };
        let result = self.inner.find_one(f).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })?;
        match result {
            Some(doc) => Ok(doc_to_bson_bytes(&doc, py)),
            None => Ok(py.None()),
        }
    }

    fn count(&self, filter: Option<&Bound<PyDict>>) -> PyResult<usize> {
        let f = match filter {
            Some(d) => py_to_document(d)?,
            None => Document::new(),
        };
        self.inner.count(f).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn update_one(
        &self,
        where_clause: &Bound<PyDict>,
        set: Option<&Bound<PyDict>>,
        unset: Option<Vec<String>>,
        inc: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let w = py_to_document(where_clause)?;
        let s = set.map(|d| py_to_document(d)).transpose()?;
        let i = inc.map(|d| py_to_document(d)).transpose()?;
        self.inner.update_one(w, s, unset, i).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn update_many(
        &self,
        where_clause: &Bound<PyDict>,
        set: Option<&Bound<PyDict>>,
        unset: Option<Vec<String>>,
        inc: Option<&Bound<PyDict>>,
    ) -> PyResult<usize> {
        let w = py_to_document(where_clause)?;
        let s = set.map(|d| py_to_document(d)).transpose()?;
        let i = inc.map(|d| py_to_document(d)).transpose()?;
        self.inner.update_many(w, s, unset, i).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn replace_one(
        &self,
        where_clause: &Bound<PyDict>,
        replacement: &Bound<PyDict>,
    ) -> PyResult<bool> {
        let w = py_to_document(where_clause)?;
        let r = py_to_document(replacement)?;
        self.inner.replace_one(w, r).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn delete_one(&self, where_clause: &Bound<PyDict>) -> PyResult<bool> {
        let w = py_to_document(where_clause)?;
        self.inner.delete_one(w).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn delete_many(&self, where_clause: &Bound<PyDict>) -> PyResult<usize> {
        let w = py_to_document(where_clause)?;
        self.inner.delete_many(w).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    /// Vector similarity search — returns list of (raw_bson_bytes, score) tuples.
    #[pyo3(signature = (filter, field, query_vector, limit=10))]
    fn vector_search_raw(
        &self,
        py: Python<'_>,
        filter: Option<&Bound<PyDict>>,
        field: &str,
        query_vector: Vec<f32>,
        limit: usize,
    ) -> PyResult<PyObject> {
        let f = match filter {
            Some(d) => py_to_document(d)?,
            None => Document::new(),
        };
        let results = self
            .inner
            .find(f)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .vector_search(field, query_vector, limit)
            .to_list()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let list = PyList::new(
            py,
            results.iter().map(|(doc, score)| {
                let bytes = doc_to_bson_bytes(doc, py);
                (bytes, *score as f64).to_object(py)
            }),
        );
        Ok(list.unwrap().into())
    }

    /// BM25 text search — returns list of (raw_bson_bytes, score) tuples.
    #[pyo3(signature = (filter, field, query, limit=10))]
    fn text_search_raw(
        &self,
        py: Python<'_>,
        filter: Option<&Bound<PyDict>>,
        field: &str,
        query: &str,
        limit: usize,
    ) -> PyResult<PyObject> {
        let f = match filter {
            Some(d) => py_to_document(d)?,
            None => Document::new(),
        };
        let results = self
            .inner
            .find(f)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .text_search(field, query, limit)
            .to_list()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let list = PyList::new(
            py,
            results.iter().map(|(doc, score)| {
                let bytes = doc_to_bson_bytes(doc, py);
                (bytes, *score as f64).to_object(py)
            }),
        );
        Ok(list.unwrap().into())
    }

    /// Return index configuration for compatibility shims.
    fn index_config(&self) -> PyResult<(Vec<String>, HashMap<String, usize>, Vec<String>)> {
        // Read the meta file to get the configured indexes.
        // This is a simplified approach — the Rust core doesn't expose
        // the IndexManager's field lists directly.
        let path = self.inner.path();
        let meta_path = path.with_extension("bson.meta");
        if let Ok(raw) = std::fs::read_to_string(&meta_path) {
            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&raw) {
                let indexes: Vec<String> = meta["indexes"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let vector_indexes: HashMap<String, usize> = meta["vector_indexes"]
                    .as_object()
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| v.as_u64().map(|d| (k.clone(), d as usize)))
                            .collect()
                    })
                    .unwrap_or_default();
                let text_indexes: Vec<String> = meta["text_indexes"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                return Ok((indexes, vector_indexes, text_indexes));
            }
        }
        Ok((Vec::new(), HashMap::new(), Vec::new()))
    }

    fn stats(&self) -> PyResult<HashMap<String, f64>> {
        let s = self.inner.stats().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })?;
        let mut map = HashMap::new();
        map.insert("documents".into(), s.documents as f64);
        map.insert("dead_records".into(), s.dead_records as f64);
        map.insert("file_size_bytes".into(), s.file_size_bytes as f64);
        map.insert("dead_ratio".into(), s.dead_ratio);
        Ok(map)
    }

    fn compact(&self) -> PyResult<()> {
        self.inner.compact().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn sync(&self) -> PyResult<()> {
        self.inner.sync().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn save_cache(&self) -> PyResult<()> {
        self.inner.save_cache().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn close(&self) -> PyResult<()> {
        self.inner.close().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn __repr__(&self) -> String {
        "NativeCollection(...)".into()
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeCollection>()?;
    Ok(())
}
