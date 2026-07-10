//! PyO3 native binding for MooFile.
//!
//! Compiles to `_native.so` and is loaded by `moofile/__init__.py`
//! as a transparent drop-in for the pure-Python `Collection`.

use std::collections::HashMap;

use bson::{Bson, Document};
use moofile_core::Collection as RustCollection;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

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
    #[pyo3(signature = (path, indexes=None, vector_indexes=None, text_indexes=None, readonly=false))]
    fn new(
        path: String,
        indexes: Option<Vec<String>>,
        vector_indexes: Option<HashMap<String, usize>>,
        text_indexes: Option<Vec<String>>,
        readonly: bool,
    ) -> PyResult<Self> {
        let mut builder = RustCollection::builder(&path);
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
    fn insert_many(&self, docs: &Bound<PyList>) -> PyResult<usize> {
        let mut rust_docs = Vec::with_capacity(docs.len());
        for item in docs.iter() {
            let dict = item.downcast::<PyDict>()?;
            rust_docs.push(py_to_document(dict)?);
        }
        let results = self.inner.insert_many(rust_docs).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;
        Ok(results.len())
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
