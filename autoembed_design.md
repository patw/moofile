# Moofile Autoembedding Design (v3 — Packaging)

## Hybrid Search + Autoembed Integration

### Current Signature

```python
# Python
def hybrid_search(self, text_field, vector_field, query_text, query_vector, limit=10)

# Rust
pub fn hybrid_search(self, text_field, vector_field, query_text, query_vector: Vec<f32>, limit)
```

### Proposed Change: `query_vector` becomes `Option<Vec<f32>>` / accept `None`

```python
# Python — query_vector defaults to None
def hybrid_search(self, text_field, vector_field, query_text, query_vector=None, limit=10)

# Rust
pub fn hybrid_search(
    self,
    text_field: impl Into<String>,
    vector_field: impl Into<String>,
    query_text: impl Into<String>,
    query_vector: Option<Vec<f32>>,
    limit: usize,
) -> HybridQuery
```

### Resolution Logic

Inside `HybridQuery::to_list()`:

```
query_vector provided?
  ├── YES → use as-is (backwards compatible)
  └── NO  → auto-embed from query_text
              ├── vector_field matches an autoembed source? (e.g., "content")
              │     → use its model + prefix + precision
              │     → target = auto_embeds["content"].target
              └── vector_field matches a raw vector field? (e.g., "embedding")
                    → find which autoembed source maps to it
                    → (reverse lookup: which source → "embedding"?)
                    → use that model
```

### Field Resolution (Dual-Use Field Names)

The `vector_field` parameter accepts either:

| You pass | What it means | Resolves to |
|----------|---------------|-------------|
| `"embedding"` | Raw vector field | `"embedding"` (unchanged) |
| `"content"` | Autoembed source | `auto_embeds["content"].target` |

This lets users write the most natural form:

```python
# One source field used for BOTH BM25 + semantic — clean!
db.find({}).hybrid_search("content", "content", "machine learning", None, 10)

# Or be explicit about the vector field
db.find({}).hybrid_search("content", "embedding", "machine learning", None, 10)

# Or pass a raw vector (existing behavior)
db.find({}).hybrid_search("content", "embedding", "ml", [0.1, 0.2, ...], 10)
```

---

## Packaging: Model Distribution

### Key Constraints

| Factor | Value |
|--------|-------|
| Model file (Q8_0) | **355 MB** |
| PyPI per-file limit | **100 MB** |
| Git + large binaries | Painful (LFS, clone size, history bloat) |
| User expectation | `pip install` is fast, model pulls separately |

### Decision: **Never ship the model in git or the wheel.**

Instead, use **auto-download on first use**, cached in a well-known location.

---

### Model URI Scheme

Users specify a model URI in the `auto_embed` config. Three forms:

```
hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf
├─ scheme ─┬─ HuggingFace repo ──┬─ filename ────┘
│          │                     │
│    "hf:" │ user/repo           │ :file.gguf (optional, default = repo default)
│          │                     │
│    Local file path (no scheme) │
./models/my-model.gguf           │
/absolute/path/to/model.gguf     │
```

### On First Open

```python
from moofile import Collection

db = Collection("data.bson",
    auto_embed={
        "content": {
            "model": "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
            "dims": 1024,
            "precision": "int8",
        }
    })
```

**Moofile does this at `Collection` open:**

```
1. Parse URI → "hf:", repo="jsonMartin/voyage-4-nano-gguf", file="voyage-4-nano-q8_0.gguf"
2. Check ~/.cache/moofile/models/jsonMartin/voyage-4-nano-gguf/voyage-4-nano-q8_0.gguf
3. Miss? → 
   a. Print: "Downloading voyage-4-nano-q8_0.gguf (355 MB)..."
   b. GET https://huggingface.co/jsonMartin/voyage-4-nano-gguf/resolve/main/voyage-4-nano-q8_0.gguf
   c. Stream to temp file with progress bar
   d. Rename into cache
4. Load model from local cache path
```

**Cache directory:**
- Linux: `~/.cache/moofile/models/`
- macOS: `~/Library/Caches/moofile/models/`
- Windows: `%LOCALAPPDATA%/moofile/cache/models/`

### Subsequent Opens

Model already cached → instant load. No network needed.

---

### Python Implementation Sketch

```python
import os
from pathlib import Path

# In Collection.__init__ or _load_embedding_model():

def _resolve_model_path(self, model_uri: str) -> str:
    """Resolve a model URI to a local file path."""
    if model_uri.startswith("hf:"):
        # hf:user/repo:filename.gguf
        rest = model_uri[3:]  # strip "hf:"
        if ":" in rest:
            repo_id, filename = rest.split(":", 1)
        else:
            repo_id = rest
            filename = None  # let HF pick default
        
        cache_dir = Path.home() / ".cache" / "moofile" / "models" / repo_id
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        if filename:
            local_path = cache_dir / filename
        else:
            # Need to list files or use a default
            local_path = cache_dir / "model.gguf"
        
        if not local_path.exists():
            self._download_model(repo_id, filename, local_path)
        
        return str(local_path)
    else:
        # Local path — use as-is
        return model_uri

def _download_model(self, repo_id: str, filename: str | None, dest: Path):
    """Download a model from HuggingFace Hub."""
    import requests
    from tqdm import tqdm  # optional progress bar
    
    url = f"https://huggingface.co/{repo_id}/resolve/main/{filename or ''}"
    print(f"Downloading {repo_id}/{filename or ''}...")
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        
        # Write to temp file, then atomically rename
        tmp = dest.with_suffix(".gguf.part")
        with open(tmp, "wb") as f:
            with tqdm(total=total, unit="B", unit_scale=True) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
        
        os.rename(tmp, dest)
```

### Rust Implementation Sketch

```rust
fn resolve_model_path(uri: &str, cache_dir: &Path) -> Result<PathBuf, MooFileError> {
    if let Some(hf_uri) = uri.strip_prefix("hf:") {
        let (repo_id, filename) = hf_uri.split_once(':')
            .map(|(r, f)| (r, Some(f)))
            .unwrap_or((hf_uri, None));
        
        let local_dir = cache_dir.join(repo_id);
        fs::create_dir_all(&local_dir)?;
        
        let local_path = match filename {
            Some(f) => local_dir.join(f),
            None => local_dir.join("model.gguf"), // default
        };
        
        if !local_path.exists() {
            download_hf_model(repo_id, filename, &local_path)?;
        }
        
        Ok(local_path)
    } else {
        // Local path
        Ok(PathBuf::from(uri))
    }
}

// The llama-gguf crate already has HuggingFace download support
// via its "huggingface" feature. We can delegate to that:
//   llama_gguf::huggingface::HfClient::download(...)
```

---

## Complete Example

```python
from moofile import Collection

# First run: auto-downloads model (~10-30s on good connection)
# Subsequent runs: instant (cached)
db = Collection("papers.bson",
    indexes=["year"],
    vector_indexes={"embedding": 1024},
    auto_embed={
        "abstract": {
            "model": "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
            "target": "embedding",
            "dims": 1024,
            "precision": "int8",
            "query_prefix": "Represent the query for retrieving supporting documents: ",
            "doc_prefix": "Represent the document for retrieval: ",
        },
    },
)

# Insert — auto-embeds
db.insert({"title": "Quantum ML", "abstract": "Quantum computing for ML...", "year": 2025})

# Semantic search
results = db.find({"year": {"$gte": 2024}}).semantic("abstract", "quantum algorithms", 5)

# Hybrid search — auto-embeds from query_text
results = db.find({"year": 2025}).hybrid_search(
    "abstract", "abstract", "quantum algorithms", None, 10
)
```

## Summary of Decisions

| Question | Decision | Why |
|----------|----------|-----|
| Model in git repo? | **No** | 355 MB binary destroys git UX |
| Model in wheel? | **No** | Exceeds 100 MB PyPI limit, punishes non-embedding users |
| Auto-download? | **Yes**, to `~/.cache/moofile/models/` | Standard pattern (Ollama, HF, llama.cpp all do this) |
| Local model path? | **Also supported** | Power users can manage models themselves |
| HF Hub identifier? | **Yes**, via `hf:user/repo:file` URI | Clean, standard, CDN-backed |
