#!/usr/bin/env python3
"""
MooFile — Python vs Rust Native benchmark.

Run:
    PYTHONPATH=. python bench_native.py
"""

import os, random, string, sys, tempfile, time
import numpy as np

N_DOCS = 10_000
N_LOOKUPS = 2_000
N_RANGES = 1_000
N_SCANS = 200
N_UPDATES = 500
N_DELETES = 200
N_VECTOR = 50
N_TEXT = 50
VECTOR_DIM = 128

STATUSES = ["active", "inactive", "trial", "expired"]
CITIES = ["NYC", "LA", "Chicago", "Houston", "Phoenix", "Austin"]
WORDS = ["machine","learning","data","science","neural","network","deep",
         "algorithm","python","database","analytics","system","cloud","server"]

def randid(): return str(random.randint(0, N_DOCS-1))
def randemail(i): return f"user{i:06d}@example.com"

def make_doc(i):
    return {
        "_id": str(i),
        "email": randemail(i),
        "age": random.randint(18, 80),
        "status": random.choice(STATUSES),
        "city": random.choice(CITIES),
        "score": random.random() * 100,
        "name": "".join(random.choices(string.ascii_lowercase, k=8)),
        "content": " ".join(random.choices(WORDS, k=random.randint(5, 15))),
        "embedding": np.random.normal(0, 1, VECTOR_DIM).tolist(),
    }

def bench(name, CollectionClass, tmp_dir):
    path = os.path.join(tmp_dir, f"bench_{name}.bson")
    t = {}  # results dict
    
    docs = [make_doc(i) for i in range(N_DOCS)]
    
    # --- Insert ---
    t0 = time.perf_counter()
    db = CollectionClass(path, indexes=["email","age","status","city"],
                         vector_indexes={"embedding": VECTOR_DIM},
                         text_indexes=["content"])
    db.insert_many(docs)
    t["insert"] = time.perf_counter() - t0
    
    # --- Cold open ---
    db.close()
    t0 = time.perf_counter()
    db = CollectionClass(path, indexes=["email","age","status","city"],
                         vector_indexes={"embedding": VECTOR_DIM},
                         text_indexes=["content"])
    t["cold_open"] = time.perf_counter() - t0
    
    fs = db.stats()["file_size_bytes"]
    
    # --- Indexed exact lookup ---
    emails = [randemail(random.randint(0, N_DOCS-1)) for _ in range(N_LOOKUPS)]
    t0 = time.perf_counter()
    for e in emails:
        db.find_one({"email": e})
    t["lookup_exact"] = time.perf_counter() - t0
    
    # --- Indexed range ---
    ages = [random.randint(20, 70) for _ in range(N_RANGES)]
    t0 = time.perf_counter()
    for a in ages:
        db.find({"age": {"$gte": a, "$lt": a+10}}).to_list()
    t["lookup_range"] = time.perf_counter() - t0
    
    # --- Full scan ---
    names = [docs[random.randint(0, N_DOCS-1)]["name"] for _ in range(N_SCANS)]
    t0 = time.perf_counter()
    for n in names:
        db.find({"name": n}).to_list()
    t["full_scan"] = time.perf_counter() - t0
    
    # --- Count ---
    t0 = time.perf_counter()
    db.count({})
    db.count({"status": "active"})
    t["count"] = time.perf_counter() - t0
    
    # --- Sort+skip+limit ---
    t0 = time.perf_counter()
    db.find({"age": {"$gt": 25}}).sort("age", True).skip(100).limit(50).to_list()
    t["sort_skip_limit"] = time.perf_counter() - t0
    
    # --- Update ---
    uids = [str(random.randint(0, N_DOCS-1)) for _ in range(N_UPDATES)]
    t0 = time.perf_counter()
    for uid in uids:
        try:
            db.update_one({"_id": uid}, set={"score": 0.0})
        except Exception:
            pass
    t["update"] = time.perf_counter() - t0
    
    # --- Delete ---
    dids = list({str(random.randint(0, N_DOCS//2)) for _ in range(N_DELETES)})
    t0 = time.perf_counter()
    for did in dids:
        db.delete_one({"_id": did})
    t["delete"] = time.perf_counter() - t0
    
    # --- Vector search ---
    qvecs = [np.random.normal(0, 1, VECTOR_DIM).tolist() for _ in range(N_VECTOR)]
    t0 = time.perf_counter()
    try:
        for qv in qvecs:
            db.find({}).vector_search("embedding", qv, limit=10).to_list()
        t["vector"] = time.perf_counter() - t0
    except (AttributeError, NotImplementedError):
        t["vector"] = None  # not supported
    
    # --- Text search ---
    tqueries = [random.choice(WORDS) for _ in range(N_TEXT)]
    t0 = time.perf_counter()
    try:
        for tq in tqueries:
            db.find({}).text_search("content", tq, limit=10).to_list()
        t["text"] = time.perf_counter() - t0
    except (AttributeError, NotImplementedError):
        t["text"] = None
    
    # --- Aggregation ---
    t0 = time.perf_counter()
    try:
        from moofile import count, mean, sum
        db.find({}).group("city").agg(count(), mean("age"), sum("score")).to_list()
        t["agg"] = time.perf_counter() - t0
    except (AttributeError, NotImplementedError):
        t["agg"] = None
    
    # --- Compact ---
    t0 = time.perf_counter()
    db.compact()
    t["compact"] = time.perf_counter() - t0
    
    db.close()
    return t, fs


def main():
    sys.path.insert(0, os.path.dirname(__file__) or ".")
    from moofile.collection import Collection as PyCollection
    
    import moofile
    if not moofile._NATIVE_LOADED:
        print("ERROR: Native extension not built. See bench_native.py header.")
        sys.exit(1)
    import moofile._rust_adapter as adapter
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        print(f"\n{'='*72}")
        print(f"  MooFile Benchmark — {N_DOCS:,} docs, vec={VECTOR_DIM}d")
        print(f"{'='*72}")
        
        py, py_fs = bench("🐍 Python", PyCollection, tmp_dir)
        rs, rs_fs = bench("🦀 Rust  ", adapter.Collection, tmp_dir)
    
    # ── Print table ────────────────────────────────────────────
    ops = [
        ("insert",          f"insert_many ({N_DOCS:,})"),
        ("cold_open",       f"cold open"),
        ("lookup_exact",    f"find_one ({N_LOOKUPS:,}x)"),
        ("lookup_range",    f"range find ({N_RANGES:,}x)"),
        ("full_scan",       f"full scan ({N_SCANS:,}x)"),
        ("count",           "count ×2"),
        ("sort_skip_limit", "sort+skip+limit"),
        ("update",          f"update_one ({N_UPDATES:,}x)"),
        ("delete",          f"delete_one ({N_DELETES:,}x)"),
        ("vector",          f"vector_search ({N_VECTOR:,}x)"),
        ("text",            f"text_search ({N_TEXT:,}x)"),
        ("agg",             "group+agg"),
        ("compact",         "compact"),
    ]
    
    print(f"\n{'Operation':<30} {'Python':>10} {'Rust':>10} {'Speedup':>10}")
    print("-" * 64)
    
    py_total = rs_total = 0.0
    py_cnt = rs_cnt = 0
    
    for key, label in ops:
        p = py.get(key)
        r = rs.get(key)
        py_total += p or 0
        rs_total += r or 0
        
        if p is None:
            p_str = "N/A"
            r_str = f"{r*1000:7.1f}ms" if r else "N/A"
            su = "-"
        elif r is None:
            p_str = f"{p*1000:7.1f}ms"
            r_str = "N/A"
            su = "-"
        else:
            p_str = f"{p*1000:7.1f}ms"
            r_str = f"{r*1000:7.1f}ms"
            su = f"{p/r:6.1f}x" if r > 0 else "-"
        
        print(f"  {label:<28} {p_str:>10} {r_str:>10} {su:>10}")
    
    print("-" * 64)
    print(f"  {'TOTAL':<28} {py_total*1000:>9.0f}ms {rs_total*1000:>9.0f}ms {py_total/rs_total:>9.1f}x")
    
    print(f"\n  Python file: {py_fs/1024:.0f} KB | Rust file: {rs_fs/1024:.0f} KB")
    print()

if __name__ == "__main__":
    main()
