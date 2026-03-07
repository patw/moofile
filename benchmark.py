"""
MooFile benchmark — measures throughput for common operations.

Run with:
    python benchmark.py

Adjust N_DOCS to test at different scales.
"""

import os
import random
import string
import tempfile
import time
from contextlib import contextmanager

from moofile import Collection, count, mean, sum


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

N_DOCS = 10_000          # documents to insert
N_LOOKUPS = 1000        # indexed lookups to perform
N_SCANS = 100            # full-scan queries to perform
STATUSES = ["active", "inactive", "trial", "expired"]
CITIES = ["NYC", "LA", "Chicago", "Houston", "Phoenix", "Austin"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextmanager
def timer(label: str):
    t0 = time.perf_counter()
    yield
    elapsed = time.perf_counter() - t0
    print(f"  {label:<40} {elapsed*1000:>8.1f} ms")


def random_email(i: int) -> str:
    return f"user{i}@example.com"


def make_doc(i: int) -> dict:
    return {
        "_id": str(i),
        "email": random_email(i),
        "age": random.randint(18, 80),
        "status": random.choice(STATUSES),
        "city": random.choice(CITIES),
        "score": random.random() * 100,
        "name": "".join(random.choices(string.ascii_lowercase, k=8)),
    }


def hr():
    print("-" * 55)


# ---------------------------------------------------------------------------
# Benchmark suite
# ---------------------------------------------------------------------------

def run(tmp_dir: str) -> None:
    path = os.path.join(tmp_dir, "bench.bson")

    print(f"\nMooFile Benchmark  (N_DOCS={N_DOCS:,})\n")
    hr()

    # -------- Insert --------------------------------------------------------
    print("INSERT")
    docs = [make_doc(i) for i in range(N_DOCS)]

    with Collection(path, indexes=["email", "age", "status", "city"]) as db:

        with timer(f"insert_many {N_DOCS:,} docs"):
            db.insert_many(docs)

        s = db.stats()
        print(f"  file size after insert: {s['file_size_bytes'] / 1024:.1f} KB")
        hr()

        # -------- Indexed lookup ---------------------------------------------
        print("INDEXED LOOKUP")
        emails = [random_email(random.randint(0, N_DOCS - 1)) for _ in range(N_LOOKUPS)]

        with timer(f"find_one by email ({N_LOOKUPS:,} lookups)"):
            for email in emails:
                db.find_one({"email": email})

        ages = [random.randint(20, 70) for _ in range(N_LOOKUPS)]
        with timer(f"find by age range ({N_LOOKUPS:,} range queries)"):
            for age in ages:
                db.find({"age": {"$gte": age, "$lt": age + 10}}).to_list()

        hr()

        # -------- Full scan --------------------------------------------------
        print("FULL SCAN (non-indexed field)")
        names = [docs[random.randint(0, N_DOCS - 1)]["name"] for _ in range(N_SCANS)]

        with timer(f"find by name ({N_SCANS:,} full scans)"):
            for name in names:
                db.find({"name": name}).to_list()

        hr()

        # -------- Count ------------------------------------------------------
        print("COUNT")

        with timer("count all documents"):
            db.count()

        with timer("count indexed: status='active'"):
            db.count({"status": "active"})

        hr()

        # -------- Update -----------------------------------------------------
        print("UPDATE")
        ids_to_update = [str(random.randint(0, N_DOCS - 1)) for _ in range(1_000)]

        with timer("update_one $set (1,000 updates)"):
            for _id in ids_to_update:
                try:
                    db.update_one({"_id": _id}, set={"score": 0.0})
                except Exception:
                    pass

        hr()

        # -------- Delete -----------------------------------------------------
        print("DELETE")
        ids_to_delete = list({str(random.randint(0, N_DOCS // 2)) for _ in range(500)})

        with timer(f"delete_one ({len(ids_to_delete)} deletes)"):
            for _id in ids_to_delete:
                db.delete_one({"_id": _id})

        s = db.stats()
        print(f"  dead_ratio after updates+deletes: {s['dead_ratio']:.1%}")
        hr()

        # -------- Aggregation ------------------------------------------------
        print("AGGREGATION")

        with timer("group by city, count+mean(age)+sum(score)"):
            (
                db.find()
                .group("city")
                .agg(count(), mean("age"), sum("score"))
                .sort("count", descending=True)
                .to_list()
            )

        with timer("group by status, count (filtered: age>30)"):
            (
                db.find({"age": {"$gt": 30}})
                .group("status")
                .agg(count())
                .to_list()
            )

        hr()

        # -------- Compaction -------------------------------------------------
        print("COMPACTION")
        size_before = s["file_size_bytes"]

        with timer("compact()"):
            db.compact()

        size_after = db.stats()["file_size_bytes"]
        saved = (size_before - size_after) / 1024
        print(f"  file size before: {size_before / 1024:.1f} KB")
        print(f"  file size after:  {size_after / 1024:.1f} KB  (saved {saved:.1f} KB)")
        hr()

        # -------- Open / reindex ---------------------------------------------
        print("OPEN (scan + index rebuild)")

    # Re-open outside the with block to time cold open
    with timer(f"Collection open + reindex ({db.count() + len(ids_to_delete)} records on disk)"):
        with Collection(path, indexes=["email", "age", "status", "city"]) as db2:
            _ = db2.count()

    hr()
    print("Done.\n")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        run(tmp_dir)
