"""
import_export.py — demonstrate the moo2json, moo2sqlite, and moo2mongo CLI tools.

Run this script directly to see a full round-trip for each tool:

    python examples/import_export.py

The script creates a temporary collection, exports it, then re-imports it
into a new collection and compares the document counts.
"""
import subprocess
import sys
import tempfile
from pathlib import Path

from moofile import Collection


# ---------------------------------------------------------------------------
# 1. Create a sample collection
# ---------------------------------------------------------------------------

def make_sample_collection(bson_path: str) -> int:
    docs = [
        {"name": "Alice",   "email": "alice@example.com",   "age": 30, "tags": ["admin", "user"]},
        {"name": "Bob",     "email": "bob@example.com",     "age": 22, "address": {"city": "Berlin"}},
        {"name": "Carol",   "email": "carol@example.com",   "age": 40, "tags": ["user"]},
        {"name": "Dave",    "email": "dave@example.com",    "age": 35},
        {"name": "Eve",     "email": "eve@example.com",     "age": 28, "tags": ["admin"]},
    ]
    with Collection(bson_path, indexes=["email"]) as db:
        db.insert_many(docs)
    return len(docs)


def doc_count(bson_path: str) -> int:
    with Collection(bson_path, readonly=True) as db:
        return db.find().count()


def run(*cmd, check=True):
    print("$", " ".join(str(c) for c in cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if check and result.returncode != 0:
        sys.exit(result.returncode)
    return result


# ---------------------------------------------------------------------------
# 2. moo2json round-trip
# ---------------------------------------------------------------------------

def demo_json(tmp: Path, n: int):
    print("\n=== moo2json round-trip ===")
    src = str(tmp / "source.bson")
    make_sample_collection(src)

    json_file = str(tmp / "export.json")
    dst = str(tmp / "from_json.bson")

    run("moo2json", src, json_file)
    run("moo2json", "--import", json_file, dst, "--indexes", "email")

    imported = doc_count(dst)
    print(f"Original: {n}  Imported: {imported}  Match: {imported == n}")
    assert imported == n, "moo2json round-trip count mismatch"


# ---------------------------------------------------------------------------
# 3. moo2sqlite round-trip
# ---------------------------------------------------------------------------

def demo_sqlite(tmp: Path, n: int):
    print("\n=== moo2sqlite round-trip ===")
    src = str(tmp / "source2.bson")
    make_sample_collection(src)

    db_file = str(tmp / "export.db")
    dst = str(tmp / "from_sqlite.bson")

    run("moo2sqlite", src, db_file)
    run("moo2sqlite", "--import", db_file, dst, "--indexes", "email")

    imported = doc_count(dst)
    print(f"Original: {n}  Imported: {imported}  Match: {imported == n}")
    assert imported == n, "moo2sqlite round-trip count mismatch"


# ---------------------------------------------------------------------------
# 4. moo2mongo round-trip (skipped if MongoDB is not reachable)
# ---------------------------------------------------------------------------

def demo_mongo(tmp: Path, n: int):
    print("\n=== moo2mongo round-trip (skipped if MongoDB unavailable) ===")
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost/moofile_demo_test", serverSelectionTimeoutMS=1000)
        client.server_info()
    except Exception as e:
        print(f"Skipping: {e}")
        return

    src = str(tmp / "source3.bson")
    make_sample_collection(src)
    dst = str(tmp / "from_mongo.bson")

    run("moo2mongo", src, "--uri", "mongodb://localhost/moofile_demo_test", "--collection", "demo", "--drop")
    run("moo2mongo", "--import", dst, "--uri", "mongodb://localhost/moofile_demo_test", "--collection", "demo", "--indexes", "email")

    imported = doc_count(dst)
    print(f"Original: {n}  Imported: {imported}  Match: {imported == n}")
    client["moofile_demo_test"].drop_collection("demo")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        n = 5  # number of sample docs

        demo_json(tmp, n)
        demo_sqlite(tmp, n)
        demo_mongo(tmp, n)

    print("\nAll demos completed.")


if __name__ == "__main__":
    main()
