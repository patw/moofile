"""
moo2sqlite — export/import between MooFile collections and SQLite databases.

Export (default):
  moo2sqlite <collection.bson> <database.sqlite> [--table <name>]

Import:
  moo2sqlite --import <database.sqlite> <collection.bson> [--table <name>]
"""
import argparse
import sqlite3
import sys
from pathlib import Path

from moofile import Collection
from moofile.cli import flatten_doc, unflatten_doc


BATCH_SIZE = 500
SAMPLE_SIZE = 100


def _derive_table_name(bson_path):
    return Path(bson_path).stem.replace('.', '_').replace('-', '_')


def cmd_export(bson_path, db_path, table_name, drop, quiet):
    if table_name is None:
        table_name = _derive_table_name(bson_path)

    with Collection(bson_path, readonly=True) as db:
        docs = db.find().to_list()

    if not docs:
        if not quiet:
            print(f"No documents found in {bson_path}", file=sys.stderr)
        return

    # Determine column set from a sample then full scan
    sample = docs[:SAMPLE_SIZE]
    columns = list(dict.fromkeys(k for d in sample for k in d))
    # Ensure all columns from all docs are covered
    all_keys = dict.fromkeys(k for d in docs for k in d)
    for k in all_keys:
        if k not in columns:
            columns.append(k)

    flat_docs = [flatten_doc(d) for d in docs]

    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        if drop:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        col_defs = ', '.join(
            f'"_id" TEXT PRIMARY KEY' if c == '_id' else f'"{c}" TEXT'
            for c in columns
        )
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

        placeholders = ', '.join('?' for _ in columns)
        insert_sql = f'INSERT OR REPLACE INTO "{table_name}" VALUES ({placeholders})'

        for start in range(0, len(flat_docs), BATCH_SIZE):
            batch = flat_docs[start:start + BATCH_SIZE]
            rows = [tuple(str(d.get(c, '')) if d.get(c) is not None else None for c in columns) for d in batch]
            cur.executemany(insert_sql, rows)

        con.commit()
    finally:
        con.close()

    if not quiet:
        print(f"Exported {len(docs)} documents to {db_path} (table: {table_name})", file=sys.stderr)


def cmd_import(db_path, bson_path, table_name, indexes, quiet):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        if table_name is None:
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cur.fetchall()]
            if not tables:
                print(f"Error: no tables found in {db_path}", file=sys.stderr)
                sys.exit(1)
            if len(tables) > 1:
                print(
                    f"Error: multiple tables in {db_path}: {tables}. "
                    "Specify one with --table.",
                    file=sys.stderr,
                )
                sys.exit(1)
            table_name = tables[0]
        cur = con.cursor()
        cur.execute(f'SELECT * FROM "{table_name}"')
        rows = [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

    docs = [unflatten_doc(row) for row in rows]
    parsed_indexes = [i.strip() for i in indexes.split(',')] if indexes else []

    total = 0
    with Collection(bson_path, indexes=parsed_indexes) as db:
        for start in range(0, len(docs), BATCH_SIZE):
            batch = docs[start:start + BATCH_SIZE]
            db.insert_many(batch)
            total += len(batch)

    if not quiet:
        print(f"Imported {total} documents into {bson_path}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        prog='moo2sqlite',
        description='Export/import MooFile collections to/from SQLite',
    )
    parser.add_argument(
        '--import', dest='do_import', action='store_true',
        help='Import mode: SQLite → MooFile (default is export: MooFile → SQLite)',
    )
    parser.add_argument('--table', help='SQLite table name (default: derived from bson filename stem)')
    parser.add_argument('--drop', action='store_true', help='Drop existing table before export')
    parser.add_argument(
        '--indexes',
        help='Comma-separated fields to index on import (MooFile side)',
        default='',
    )
    parser.add_argument('--quiet', action='store_true', help='Suppress progress output')
    parser.add_argument('src', help='Source: collection.bson (export) or database.sqlite (import)')
    parser.add_argument('dst', help='Destination: database.sqlite (export) or collection.bson (import)')

    args = parser.parse_args()

    if args.do_import:
        cmd_import(args.src, args.dst, args.table, args.indexes, args.quiet)
    else:
        cmd_export(args.src, args.dst, args.table, args.drop, args.quiet)


if __name__ == '__main__':
    main()
