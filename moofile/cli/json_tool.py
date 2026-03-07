"""
moo2json — export/import between MooFile collections and JSON files.

Export (default):
  moo2json <collection.bson> <output.json>
  moo2json <collection.bson> -               # stdout

Import:
  moo2json --import <input.json> <collection.bson>
  moo2json --import - <collection.bson>      # stdin
"""
import argparse
import json
import sys

from moofile import Collection
from moofile.cli import serialize_doc


BATCH_SIZE = 1000


def _load_json_docs(fp):
    """Read JSON array or NDJSON from a file-like object."""
    raw = fp.read().strip()
    if not raw:
        return []
    if raw[0] == '[':
        return json.loads(raw)
    # NDJSON: one document per line
    docs = []
    for line in raw.splitlines():
        line = line.strip()
        if line:
            docs.append(json.loads(line))
    return docs


def cmd_export(bson_path, output_path, quiet):
    with Collection(bson_path, readonly=True) as db:
        docs = [serialize_doc(d) for d in db.find().to_list()]

    if output_path == '-':
        json.dump(docs, sys.stdout, indent=2)
        sys.stdout.write('\n')
    else:
        with open(output_path, 'w') as f:
            json.dump(docs, f, indent=2)
            f.write('\n')

    if not quiet:
        dest = 'stdout' if output_path == '-' else output_path
        print(f"Exported {len(docs)} documents to {dest}", file=sys.stderr)


def cmd_import(input_path, bson_path, indexes, quiet):
    if input_path == '-':
        docs = _load_json_docs(sys.stdin)
    else:
        with open(input_path) as f:
            docs = _load_json_docs(f)

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
        prog='moo2json',
        description='Export/import MooFile collections to/from JSON',
    )
    parser.add_argument(
        '--import', dest='do_import', action='store_true',
        help='Import mode: JSON → MooFile (default is export: MooFile → JSON)',
    )
    parser.add_argument(
        '--indexes',
        help='Comma-separated fields to index on import (e.g. email,age)',
        default='',
    )
    parser.add_argument('--quiet', action='store_true', help='Suppress progress output')
    parser.add_argument('src', help='Source file (collection.bson or input.json or -)')
    parser.add_argument('dst', help='Destination file (output.json or collection.bson or -)')

    args = parser.parse_args()

    if args.do_import:
        cmd_import(args.src, args.dst, args.indexes, args.quiet)
    else:
        cmd_export(args.src, args.dst, args.quiet)


if __name__ == '__main__':
    main()
