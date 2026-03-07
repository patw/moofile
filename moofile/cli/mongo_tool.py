"""
moo2mongo — export/import between MooFile collections and MongoDB.

Export (default):
  moo2mongo <collection.bson> --uri <uri> --collection <name>

Import:
  moo2mongo --import --uri <uri> --collection <name> <collection.bson>
"""
import argparse
import sys

from moofile import Collection
from moofile.cli import serialize_doc


BATCH_SIZE = 1000


def _get_mongo_db(client, uri):
    from pymongo.uri_parser import parse_uri
    parsed = parse_uri(uri)
    db_name = parsed.get('database')
    if not db_name:
        print(
            "Error: MongoDB URI must include a database name "
            "(e.g. mongodb://localhost/mydb)",
            file=sys.stderr,
        )
        sys.exit(1)
    return client[db_name]


def cmd_export(bson_path, uri, collection_name, drop, quiet):
    from pymongo import MongoClient

    with Collection(bson_path, readonly=True) as db:
        docs = [serialize_doc(d) for d in db.find().to_list()]

    client = MongoClient(uri)
    try:
        mongo_db = _get_mongo_db(client, uri)
        mongo_col = mongo_db[collection_name]
        if drop:
            mongo_col.drop()
        total = 0
        for start in range(0, len(docs), BATCH_SIZE):
            batch = docs[start:start + BATCH_SIZE]
            if batch:
                mongo_col.insert_many(batch)
            total += len(batch)
    finally:
        client.close()

    if not quiet:
        print(f"Exported {total} documents to {uri} / {collection_name}", file=sys.stderr)


def cmd_import(uri, collection_name, bson_path, indexes, quiet):
    from pymongo import MongoClient

    client = MongoClient(uri)
    try:
        mongo_db = _get_mongo_db(client, uri)
        mongo_col = mongo_db[collection_name]
        docs = [serialize_doc(d) for d in mongo_col.find()]
    finally:
        client.close()

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
        prog='moo2mongo',
        description='Export/import MooFile collections to/from MongoDB',
    )
    parser.add_argument(
        '--import', dest='do_import', action='store_true',
        help='Import mode: MongoDB → MooFile (default is export: MooFile → MongoDB)',
    )
    parser.add_argument('--uri', required=True, help='MongoDB connection URI')
    parser.add_argument('--collection', required=True, help='MongoDB collection name')
    parser.add_argument('--drop', action='store_true', help='Drop target MongoDB collection before export')
    parser.add_argument(
        '--indexes',
        help='Comma-separated fields to index on import (MooFile side)',
        default='',
    )
    parser.add_argument('--quiet', action='store_true', help='Suppress progress output')
    parser.add_argument('bson', help='MooFile collection path (collection.bson)')

    args = parser.parse_args()

    if args.do_import:
        cmd_import(args.uri, args.collection, args.bson, args.indexes, args.quiet)
    else:
        cmd_export(args.bson, args.uri, args.collection, args.drop, args.quiet)


if __name__ == '__main__':
    main()
