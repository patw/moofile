"""
moosh — interactive MooFile shell.

Opens a collection and drops into a Python REPL with `db` pre-bound.

    moosh users.bson
    moosh users.bson --indexes email,age
    moosh users.bson --readonly
"""
import argparse
import code
import sys

from moofile import (
    Collection,
    DocumentNotFoundError,
    DuplicateKeyError,
    MooFileError,
    ReadOnlyError,
    collect,
    count,
    first,
    last,
    max,
    mean,
    min,
    sum,
)


BANNER = """\
moosh — MooFile interactive shell
  db        : Collection (use db.find(), db.insert(), db.update_one(), ...)
  Aggregation helpers : count  sum  mean  min  max  collect  first  last

Quick examples:
  db.find().to_list()
  db.find({{"age": {{"$gt": 25}}}}).sort("age").limit(5).to_list()
  db.insert({{"name": "Alice", "age": 30}})
  db.find().group("status").agg(count(), mean("age")).to_list()

Type exit() or Ctrl-D to quit.
Collection: {path}  ({mode})
"""


def main():
    parser = argparse.ArgumentParser(
        prog='moosh',
        description='Interactive MooFile shell — opens a collection in a Python REPL',
    )
    parser.add_argument('bson', help='Path to the .bson collection file')
    parser.add_argument(
        '--indexes',
        help='Comma-separated fields to index (e.g. email,age)',
        default='',
    )
    parser.add_argument('--readonly', action='store_true', help='Open collection read-only')
    args = parser.parse_args()

    parsed_indexes = [i.strip() for i in args.indexes.split(',')] if args.indexes else []
    mode = 'read-only' if args.readonly else 'read-write'

    with Collection(args.bson, indexes=parsed_indexes, readonly=args.readonly) as db:
        banner = BANNER.format(path=args.bson, mode=mode)
        local_vars = {
            'db': db,
            # aggregation helpers
            'count': count, 'sum': sum, 'mean': mean,
            'min': min, 'max': max, 'collect': collect,
            'first': first, 'last': last,
            # exceptions (handy for except clauses)
            'MooFileError': MooFileError,
            'DuplicateKeyError': DuplicateKeyError,
            'DocumentNotFoundError': DocumentNotFoundError,
            'ReadOnlyError': ReadOnlyError,
        }
        code.interact(banner=banner, local=local_vars, exitmsg='')


if __name__ == '__main__':
    main()
