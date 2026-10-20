"""
Aggregation functions for use with Query.group().agg(...).

Output field naming convention:
    count()          -> "count"
    sum("revenue")   -> "sum_revenue"
    mean("age")      -> "mean_age"
    min("score")     -> "min_score"
    max("score")     -> "max_score"
    collect("tags")  -> "collect_tags"
    first("name")    -> "first_name"
    last("name")     -> "last_name"

Semantics are the Rust core's, which is the system of record (and which the
native backend now runs directly): ``sum``/``mean`` take numbers only and skip
anything else (including bools); integers sum to an integer; ``min``/``max``
use the same total order as ``sort()``; ``first``/``last`` read the first/last
document in the group, giving ``None`` if it lacks the field.
"""

import builtins as _builtins

from .query import sort_key


class AggFunc:
    """Descriptor for a single aggregation operation.

    ``kind`` names the operation for the native core; an ``AggFunc`` built by
    hand with no ``kind`` still works, but makes the native backend fall back
    to grouping in Python.
    """

    __slots__ = ("output_name", "field", "_func", "kind")

    def __init__(self, output_name: str, field, func, kind=None) -> None:
        self.output_name = output_name
        self.field = field
        self._func = func
        self.kind = kind

    def compute(self, docs: list):
        return self._func(docs)


def _numbers(docs, field):
    return [
        d[field] for d in docs
        if field in d and isinstance(d[field], (int, float)) and not isinstance(d[field], bool)
    ]


def count() -> AggFunc:
    """Count the number of documents in the group."""
    return AggFunc("count", None, lambda docs: len(docs), "count")


def sum(field: str) -> AggFunc:
    """Sum of the numeric values of field across the group."""
    def _sum(docs):
        vals = _numbers(docs, field)
        if _builtins.all(isinstance(v, int) for v in vals):
            return _builtins.sum(vals)
        return float(_builtins.sum(vals))

    return AggFunc(f"sum_{field}", field, _sum, "sum")


def mean(field: str) -> AggFunc:
    """Arithmetic mean of the numeric values of field across the group."""
    def _mean(docs):
        vals = _numbers(docs, field)
        return _builtins.sum(vals) / len(vals) if vals else None

    return AggFunc(f"mean_{field}", field, _mean, "mean")


def _extreme(docs, field, better):
    best = None
    found = False
    for d in docs:
        if field in d:
            v = d[field]
            if not found or better(sort_key(v), sort_key(best)):
                best, found = v, True
    return best


def min(field: str) -> AggFunc:
    """Minimum field value in the group (first of equals)."""
    return AggFunc(
        f"min_{field}", field, lambda docs: _extreme(docs, field, lambda a, b: a < b), "min"
    )


def max(field: str) -> AggFunc:
    """Maximum field value in the group (first of equals)."""
    return AggFunc(
        f"max_{field}", field, lambda docs: _extreme(docs, field, lambda a, b: a > b), "max"
    )


def collect(field: str) -> AggFunc:
    """Collect all field values in the group as a list."""
    return AggFunc(
        f"collect_{field}", field, lambda docs: [d[field] for d in docs if field in d], "collect"
    )


def first(field: str) -> AggFunc:
    """The field's value in the group's first document (None if absent)."""
    return AggFunc(
        f"first_{field}", field, lambda docs: docs[0].get(field) if docs else None, "first"
    )


def last(field: str) -> AggFunc:
    """The field's value in the group's last document (None if absent)."""
    return AggFunc(
        f"last_{field}", field, lambda docs: docs[-1].get(field) if docs else None, "last"
    )
