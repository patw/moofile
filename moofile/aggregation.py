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
"""

import builtins as _builtins


class AggFunc:
    """Descriptor for a single aggregation operation."""

    __slots__ = ("output_name", "field", "_func")

    def __init__(self, output_name: str, field, func) -> None:
        self.output_name = output_name
        self.field = field
        self._func = func

    def compute(self, docs: list):
        return self._func(docs)


def count() -> AggFunc:
    """Count the number of documents in the group."""
    return AggFunc("count", None, lambda docs: len(docs))


def sum(field: str) -> AggFunc:
    """Sum of field values across the group."""
    _f = field
    return AggFunc(
        f"sum_{_f}",
        _f,
        lambda docs: _builtins.sum(d[_f] for d in docs if _f in d),
    )


def mean(field: str) -> AggFunc:
    """Arithmetic mean of field values across the group."""
    _f = field

    def _mean(docs):
        vals = [d[_f] for d in docs if _f in d]
        return _builtins.sum(vals) / len(vals) if vals else None

    return AggFunc(f"mean_{_f}", _f, _mean)


def min(field: str) -> AggFunc:
    """Minimum field value in the group."""
    _f = field

    def _min(docs):
        vals = [d[_f] for d in docs if _f in d]
        return _builtins.min(vals) if vals else None

    return AggFunc(f"min_{_f}", _f, _min)


def max(field: str) -> AggFunc:
    """Maximum field value in the group."""
    _f = field

    def _max(docs):
        vals = [d[_f] for d in docs if _f in d]
        return _builtins.max(vals) if vals else None

    return AggFunc(f"max_{_f}", _f, _max)


def collect(field: str) -> AggFunc:
    """Collect all field values in the group as a list."""
    _f = field
    return AggFunc(
        f"collect_{_f}",
        _f,
        lambda docs: [d[_f] for d in docs if _f in d],
    )


def first(field: str) -> AggFunc:
    """First value of field encountered in the group."""
    _f = field

    def _first(docs):
        for d in docs:
            if _f in d:
                return d[_f]
        return None

    return AggFunc(f"first_{_f}", _f, _first)


def last(field: str) -> AggFunc:
    """Last value of field encountered in the group."""
    _f = field

    def _last(docs):
        result = None
        for d in docs:
            if _f in d:
                result = d[_f]
        return result

    return AggFunc(f"last_{_f}", _f, _last)
