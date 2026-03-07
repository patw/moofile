from bson import ObjectId
import json


def serialize_value(v):
    """Recursively convert BSON types to JSON-safe Python types."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, dict):
        return {k: serialize_value(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [serialize_value(i) for i in v]
    return v


def serialize_doc(doc):
    return {k: serialize_value(v) for k, v in doc.items()}


def flatten_doc(doc):
    """Return a copy of doc with any non-scalar values JSON-encoded."""
    result = {}
    for k, v in doc.items():
        sv = serialize_value(v)
        if isinstance(sv, (dict, list)):
            result[k] = json.dumps(sv)
        else:
            result[k] = sv
    return result


def unflatten_doc(row):
    """
    Try to parse string values that look like JSON objects/arrays back to Python types.
    Used when importing from SQLite to restore nested structures.
    """
    result = {}
    for k, v in row.items():
        if isinstance(v, str) and v and v[0] in ('{', '['):
            try:
                result[k] = json.loads(v)
            except json.JSONDecodeError:
                result[k] = v
        else:
            result[k] = v
    return result
