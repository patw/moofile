"""Filter operator evaluation ($gt, $lt, $in, etc.)."""


def apply_op(op: str, field_value, op_value) -> bool:
    """
    Evaluate a single filter operator against a field value.
    Raises ValueError for unknown operators.
    """
    if op == "$eq":
        return field_value == op_value
    if op == "$ne":
        return field_value != op_value
    if op == "$gt":
        try:
            return field_value > op_value
        except TypeError:
            return False
    if op == "$gte":
        try:
            return field_value >= op_value
        except TypeError:
            return False
    if op == "$lt":
        try:
            return field_value < op_value
        except TypeError:
            return False
    if op == "$lte":
        try:
            return field_value <= op_value
        except TypeError:
            return False
    if op == "$in":
        return field_value in op_value
    if op == "$nin":
        return field_value not in op_value
    raise ValueError(f"Unknown operator: {op!r}")
