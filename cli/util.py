def ref_name(obj):
    """Extract schema name from a $ref value."""
    return obj["$ref"].split("/")[-1]
