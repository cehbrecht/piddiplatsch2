def drop_empty(d):
    """Recursively clean empty fields"""
    result = {}
    for k, v in d.items():
        if v in ("", [], {}, None):
            continue
        if isinstance(v, dict):
            nested = drop_empty(v)
            if nested:
                result[k] = nested
        else:
            result[k] = v
    return result
