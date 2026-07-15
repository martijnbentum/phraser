EMPTY_ID= b'\x00\x00\x00\x00\x00\x00\x00\x00'

def ensure_consistent_link(a, b, attr, add_method_name, update_database=True):
    """
    Generic consistency + propagation:
    - If both a and b have attr and mismatch → error
    - If only one has it → propagate to the other via add_method_name
    - If neither has it → no action
    """
    a_val = getattr(a, attr, None)
    b_val = getattr(b, attr, None)
    if a_val == EMPTY_ID  and b_val == EMPTY_ID :
        return
    if a_val == EMPTY_ID: 
        a_val = None
    if b_val == EMPTY_ID:
        b_val = None

    # both set → must match
    if a_val and b_val:
        if a_val != b_val:
            raise ValueError(f"{attr} mismatch: {a_val} vs {b_val}")
        return

    # propagate from a → b
    if a_val and not b_val:
        if not hasattr(b, add_method_name): return
        getattr(b, add_method_name)(
            **{attr: a_val},
            update_database=update_database,
            propagate=True,
        )
        return

    # propagate from b → a
    if b_val and not a_val:
        if not hasattr(a, add_method_name): return
        getattr(a, add_method_name)(
            **{attr: b_val},
            update_database=update_database,
            propagate=True,
        )


def write_changes_to_db(segments, store):
    for segment in segments:
        if segment._save_status == 'save':
            segment.save(overwrite=True)
        elif segment._save_status == 'update':
            store.update(segment._old_key, segment)
        segment._save_status = None
