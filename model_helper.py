EMPTY_ID= '0000000000000000'

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
            reverse_link=False,
            update_database=update_database,
            propagate=True,
        )
        return

    # propagate from b → a
    if b_val and not a_val:
        if not hasattr(a, add_method_name): return
        getattr(a, add_method_name)(
            **{attr: b_val},
            reverse_link=False,
            update_database=update_database,
            propagate=True,
        )


def fix_references(segment, old_key, new_key):
    refs_changed = False

    # parent
    parent = getattr(segment, "parent", None)
        
    if parent is not None and hasattr(parent, "child_keys"):
        new_list = [
            new_key if ck == old_key else ck
            for ck in parent.child_keys
        ]
        if new_list != parent.child_keys:
            parent.child_keys = new_list
            refs_changed = True
            if parent._save_status is None:
                parent._save_status = "save"

    # children
    for child in getattr(segment, "children", []):
        if getattr(child, "parent_key", None) == old_key:
            child.parent_key = new_key
            refs_changed = True
            if child._save_status is None:
                child._save_status = "save"


def write_changes_to_db(segments, cache):
    for segment in segments:
        if segment._save_status == 'save':
            segment.save(overwrite=True)
        elif segment._save_status == 'update':
            cache.update(segment._old_key, segment)
        segment._save_status = None
