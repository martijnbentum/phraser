def ensure_consistent_link(a, b, attr, add_method_name, update_database=True):
    """
    Generic consistency + propagation:
    - If both a and b have attr and mismatch → error
    - If only one has it → propagate to the other via add_method_name
    - If neither has it → no action
    """
    a_val = getattr(a, attr, None)
    b_val = getattr(b, attr, None)
    if a_val == 'EMPTY' and b_val == 'EMPTY' :
        return
    if a_val == 'EMPTY': 
        a_val = None
    if b_val == 'EMPTY':
        b_val = None

    # both set → must match
    if a_val and b_val:
        if a_val != b_val:
            raise ValueError(f"{attr} mismatch: {a_val} vs {b_val}")
        return

    # propagate from a → b
    if a_val and not b_val:
        getattr(b, add_method_name)(
            **{attr: a_val},
            reverse_link=False,
            update_database=update_database,
            propagate=True,
        )
        return

    # propagate from b → a
    if b_val and not a_val:
        getattr(a, add_method_name)(
            **{attr: b_val},
            reverse_link=False,
            update_database=update_database,
            propagate=True,
        )

