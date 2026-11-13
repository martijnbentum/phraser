'''Plan routine segment edits and combined tree deletions from stored rows.'''

from copy import copy

from ..models import Phone, Phrase, Syllable, Word
from ..storage import key_helper, struct_value
from ..storage.store import value_key_to_instance
from .changes import add_change

SEGMENT_MODELS = {cls.__name__: cls for cls in (Phrase, Word, Syllable, Phone)}


def plan_delete(plan, store, txn, keys):
    '''Add trees and their index/link deletions, leaving the Store unchanged.
    plan:     change plan to extend only after every helper check succeeds
    store:    open Store whose rows are read through txn
    txn:      read transaction used to inspect the current rows
    keys:     one segment key or an iterable of roots; descendants included

    Overlapping roots are collected once. Parent links find descendants,
    independent of timing. Retained parent/phrase references and markers in
    the affected recordings block deletion. All label-index entries pointing
    to deleted rows are removed, including stale ones. Submit roots together
    to scan each affected recording and the label index only once per call.
    '''
    staged = _staged_plan(plan)
    if isinstance(keys, bytes): keys = [keys]
    roots = [_load_segment(store, txn, key) for key in dict.fromkeys(keys)]
    groups = {}
    for row in roots: groups.setdefault(row['audio_id'], []).append(row)
    deleted = _planned_deletions(plan)
    changed = _planned_keys(plan)
    existing = {}
    for change in plan.changes:
        existing[(change['database'], change['key'])] = change
    selected = {}
    for audio_id, group in groups.items():
        rows = list(_audio_rows(store, txn, audio_id))
        chosen = _select_trees(group, rows)
        _check_references(rows, chosen, deleted)
        _add_main_deletions(staged, chosen, deleted, changed)
        deleted.update(chosen)
        selected.update(chosen)
        _delete_unused_links(staged, store, txn, rows, chosen, deleted,
            existing)
    _delete_label_entries(staged, store, txn, selected, existing)
    _commit_staged(plan, staged)


def plan_update(plan, store, txn, key, **fields):
    '''Add an in-place segment edit and maintain a changed label's index.
    plan:      change plan; left unchanged if this helper refuses the edit
    store:     open Store whose current rows are inspected
    txn:       read transaction used to inspect the current row
    key:       segment key, which remains unchanged
    fields:    stored non-relationship fields, such as label, end or flags

    Refuses identity/relationship fields and version. An explicit end must
    exceed start; existing bad durations do not prevent unrelated edits.
    Children are not checked against a new end; verify and re-audit afterward.
    '''
    staged = _staged_plan(plan)
    if plan._contains('main', key):
        raise ValueError(f'Plan already changes {key.hex()}')
    row = _load_segment(store, txn, key)
    _check_fields(row, fields)
    segment = value_key_to_instance(store, row['raw'], key)
    for name, value in fields.items(): setattr(segment, name, value)
    add_change(staged, 'main', key, row['raw'],
        struct_value.pack_instance(segment))
    if segment.label != row['label']:
        _move_label_entry(staged, store, txn, row, segment.label)
    _commit_staged(plan, staged)


def _staged_plan(plan):
    if plan.status != 'planned':
        raise ValueError('Only planned changes can be extended')
    staged = copy(plan)
    staged._changes = {}
    return staged


def _commit_staged(plan, staged):
    plan._extend(staged.changes)


def _planned_keys(plan):
    keys = set()
    for change in plan.changes:
        if change['database'] == 'main': keys.add(bytes.fromhex(change['key']))
    return keys


def _planned_deletions(plan):
    keys = set()
    for change in plan.changes:
        if change['database'] == 'main' and change['after'] is None:
            keys.add(bytes.fromhex(change['key']))
    return keys


def _check_fields(row, fields):
    stored = set(struct_value.unpack_instance(row['object_type'], row['raw']))
    protected = {'version', 'start', 'identifier', 'audio_id', 'speaker_id',
        'parent_id', 'parent_start', 'phrase_id', 'phrase_start'}
    unsupported = sorted(set(fields) - (stored - protected))
    if unsupported:
        message = f"{row['object_type']} cannot update {unsupported}; "
        message += 'use a separately reviewed byte-level repair'
        raise ValueError(message)
    if 'end' in fields and fields['end'] <= row['start']:
        raise ValueError('end must be greater than start')


def _load_segment(store, txn, key):
    raw = txn.get(key, db=store.DB.db['main'])
    if raw is None: raise ValueError(f'No row for key {key.hex()}')
    row = _decode_row(key, raw)
    if row['object_type'] not in SEGMENT_MODELS:
        raise ValueError(f"Not a segment: {row['object_type']}")
    return row


def _decode_row(key, raw):
    info = key_helper.unpack_key(key)
    row = struct_value.unpack_instance(info['object_type'], raw)
    row.update(info)
    row.update(key=key, raw=raw)
    return row


def _audio_rows(store, txn, audio_id):
    '''Decode the segment and marker rows of one recording once.'''
    prefix = key_helper.audio_id_to_key(audio_id)[:9]
    with txn.cursor(db=store.DB.db['main']) as cursor:
        if not cursor.set_range(prefix): return
        for key, raw in cursor:
            if not key.startswith(prefix): break
            if len(key) == key_helper.AUDIO_LEN: continue
            yield _decode_row(key, raw)


def _select_trees(roots, rows):
    children = {}
    for row in rows:
        if 'parent_id' not in row: continue
        children.setdefault(row['parent_id'], []).append(row)
    selected = {}
    pending = list(roots)
    while pending:
        row = pending.pop()
        if row['key'] in selected: continue
        selected[row['key']] = row
        model = SEGMENT_MODELS[row['object_type']]
        if model.child_class is None: continue
        for child in children.get(row['identifier'], []):
            if child['object_type'] == model.child_class.__name__:
                pending.append(child)
    return selected


def _check_references(rows, selected, deleted):
    identifiers = {row['identifier'] for row in selected.values()}
    for row in rows:
        if row['key'] in selected or row['key'] in deleted: continue
        references = (row.get('parent_id'), row.get('phrase_id'))
        if not any(identifier in identifiers for identifier in references):
            continue
        message = f"Retained {row['object_type']} {row['key'].hex()} "
        message += 'references a selected tree'
        raise ValueError(message)


def _add_main_deletions(plan, selected, deleted, changed):
    for key, row in selected.items():
        if key in deleted: continue
        if key in changed: raise ValueError(f'Plan already changes {key.hex()}')
        add_change(plan, 'main', key, row['raw'], None)
        changed.add(key)


def _delete_unused_links(plan, store, txn, rows, selected, deleted, existing):
    removed, retained = set(), set()
    for row in rows:
        if row['object_type'] != 'Phrase': continue
        link = key_helper.pack_speaker_audio_key(row['speaker_id'],
            row['audio_id'])
        if row['key'] in selected: removed.add(link)
        if row['key'] not in deleted: retained.add(link)
    for key in sorted(removed - retained):
        value = txn.get(key, db=store.DB.db['speaker_audio'])
        if value is not None:
            _add_deletion(plan, 'speaker_audio', key, value, existing)


def _delete_label_entries(plan, store, txn, selected, existing):
    if not selected: return
    with txn.cursor(db=store.DB.db['label_segment']) as cursor:
        for key, value in cursor:
            target = key_helper.label_index_key_to_instance_key(key)
            if target in selected:
                _add_deletion(plan, 'label_segment', key, value, existing)


def _add_deletion(plan, database, key, value, existing):
    change = existing.get((database, key.hex()))
    if change is not None:
        if change['after'] is None: return
        raise ValueError(f'Plan already changes {database} {key.hex()}')
    add_change(plan, database, key, value, None)


def _move_label_entry(plan, store, txn, row, label):
    database = store.DB.db['label_segment']
    old = key_helper.label_to_label_index_key(row['label'],
        row['object_type'], row['key'])
    value = txn.get(old, db=database)
    if value is None: value = b''
    else: add_change(plan, 'label_segment', old, value, None)
    new = key_helper.label_to_label_index_key(label, row['object_type'],
        row['key'])
    add_change(plan, 'label_segment', new, txn.get(new, db=database), value)
