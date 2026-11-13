'''Saved byte changes, atomic application and optional immediate undo.

Records support inspection and reviewed reconstruction. Interrupted writes
are inspected with check_change_state; they are never retried implicitly.
'''

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import struct
from uuid import uuid4

from ..storage import key_helper, struct_value
from .records import save_record, write_receipt

DATABASES = ('main', 'label_segment', 'speaker_audio')
STATUSES = ('planned', 'applying', 'applied', 'rolling_back', 'rolled_back')
EXECUTION_DEFAULTS = {'applied_txn_id': None, 'verified': False,
    'rollback_transaction_id': None, 'rollback_verified': False, 'error': None,
    'domain_verification': None}


class _ChangeList(list):
    '''Inspection snapshot that rejects accidental list mutation.'''

    def _readonly(self, *args, **kwargs):
        message = 'plan.changes is read-only; use merge_change_plan '
        message += 'or the planning helpers'
        raise TypeError(message)

    append = clear = extend = insert = pop = remove = _readonly
    reverse = sort = __setitem__ = __delitem__ = _readonly
    __iadd__ = __imul__ = _readonly


class ChangePlan:
    '''Original/proposed bytes and captured context for a reviewed repair.'''

    def __init__(self, content):
        vars(self).update(EXECUTION_DEFAULTS)
        vars(self).update({k: v for k, v in content.items() if k != 'changes'})
        self._changes = {}
        self._extend(content['changes'])

    @property
    def changes(self):
        '''Return ordered changes for inspection; extend through helpers.'''
        return _ChangeList(dict(c) for c in self._changes.values())

    def _extend(self, changes):
        '''Validate a complete addition before extending the plan.'''
        pending = {}
        for change in changes:
            identity = (change['database'], change['key'])
            if identity in self._changes or identity in pending:
                raise ValueError(f'Plan already changes {identity}')
            pending[identity] = change
        self._changes.update(pending)

    def _contains(self, database, key):
        return (database, key.hex()) in self._changes

    def __repr__(self):
        return f'<ChangePlan {self.status} changes={len(self._changes)}>'

    def count_changes(self, by='database'):
        '''Count by database, operation or main-row type; indexes separately.'''
        if by not in ('database', 'operation', 'object_type'):
            raise ValueError('Count by database, operation or object_type')
        counts = {}
        for change in self._changes.values():
            value = change.get(by, 'unknown')
            if by == 'object_type':
                value = change['database']
                if value == 'main':
                    value = change['context'].get(by, value)
            counts[value] = counts.get(value, 0) + 1
        return counts

    def summary(self, limit=5):
        '''Print bounded examples; inspect .changes for every affected row.'''
        if type(limit) is not int or limit < 0:
            raise ValueError('limit must be a non-negative integer')
        print(self.reason)
        print(f'status: {self.status}  database: {self.database["path"]}')
        print(f'bytes verified: {self.verified}')
        domain = self.domain_verification or {'status': 'not_run'}
        print(f'domain verification: {domain["status"]}')
        print(f'changes: {len(self.changes):,}  {self.count_changes()}')
        print(f'operations: {self.count_changes("operation")}')
        print(f'row types: {self.count_changes("object_type")}')
        for change in self.changes[:limit]:
            print('  ' + _preview(change))
        if len(self.changes) > limit:
            print('  Inspect plan.changes for the complete affected-row list.')
        if self.error: print(f'error: {self.error}')

    def to_json(self):
        content = {k: v for k, v in vars(self).items() if k != '_changes'}
        content['changes'] = list(self.changes)
        return content


def _preview(change):
    context = change['context']
    text = f'{change["operation"]} {change["database"]} '
    text += context.get('object_type', '')
    if 'audio_id' in context: text += f' audio={context["audio_id"]}'
    if 'start' in context: text += f' start={context["start"]}'
    before, after = context.get('before') or {}, context.get('after') or {}
    fields = context.get('changed_fields', [])
    if fields:
        for field in fields:
            text += f' {field}: {_short_value(before.get(field))}'
            text += f' -> {_short_value(after.get(field))}'
    else:
        row = before or after
        for field in ('label', 'filename', 'end'):
            if field in row: text += f' {field}={_short_value(row[field])}'
    return text + f' key={change["key"]}'


def _short_value(value):
    text = repr(value)
    return text if len(text) <= 60 else text[:57] + '...'


def create_change_plan(store, txn, reason, metadata=None):
    '''Create a proposal using one read transaction for original bytes.
    store:       Store being repaired
    txn:         transaction used by the caller to read the original rows
    reason:      explanation of the proposed repair
    metadata:    optional plain JSON dictionary, such as an audit reference
    '''
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError('A change plan needs a reason')
    if metadata is None: metadata = {}
    if not isinstance(metadata, dict):
        raise ValueError('metadata must be a JSON dictionary')
    metadata = json.loads(json.dumps(metadata, allow_nan=False))
    store._ensure_open()
    return ChangePlan({'version': 1, 'kind': 'byte_changes', 'id': uuid4().hex,
        'reason': reason, 'created_utc': datetime.now(timezone.utc).isoformat(),
        'database': _database_identity(store), 'transaction_id': txn.id(),
        'metadata': metadata, 'status': 'planned', 'changes': []})


def add_change(plan, database, key, before, after):
    '''Record one row, including original/proposed bytes and readable context.
    plan:        proposal created by create_change_plan
    database:    main, label_segment or speaker_audio
    key:         persisted bytes key
    before:      original bytes, or None if absent
    after:       proposed bytes, or None to delete
    '''
    if plan.status != 'planned':
        raise ValueError('Only planned changes can be extended')
    if database not in DATABASES: raise ValueError('Unsupported database')
    if not isinstance(key, bytes) or not key:
        raise ValueError('key must be nonempty bytes')
    for value in (before, after):
        if value is not None and not isinstance(value, bytes):
            raise ValueError('Row values must be bytes or None')
    if before == after: return
    operation = 'update'
    if before is None: operation = 'insert'
    elif after is None: operation = 'delete'
    plan._extend([{'database': database, 'key': key.hex(),
        'before': _hex_value(before), 'after': _hex_value(after),
        'operation': operation, 'context': _row_context(database, key,
            before, after)}])


def merge_change_plan(plan, other):
    '''Extend a planned repair with another plan from the same snapshot.
    plan:     destination; its reason and metadata remain in effect
    other:    source plan, left unchanged; duplicate keys are refused

    The whole addition is validated before mutation. Duration assessment
    plans must be applied separately to preserve their assessment policy.
    '''
    if plan.status != 'planned' or other.status != 'planned':
        raise ValueError('Only planned changes can be merged')
    if plan.database != other.database:
        raise ValueError('Cannot merge plans from different stores')
    if plan.transaction_id != other.transaction_id:
        raise ValueError('Cannot merge plans from different transactions')
    assessments = 'duration_assessment' in plan.metadata
    assessments = assessments or 'duration_assessment' in other.metadata
    if assessments:
        raise ValueError('Duration assessment plans must be applied separately')
    _validate_plan(other.to_json())
    plan._extend(deepcopy(list(other.changes)))


def _row_context(database, key, before, after):
    '''Decode saved bytes where possible; raw byte repairs are also allowed.'''
    target = key
    if database == 'label_segment':
        target = key_helper.label_index_key_to_instance_key(key)
    try: context = key_helper.unpack_key(target)
    except (ValueError, KeyError, struct.error): return {'key': key.hex()}
    context = _json_values(context)
    if database != 'main': return context
    kind = context.get('object_type')
    original = _decoded_value(kind, before)
    proposed = _decoded_value(kind, after)
    context['before'] = original
    context['after'] = proposed
    if original is not None and proposed is not None:
        fields = []
        for field in original.keys() | proposed.keys():
            if original.get(field) != proposed.get(field): fields.append(field)
        context['changed_fields'] = sorted(fields)
    return context


def _decoded_value(kind, value):
    if value is None: return None
    try: return _json_values(struct_value.unpack_instance(kind, value))
    except (AttributeError, ValueError, KeyError, struct.error): return None


def _json_values(values):
    result = {}
    for key, value in values.items():
        result[key] = value.hex() if isinstance(value, bytes) else value
    return result


def plan_audio_ids(plan):
    '''Recording IDs from changed keys; speaker-only edits need a full audit.'''
    audio_ids = set()
    for change in plan.changes:
        key = bytes.fromhex(change['key'])
        if change['database'] == 'label_segment':
            key = key_helper.label_index_key_to_instance_key(key)
        info = key_helper.unpack_key(key)
        if info.get('object_type') == 'Audio':
            audio_ids.add(info['identifier'])
        elif 'audio_id' in info: audio_ids.add(info['audio_id'])
    return sorted(audio_ids)


def save_change_plan(plan, directory=None, name='repair', *, path=None):
    '''Save a new numbered record beside an audit, or at an explicit path.
    plan:         ordinary byte-change proposal
    directory:    audit report folder, outside the LMDB directory
    name:         short filename label, used with directory
    path:         explicit destination instead of directory; never overwritten
    '''
    _validate_plan(plan.to_json())
    if plan.status != 'planned':
        raise ValueError('Only planned changes can be saved as new records')
    return save_record(plan.to_json(), directory, name, path=path)


def load_change_plan(path):
    '''Read and validate an ordinary byte plan; bulk receipts are refused.'''
    with open(path, encoding='utf-8') as stream:
        content = json.load(stream)
    _validate_plan(content)
    return ChangePlan(content)


def _validate_plan(content):
    '''Reject malformed executable records before touching the database.'''
    try:
        if type(content['version']) is not int or content['version'] != 1:
            raise ValueError('Expected version 1 byte_changes record')
        if content['kind'] != 'byte_changes':
            raise ValueError('Expected version 1 byte_changes record')
        if content['status'] not in STATUSES:
            raise ValueError('Unknown change status')
        reason = content['reason']
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError('A change plan needs a reason')
        if not isinstance(content['metadata'], dict):
            raise ValueError('metadata must be a dictionary')
        database = content['database']
        if not isinstance(database['path'], str):
            raise ValueError('database.path must be a string')
        for field in ('device', 'inode'):
            if type(database[field]) is not int:
                raise ValueError(f'database.{field} must be an integer')
        _validate_transaction_fields(content)
        _validate_changes(content['changes'])
        json.dumps(content, allow_nan=False)
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError(f'Malformed change record: {error}') from error


def _validate_transaction_fields(content):
    fields = ('transaction_id', 'applied_txn_id', 'rollback_transaction_id')
    for field in fields:
        value = content.get(field)
        if value is None and field != 'transaction_id': continue
        if type(value) is not int or value < 0:
            raise ValueError(f'{field} must be a non-negative integer')
    if content['status'] == 'applied' and content.get('applied_txn_id') is None:
        raise ValueError('Applied record is missing applied_txn_id')


def _validate_changes(changes):
    if not isinstance(changes, list): raise ValueError('changes must be a list')
    seen = set()
    for change in changes:
        name = change['database']
        if name not in DATABASES:
            raise ValueError(f'Unsupported database: {name}')
        key = bytes.fromhex(change['key'])
        if not key: raise ValueError('Empty key in change plan')
        identity = (name, key)
        if identity in seen: raise ValueError('Duplicate key in change plan')
        seen.add(identity)
        _value(change, 'before')
        _value(change, 'after')
        if not isinstance(change['context'], dict):
            raise ValueError('Change context must be a dictionary')
        if change['operation'] not in ('insert', 'update', 'delete'):
            raise ValueError('Unknown change operation')


def apply_change_plan(store, path, *, retry_original=False):
    '''Apply reviewed original/proposed bytes in one transaction.
    store:             Store matching the record's database identity
    path:              saved proposal, updated with its execution receipt
    retry_original:    explicitly retry an interrupted applying record;
                       every row must still match its original bytes

    Refuses stale bytes and completed records. Interrupted records require
    inspection with check_change_state and explicit retry_original=True.
    '''
    plan = load_change_plan(path)
    retry = retry_original and plan.status == 'applying'
    if plan.status != 'planned' and not retry:
        raise ValueError('Only planned changes can be applied; inspect state')
    _check_database(store, plan)
    if retry:
        plan.applied_txn_id = None
        plan.verified = False
    return _run_change(store, path, plan, reverse=False)


def rollback_changes(store, path):
    '''Attempt immediate undo; later writes require reviewed reconstruction.
    store:    Store matching the record's database identity
    path:     completed ordinary repair; no later database writes allowed
    '''
    plan = load_change_plan(path)
    if plan.status != 'applied':
        raise ValueError('Only applied changes can be undone; inspect state')
    _check_database(store, plan)
    return _run_change(store, path, plan, reverse=True)


def _run_change(store, path, plan, reverse):
    previous = plan.status
    plan.status = 'rolling_back' if reverse else 'applying'
    if not reverse: plan.domain_verification = None
    field = 'rollback_transaction_id' if reverse else 'applied_txn_id'
    write_receipt(path, plan.to_json())
    try: _execute(store, plan, reverse)
    except Exception as error:
        if getattr(plan, field) is None: plan.status = previous
        plan.error = str(error)
        write_receipt(path, plan.to_json())
        raise
    plan.status = 'rolled_back' if reverse else 'applied'
    plan.error = None
    write_receipt(path, plan.to_json())
    return plan


def _execute(store, plan, reverse):
    before, after = ('after', 'before') if reverse else ('before', 'after')
    committed = False
    reuse_keys = False
    field = 'rollback_transaction_id' if reverse else 'applied_txn_id'
    try:
        with store.DB.env.begin(write=True) as txn:
            reuse_keys = _can_reuse_query_keys(store, plan, txn.id() - 1)
            if reverse and txn.id() - 1 != plan.applied_txn_id:
                raise ValueError('Database changed; immediate undo is stale')
            if not reverse and 'duration_assessment' in plan.metadata:
                if txn.id() - 1 != plan.transaction_id:
                    message = 'Store changed since duration assessment; '
                    message += 'prepare a fresh assessment plan'
                    raise ValueError(message)
            _check_values(store, txn, plan, before)
            for change in plan.changes:
                _write_change(store, txn, change, after)
            _check_values(store, txn, plan, after)
            committed_id = txn.id() if plan.changes else txn.id() - 1
        committed = True
        setattr(plan, field, committed_id)
        with store.DB.env.begin() as txn:
            _check_values(store, txn, plan, after)
        verified = 'rollback_verified' if reverse else 'verified'
        setattr(plan, verified, True)
    finally:
        if committed: _refresh_store(store, reuse_keys)


def check_change_state(store, path):
    '''Describe current bytes without changing the Store or saved receipt.
    store:    Store matching the saved database identity
    path:     ordinary change record, including interrupted operations

    state is original, proposed, mixed, conflicting or both (empty/no-op).
    This describes current values, not proof of the operation's history.
    '''
    plan = load_change_plan(path)
    _check_database(store, plan)
    counts = dict(original=0, proposed=0, both=0, conflicting=0)
    with store.DB.env.begin() as txn:
        for change in plan.changes:
            key = bytes.fromhex(change['key'])
            current = txn.get(key, db=store.DB.db[change['database']])
            original = current == _value(change, 'before')
            proposed = current == _value(change, 'after')
            if original and proposed: state = 'both'
            elif original: state = 'original'
            elif proposed: state = 'proposed'
            else: state = 'conflicting'
            counts[state] += 1
        transaction_id = txn.id()
    if counts['conflicting']: state = 'conflicting'
    elif counts['original'] and counts['proposed']: state = 'mixed'
    elif counts['original']: state = 'original'
    elif counts['proposed']: state = 'proposed'
    else: state = 'both'
    return {'state': state, 'counts': counts, 'transaction_id': transaction_id,
        'recorded_status': plan.status}


def _hex_value(value):
    return None if value is None else value.hex()


def _value(change, field):
    value = change[field]
    return None if value is None else bytes.fromhex(value)


def _database_identity(store):
    path = Path(store.path).resolve()
    stat = (path / 'data.mdb').stat()
    return {'path': str(path), 'device': stat.st_dev, 'inode': stat.st_ino}


def _check_database(store, plan):
    store._ensure_open()
    if _database_identity(store) != plan.database:
        raise ValueError('Changes report belongs to a different database')


def _check_values(store, txn, plan, field):
    for change in plan.changes:
        key = bytes.fromhex(change['key'])
        current = txn.get(key, db=store.DB.db[change['database']])
        if current != _value(change, field):
            raise ValueError(f'Current bytes conflict for {key.hex()}')


def _write_change(store, txn, change, field):
    key = bytes.fromhex(change['key'])
    value = _value(change, field)
    database = store.DB.db[change['database']]
    if value is None: txn.delete(key, db=database)
    else: txn.put(key, value, db=database)


def _can_reuse_query_keys(store, plan, transaction_id):
    '''Reuse current membership only for repairs that keep every main key.'''
    for change in plan.changes:
        if change['database'] != 'main': continue
        if change['before'] is None or change['after'] is None: return False
    if not hasattr(store, '_rank_to_keys_dict'):
        return not store.eager_query_roots
    return store._rank_to_keys_transaction_id == transaction_id


def _refresh_store(store, reuse_keys):
    store._cache.clear()
    store._classes_loaded.clear()
    if not reuse_keys:
        store.refresh_query_roots()
        return
    if hasattr(store, '_rank_to_keys_dict'):
        store._rank_to_keys_transaction_id = store.DB.env.info()['last_txnid']
    store.attach_query_roots()
