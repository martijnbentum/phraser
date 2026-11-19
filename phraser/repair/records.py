'''Small durable JSON receipts and immutable compressed row evidence.'''

import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import tempfile

DATABASES = ('main', 'label_segment', 'speaker_audio')
PAYLOAD_MAGIC = b'PHRASER-CHANGES-2\n'
PAYLOAD_FRAME = struct.Struct('>BIII')
ABSENT = 0xffffffff


def save_record(content, directory=None, name='repair', *, path=None):
    '''Save a new record without overwriting another; return its path.
    content:      plain JSON record with version, kind and database path
    directory:    audit folder for a numbered change_<number>_<name>.json
    name:         short filename component containing letters, digits, _ or -
    path:         explicit output filename, instead of directory
    '''
    path = record_path(directory, name, path)
    payload = record_json(content, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(path, payload, replace=False)
    return path


def record_path(directory, name, path):
    if (directory is None) == (path is None):
        raise ValueError('Supply either directory or path')
    if path is None: path = _numbered_path(directory, name)
    return Path(path).absolute()


def write_receipt(path, content):
    '''Atomically replace an existing record and flush its file and directory.
    path:       existing record outside the LMDB directory
    content:    full replacement record, including execution progress
    '''
    path = Path(path).absolute()
    payload = record_json(content, path)
    if not path.is_file(): raise FileNotFoundError(path)
    _write_json(path, payload, replace=True)


def _numbered_path(directory, name):
    if not isinstance(name, str) or not re.fullmatch(r'[\w-]+', name):
        raise ValueError('name must contain only letters, digits, _ or -')
    directory = Path(directory).absolute()
    number = 0
    for path in directory.glob('change_*'):
        match = re.match(r'^change_(\d+)_', path.name)
        if match: number = max(number, int(match[1]))
    return directory / f'change_{number + 1:03d}_{name}.json'


def record_json(content, path):
    '''Validate the shared envelope and location before writing anything.'''
    if not isinstance(content, dict):
        raise ValueError('Record must be a dictionary')
    versions = (1, 2) if content.get('kind') == 'byte_changes' else (1,)
    if type(content.get('version')) is not int:
        raise ValueError('Unsupported or missing record version')
    if content['version'] not in versions:
        raise ValueError('Unsupported or missing record version')
    if content.get('kind') not in ('byte_changes', 'bulk'):
        raise ValueError('Unsupported or missing record kind')
    database = content.get('database')
    if not isinstance(database, dict):
        raise ValueError('Record needs a database path')
    root = database.get('path')
    if not isinstance(root, str) or not root:
        raise ValueError('Record needs a database path')
    if path.resolve().is_relative_to(Path(root).resolve()):
        raise ValueError('Changes reports must be outside the LMDB directory')
    return json.dumps(content, indent=2, allow_nan=False) + '\n'


def _write_json(path, payload, replace):
    '''Publish a flushed temporary file, exclusively for a new record.'''
    descriptor, temporary = tempfile.mkstemp(dir=path.parent,
        prefix='.changes_')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        _publish_file(temporary, path, replace)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)


def _publish_file(temporary, path, replace=False):
    '''Publish flushed evidence and make the directory entry durable.'''
    if replace: os.replace(temporary, path)
    else: os.link(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


def _file_checksum(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write_change_payload(path, changes):
    '''Write immutable compressed key/before/after frames, without context.'''
    if path.exists(): raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent,
        prefix='.changes_')
    count = 0
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            with gzip.GzipFile(filename='', fileobj=stream, mode='wb',
                compresslevel=1, mtime=0) as packed:
                packed.write(PAYLOAD_MAGIC)
                for change in changes:
                    _write_payload_row(packed, change)
                    count += 1
            stream.flush()
            os.fsync(stream.fileno())
        _publish_file(temporary, path)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
    return {'file': path.name, 'sha256': _file_checksum(path), 'count': count}


def _write_payload_row(stream, change):
    key = bytes.fromhex(change['key'])
    values, sizes = [], []
    for field in ('before', 'after'):
        value = change[field]
        value = None if value is None else bytes.fromhex(value)
        if value is not None and len(value) >= ABSENT:
            raise ValueError('Change payload value exceeds frame size')
        values.append(value)
        sizes.append(ABSENT if value is None else len(value))
    rank = DATABASES.index(change['database'])
    stream.write(PAYLOAD_FRAME.pack(rank, len(key), *sizes))
    stream.write(key)
    for value in values:
        if value is not None: stream.write(value)


def read_change_payload(directory, payload):
    '''Check the payload checksum, then yield original/proposed byte rows.'''
    name = payload['file']
    if name in ('', '.', '..') or Path(name).name != name:
        raise ValueError('Payload must be a filename beside its receipt')
    path = directory / name
    if _file_checksum(path) != payload['sha256']:
        raise ValueError('Change payload checksum mismatch')
    count = 0
    try:
        with gzip.open(path, 'rb') as stream:
            if stream.read(len(PAYLOAD_MAGIC)) != PAYLOAD_MAGIC:
                raise ValueError('Invalid change payload header')
            while True:
                frame = stream.read(PAYLOAD_FRAME.size)
                if not frame: break
                count += 1
                if count > payload['count']:
                    raise ValueError('Change payload count mismatch')
                yield _read_payload_row(stream, frame)
    except (EOFError, OSError, struct.error, IndexError) as error:
        raise ValueError('Invalid change payload') from error
    if count != payload['count']:
        raise ValueError('Change payload count mismatch')


def _read_payload_row(stream, frame):
    rank, key_size, before_size, after_size = PAYLOAD_FRAME.unpack(frame)
    key = _read_exact(stream, key_size)
    before = _read_payload_value(stream, before_size)
    after = _read_payload_value(stream, after_size)
    operation = 'insert' if before is None else 'update'
    if after is None: operation = 'delete'
    return {'database': DATABASES[rank], 'key': key.hex(),
        'before': None if before is None else before.hex(),
        'after': None if after is None else after.hex(), 'operation': operation}


def _read_payload_value(stream, size):
    return None if size == ABSENT else _read_exact(stream, size)


def _read_exact(stream, size):
    value = stream.read(size)
    if len(value) != size: raise ValueError('Truncated change payload')
    return value
