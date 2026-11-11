'''Durable JSON records beside an audit, independent of repair execution.'''

import json
import os
from pathlib import Path
import re
import tempfile


def save_record(content, directory=None, name='repair', *, path=None):
    '''Save a new record without overwriting another; return its path.
    content:      plain JSON record with version, kind and database path
    directory:    audit folder for a numbered change_<number>_<name>.json
    name:         short filename component containing letters, digits, _ or -
    path:         explicit output filename, instead of directory
    '''
    if (directory is None) == (path is None):
        raise ValueError('Supply either directory or path')
    if path is None:
        path = _numbered_path(directory, name)
    path = Path(path).absolute()
    payload = _record_payload(content, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(path, payload, replace=False)
    return path


def write_receipt(path, content):
    '''Atomically replace an existing record and flush its file and directory.
    path:       existing record outside the LMDB directory
    content:    full replacement record, including execution progress
    '''
    path = Path(path).absolute()
    payload = _record_payload(content, path)
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


def _record_payload(content, path):
    '''Validate the shared envelope and location before writing anything.'''
    if not isinstance(content, dict):
        raise ValueError('Record must be a dictionary')
    if type(content.get('version')) is not int or content['version'] != 1:
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
        if replace: os.replace(temporary, path)
        else: os.link(temporary, path)
        if os.path.exists(temporary): os.unlink(temporary)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
