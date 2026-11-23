'''Copy recording materials or whole stores, and reclaim unused pages.'''

from itertools import zip_longest
from pathlib import Path
import shutil
from stat import S_IMODE
import tempfile
from time import monotonic

import lmdb
from progressbar import progressbar

from .storage import key_helper
from .storage.store import Store, value_key_to_instance


def copy_store(source_store, destination, compact=True):
    '''Copy all databases, verify their bytes, and return an open Store.
    source_store: open source Store, left open and unchanged
    destination:  new destination directory
    compact:      omit unused LMDB pages while copying
    '''
    source_store._ensure_open()
    path = Path(destination)
    _check_destination(source_store, path)
    source_path = Path(source_store.path)
    file_mode = S_IMODE((source_path / 'data.mdb').stat().st_mode)
    directory_mode = S_IMODE(source_path.stat().st_mode)
    path.mkdir(mode=0o700)
    try:
        print(f'Copying whole store to {path}...', flush=True)
        _copy_verified(source_store, path, compact)
        (path / 'data.mdb').chmod(file_mode)
        path.chmod(directory_mode)
        return Store(path=path)
    except BaseException:
        shutil.rmtree(path)
        raise


def compact(source_store, keep_backup=False):
    '''Replace a store with a verified compact copy and reopen it.
    source_store: open Store; caller must prevent other access during work
    keep_backup:  retain the original data.mdb after successful reopening

    Return before/after file and allocated sizes, seconds, and backup path.
    Other handles must be closed. No process may write during this operation.
    '''
    source_store._ensure_open()
    print('WARNING: No writes may happen to this store during compaction. '
        'Close all other store handles before continuing.', flush=True)
    started = monotonic()
    root = Path(source_store.path).resolve()
    before = _file_sizes(root / 'data.mdb')
    mode = S_IMODE((root / 'data.mdb').stat().st_mode)
    temporary = Path(tempfile.mkdtemp(prefix=f'.{root.name}-compact-',
        dir=root.parent))
    backup = None
    try:
        transaction_id = _copy_verified(source_store, temporary, True)
        (temporary / 'data.mdb').chmod(mode)
        backup = Path(tempfile.mkdtemp(prefix=f'.{root.name}-backup-',
            dir=root.parent))
        _check_transaction(source_store, transaction_id)
        _replace_store_data(source_store, root, temporary, backup)
        after = _file_sizes(root / 'data.mdb')
        if not keep_backup:
            shutil.rmtree(backup)
            backup = None
        return {'before': before, 'after': after,
            'elapsed_seconds': monotonic() - started,
            'backup_path': str(backup) if backup is not None else None}
    finally:
        shutil.rmtree(temporary)
        if backup is not None and backup.exists():
            if not any(backup.iterdir()): backup.rmdir()


def _check_destination(source, destination):
    '''Reject existing destinations and paths inside the source store.'''
    root = Path(source.path).resolve()
    path = destination.resolve()
    if path == root or path.is_relative_to(root):
        raise ValueError('destination must be outside the source store')
    if destination.exists():
        raise FileExistsError(f'destination already exists: {destination}')


def _copy_verified(source, destination, compact):
    '''Copy a stable source and compare every named database byte for byte.'''
    transaction_id = source.DB.env.info()['last_txnid']
    source.DB.env.copy(str(destination), compact=compact)
    print('Verifying copied database keys and values...', flush=True)
    _verify_copy(source, destination)
    _check_transaction(source, transaction_id)
    return transaction_id


def _check_transaction(source, transaction_id):
    if source.DB.env.info()['last_txnid'] != transaction_id:
        raise RuntimeError('source changed during copying; replacement aborted')


def _verify_copy(source, destination):
    '''Stream comparisons of all named databases, including indexes.'''
    with source.DB.env.begin() as original:
        names = list(original.cursor().iternext(values=False))
        copied = lmdb.open(str(destination), readonly=True, lock=False,
            max_dbs=len(names))
        try:
            with copied.begin() as duplicate:
                other_names = list(duplicate.cursor().iternext(values=False))
                if names != other_names:
                    raise ValueError('copied database names differ')
                for name in names:
                    _verify_database(source.DB.env, copied, original,
                        duplicate, name)
        finally:
            copied.close()


def _verify_database(source, copied, original, duplicate, name):
    '''Compare ordered key/value pairs without loading a whole database.'''
    source_db = source.open_db(name, txn=original, create=False)
    copied_db = copied.open_db(name, txn=duplicate, create=False)
    with original.cursor(source_db) as left:
        with duplicate.cursor(copied_db) as right:
            for before, after in zip_longest(left, right):
                if before != after:
                    raise ValueError(f'copied database differs: {name!r}')


def _replace_store_data(store, root, temporary, backup):
    '''Install verified data, restoring the original on reopening failure.'''
    print('Replacing data and reopening store...', flush=True)
    original = backup / 'data.mdb'
    try:
        store.close()
        (root / 'data.mdb').replace(original)
        (temporary / 'data.mdb').replace(root / 'data.mdb')
        store.open()
        store.refresh_query_roots()
    except BaseException:
        try:
            store.close()
            if original.exists(): original.replace(root / 'data.mdb')
            store.open()
            store.refresh_query_roots()
        except BaseException as error:
            message = f'compaction recovery failed; inspect {root} '
            message += f'and original backup {backup}'
            raise RuntimeError(message) from error
        raise


def _file_sizes(path):
    stat = path.stat()
    return {'file_bytes': stat.st_size, 'allocated_bytes': stat.st_blocks * 512}


def copy_audio_materials(audio_keys, goal_store_root, source_store):
    '''Copy recordings and their annotations; return an open goal Store.
    audio_keys:      source Audio keys; repeated keys are copied once
    goal_store_root: new or empty directory for the destination store
    source_store:    open source Store, left open and unchanged

    Preserve persisted keys, metadata, and audio filenames. Include linked
    speakers and all phrases, words, syllables, and phones; exclude markers.
    Copy existing speaker-audio links for the selected recordings only.
    Audio files are not copied. The caller closes the returned store.
    '''
    source_store._ensure_open()
    root = Path(goal_store_root)
    if root.resolve() == Path(source_store.path).resolve():
        raise ValueError('goal store must differ from source store')
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise FileExistsError('goal store root must be new or empty')
    keys = list(dict.fromkeys(audio_keys))
    started = monotonic()
    print(f'Checking {len(keys):,} source Audio keys...', flush=True)
    _check_audio_keys(keys, source_store)
    print(f'Creating destination store: {root}', flush=True)
    goal = Store(path=root)
    copied_speakers = set()
    try:
        print('Copying recording hierarchies...', flush=True)
        for key in progressbar(keys):
            _copy_audio(key, source_store, goal, copied_speakers)
            goal._cache.clear()
        print('Refreshing destination query roots...', flush=True)
        goal.refresh_query_roots()
    except Exception:
        goal.close()
        raise
    elapsed = monotonic() - started
    n_objects = len(goal.save_key_counter)
    print(f'Copied {len(keys):,} recordings; {n_objects:,} objects including '
        f'{len(copied_speakers):,} speakers in {elapsed:.1f}s.', flush=True)
    return goal


def _check_audio_keys(keys, source):
    '''Check supplied identities before creating the destination.'''
    for key in keys:
        if not isinstance(key, bytes) or len(key) != key_helper.AUDIO_LEN:
            raise ValueError('audio_keys must contain Audio byte keys')
        if key_helper.key_to_object_type(key) != 'Audio':
            raise ValueError('audio_keys must contain Audio byte keys')
        if source.DB.load(key) is None:
            raise KeyError(f'Audio key not found: {key.hex()}')


def _copy_audio(audio_key, source, goal, copied_speakers):
    '''Copy all annotation rows for an audio and its linked speakers.'''
    audio_id = key_helper.key_to_audio_identifier(audio_key)
    keys = [audio_key]
    for class_name in ('Phrase', 'Word', 'Syllable', 'Phone'):
        keys.extend(source.DB.audio_id_to_child_keys(audio_id, class_name))
    objects = _copy_rows(keys, source, goal)
    speaker_keys = set()
    for obj in objects:
        key = getattr(obj, 'speaker_key', None)
        if key is not None: speaker_keys.add(key)
    missing = sorted(speaker_keys - copied_speakers)
    _copy_rows(missing, source, goal)
    copied_speakers.update(missing)
    _copy_speaker_audio_links(objects[0], speaker_keys, source, goal)


def _copy_speaker_audio_links(audio, speaker_keys, source, goal):
    '''Copy existing source links for this recording.'''
    for key in sorted(speaker_keys):
        speaker = goal.load(key)
        link = key_helper.speaker_audio_link(speaker, audio)
        if source.DB.load(link, db_name='speaker_audio') is None:
            continue
        speaker.add_audio(audio)


def _copy_rows(keys, source, goal):
    '''Deserialize fresh objects so cached source relationships never leak.'''
    objects = []
    values = source.DB.load_many(keys)
    for key, value in zip(keys, values, strict=True):
        if value is None: raise KeyError(f'linked key not found: {key.hex()}')
        objects.append(value_key_to_instance(goal, value, key))
    goal.save_many(objects)
    return objects
