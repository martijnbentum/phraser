'''Copy complete recording hierarchies between Phraser stores.'''

from pathlib import Path

from . import key_helper
from .store import Store, value_key_to_instance


def copy_audio_materials(audio_keys, goal_store_root, source_store):
    '''Copy recordings and their annotations; return an open goal Store.
    audio_keys:      source Audio keys; repeated keys are copied once
    goal_store_root: new or empty directory for the destination store
    source_store:    open source Store, left open and unchanged

    Preserve persisted keys, metadata, and audio filenames. Include linked
    speakers and all phrases, words, syllables, and phones; exclude markers.
    Audio files are not copied. The caller closes the returned store.
    '''
    source_store._ensure_open()
    root = Path(goal_store_root)
    if root.resolve() == Path(source_store.path).resolve():
        raise ValueError('goal store must differ from source store')
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise FileExistsError('goal store root must be new or empty')
    keys = list(dict.fromkeys(audio_keys))
    _check_audio_keys(keys, source_store)
    goal = Store(path=root)
    copied_speakers = set()
    try:
        for key in keys:
            _copy_audio(key, source_store, goal, copied_speakers)
            goal._cache.clear()
        goal.refresh_query_roots()
    except Exception:
        goal.close()
        raise
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


def _copy_rows(keys, source, goal):
    '''Deserialize fresh objects so cached source relationships never leak.'''
    objects = []
    values = source.DB.load_many(keys)
    for key, value in zip(keys, values, strict=True):
        if value is None: raise KeyError(f'linked key not found: {key.hex()}')
        objects.append(value_key_to_instance(goal, value, key))
    goal.save_many(objects)
    return objects
