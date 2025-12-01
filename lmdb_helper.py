import lmdb
import locations
import pickle
from pathlib import Path

def open_lmdb(env=None, path=locations.cgn_lmdb, map_size=1024**4):
    '''
    env : lmdb.Environment or None
    path : str
    map_size : int

    lmdb.Environment    The LMDB environment ready for use.
    '''
    if env is None:
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        env = lmdb.open(str(path), map_size = map_size)
    return env

def _key_bytes(key):
    '''Convert key to bytes if necessary.
    '''
    if isinstance(key, bytes):
        return key
    return str(key).encode("utf-8")


def write(key, value, env=None, path=locations.cgn_lmdb, overwrite = False):
    '''Write value as a pickled object to LMDB under key.
    key : Any
        Key under which the object is stored. Converted to bytes if needed.
    value : Any
        Python object to store (pickled before writing).
    env : lmdb.Environment or None
        Existing LMDB environment; if None, a default one is opened.
    path : str
        Path to the LMDB directory (only used when env is None).
    '''
    env = open_lmdb(env, path)
    k = _key_bytes(key)
    v = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)  
    if not overwrite:
        exists= key_exists(k, env=env)
        if exists:
            m = f'Key {key} already exists in LMDB store at {path}. '
            m += f'Use overwrite=True to overwrite.'
            raise KeyError(m)
    with env.begin(write=True) as txn:
        txn.put(k, v)  # overwrite=True by default


def load(key, env=None, path=locations.cgn_lmdb):
    '''load and unpickle object from LMDB under key.
    key : Any
        Key to retrieve. Converted to bytes automatically.
    env : lmdb.Environment or None
        Existing LMDB environment; if None, a default one is opened.
    path : str
        Path to the LMDB directory (only used when env is None).
    '''
    env = open_lmdb(env, path)
    k = _key_bytes(key)

    with env.begin() as txn:
        raw = txn.get(k)
        if raw is None:
            return None
        return pickle.loads(raw) 

def load_many(keys, env=None, path=locations.cgn_lmdb):
    """
    Load multiple LMDB values in a single read transaction.
    keys : list
        List of keys (bytes or convertible to bytes) to retrieve.
    env : lmdb.Environment or None
        Existing environment. If None, one is created.
    db_path : str
        LMDB path (only used if env is None).
    """
    env = open_lmdb(env, path)
    keys_b = [_key_bytes(k) for k in keys]

    results = {}
    with env.begin() as txn:
        for k, k_raw in zip(keys, keys_b):
            raw = txn.get(k_raw)
            if raw is not None:
                results[k] = pickle.loads(raw)
            else:
                results[k] = None
    return results

def key_exists(key, env=None, path=locations.cgn_lmdb):
    '''Check whether a key exists in the LMDB store.
    key : Any
        Key to check. Converted to bytes automatically.
    env : lmdb.Environment or None
        Existing LMDB environment; if None, a default one is opened.
    path : str
        Path to the LMDB directory (only used when env is None).
    '''
    env = open_lmdb(env, path)
    k = _key_bytes(key)
    with env.begin() as txn:
        return txn.get(k) is not None

def get_keys_with_prefix( prefix, env = None, path = locations.cgn_lmdb):
    """Return all keys (as bytes) starting with prefix (string or bytes)."""
    env = open_lmdb(env, path)
    prefix = _key_bytes(prefix)

    result = []
    with env.begin() as txn:
        cursor = txn.cursor()
        if cursor.set_range(prefix):      # jump to first >= prefix
            for k, _ in cursor:
                if not k.startswith(prefix):
                    break
                result.append(k)
    return result

def all_keys(env = None, path = locations.cgn_lmdb):
    env = open_lmdb(env, path)
    keys = []
    with env.begin() as txn:
        cursor = txn.cursor()
        for k, _ in cursor:
            keys.append(k)
    return keys

def object_type_to_keys_dict(env = None, path = locations.cgn_lmdb):
    import lmdb_key
    all_keys_list = all_keys(env, path)
    d = {}
    for key in all_keys_list:
        object_type = lmdb_key.key_to_object_type(key)
        d.setdefault(object_type, []).append(key)
    return d

def all_object_type_keys(object_type, d = None, env = None, 
    path = locations.cgn_lmdb):
    if d is None: d = object_type_to_keys_dict(env, path)
    if object_type not in d:
        raise ValueError(f'No keys found for object type: {object_type}')
    selected_keys = d[object_type]
    return selected_keys

def all_audio_keys(env = None, path = locations.cgn_lmdb):
    return all_object_type_keys('Audio', env, path)

def all_phrase_keys(env = None, path = locations.cgn_lmdb):
    return all_object_type_keys('Phrase', env, path)

def all_word_keys(env = None, path = locations.cgn_lmdb):
    return all_object_type_keys('Word', env, path)

def all_syllable_keys(env = None, path = locations.cgn_lmdb):
    return all_object_type_keys('Syllable', env, path)

def all_phone_keys(env = None, path = locations.cgn_lmdb):
    return all_object_type_keys('Phone', env, path)

def all_speaker_keys(env = None, path = locations.cgn_lmdb):
    return all_object_type_keys('Speaker', env, path)


        
    

def delete(key, env=None, path=locations.cgn_lmdb):
    env = open_lmdb(env, path)
    k= _key_bytes(key)
    if not key_exists(k, env=env): return

    with env.begin(write=True) as txn:
        txn.delete(k)

def delete_many(keys, env=None, path=locations.cgn_lmdb):
    env = open_lmdb(env, path)
    keys_b = [_key_bytes(k) for k in keys]

    with env.begin(write=True) as txn:
        for k in keys_b:
            txn.delete(k)

def delete_with_prefix( prefix, env = None, path = locations.cgn_lmdb):
    """Delete all keys starting with prefix (string or bytes)."""
    env = open_lmdb(env, path)

    keys_to_delete = lmdb_keys_with_prefix(prefix, env=env, path=path)
    print(f"Deleting {len(keys_to_delete)} keys with prefix {prefix}.")

    with env.begin(write=True) as txn:
        for k in keys_to_delete:
            txn.delete(k)

def delete_all(env=None, path=locations.cgn_lmdb):
    """Delete all keys in the LMDB store."""
    env = open_lmdb(env, path)

    keys_to_delete = all_keys(env)
    print(f"Deleting all {len(keys_to_delete)} keys.")

    with env.begin(write=True) as txn:
        for k in keys_to_delete:
            txn.delete(k)

