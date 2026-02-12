import lmdb
import locations
import pickle
from pathlib import Path
from progressbar import progressbar


def instance_to_child_keys(audio_id, model_type = 'Phrase', env = None,
    path = locations.cgn_lmdb):
    import lmdb_key
    mapper = lmdb_key.TYPE_TO_RANK_MAP 
    rank = mapper[model_type]
    prefix = f"{audio_id}:{rank}:".encode()
    with env.begin() as txn:
        cur = txn.cursor()
        if not cur.set_range(prefix):
            return
        for k in cur.iternext(keys=True, values=False):
            if not k.startswith(prefix):
                break
            yield k  # or (k, v)


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

def write_many(keys, values, env=None, path=locations.cgn_lmdb, 
    overwrite = False):

    env = open_lmdb(env, path)
        
    #fail early if any key exists and overwrite is False
    if check_any_key_exist(keys, env=env, path=path) and not overwrite:
        m = f'At least one key already exists in LMDB store at {path}. '
        m += f'Use overwrite=True to overwrite.'
        m += f'written nothing.'
        raise KeyError(m)

    with env.begin(write=True) as txn:
        for key, value in progressbar(zip(keys, values), max_value=len(keys)):
            k = _key_bytes(key)
            v = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)  
            txn.put(k, v)


def check_any_key_exist(keys, env=None, path=locations.cgn_lmdb):
    db_keys = all_keys(env, path)
    for key in keys:
        k = _key_bytes(key)
        if k in db_keys: return True
    return False


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

    objs = [[] for _ in range(len(keys))]
    with env.begin() as txn:
        for index, k_raw in enumerate(keys_b):
            raw = txn.get(k_raw)
            if raw is not None:
                objs[index] = pickle.loads(raw)
            else:
                objs[index] = None
    return objs

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

def iter_model_keys_for_audio_id(audio_id, model_type = 'Phrase', env = None,
    path = locations.cgn_lmdb):
    import lmdb_key
    mapper = lmdb_key.TYPE_TO_RANK_MAP 
    rank = mapper[model_type]
    prefix = f"{audio_id}:{rank}:".encode()
    with env.begin() as txn:
        cur = txn.cursor()
        if not cur.set_range(prefix):
            return
        for k in cur.iternext(keys=True, values=False):
            if not k.startswith(prefix):
                break
            yield k  # or (k, v)

def get_all_keys_with_audio_id(audio_id, env = None, path = locations.cgn_lmdb):
    return get_all_keys_with_prefix(audio_id, env, path)

def get_all_keys_with_prefix(prefix, env = None, path = locations.cgn_lmdb):
    """Return all keys (as bytes) starting with prefix (string or bytes)."""
    env = open_lmdb(env, path)
    prefix = _key_bytes(prefix)

    result = []
    with env.begin() as txn:
        cursor = txn.cursor()
        if cursor.set_range(prefix):      # jump to first >= prefix
            for k in cursor.iternext(keys=True, values=False):
                if not k.startswith(prefix):
                    break
                result.append(k)
    return result

def all_keys(env = None, path = locations.cgn_lmdb):
    if env is None: env = open_lmdb(env, path)
    keys = []
    with env.begin() as txn:
        cursor = txn.cursor()
        for k, _ in cursor:
            keys.append(k)
    return set(keys)

def object_type_to_keys_dict(env = None, path = locations.cgn_lmdb):
    import lmdb_key
    if env is None: env = open_lmdb(path = path)
    d = {}
    with env.begin() as txn:
        cursor = txn.cursor()
        for key, _ in cursor:
            object_type = lmdb_key.key_to_object_type(key)
            d.setdefault(object_type, []).append(key)
    return d

def all_object_type_keys(object_type, env = None, 
    path = locations.cgn_lmdb, d = None):
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
    batch_size = 10_000
    i = 0
    with env.begin(write=True) as txn:
        for k in progressbar(keys_b):
            i += 1
            txn.delete(k)
            if i % batch_size == 0:
                txn.commit()
                txn = env.begin(write=True)

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

    delete_many(keys_to_delete, env=env)







'''
other open_env with read_only mode check and reuse if possible:
def get_lmdb_env(env = None, path = None, read_only = True, max_dbs = 0, 
    readahead = True):
    Return an LMDB environment matching the requested mode.

    If an existing env is provided and its read_only mode matches,
    it is reused. If the mode differs, the env is closed and a new
    one is opened.

    env:  Optional existing lmdb.Environment.
    path: Path to the LMDB directory (required if env is None).
    read_only: Open environment in read-only mode if True.
    max_dbs: LMDB max_dbs parameter.
    readahead: Enable LMDB readahead (default True). Set to False for random access patterns.

    if env is not None:
        # lmdb.Environment exposes read-only state
        if env.readonly == read_only:
            return env
        env.close()

    if path is None:
        raise ValueError('path is required when env is None')

    return lmdb.open(
        path,
        readonly=read_only,
        lock=not read_only,
        readahead=readahead,
        max_dbs=max_dbs,
    )
'''
