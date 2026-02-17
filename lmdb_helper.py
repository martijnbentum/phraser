import lmdb
import lmdb_key
import locations
import pickle
from pathlib import Path
from progressbar import progressbar


def audio_id_to_child_keys(audio_id, child_class = 'Phrase', env = None, 
    path = locations.cgn_lmdb):
    env = open_lmdb(env, path)
    prefix = lmdb_key.audio_id_to_scan_prefix(audio_id, child_class)
    with env.begin() as txn:
        cur = txn.cursor()
        if not cur.set_range(prefix):
            return
        for k in cur.iternext(keys=True, values=False):
            if not k.startswith(prefix):
                break
            yield k  # or (k, v)


def instance_to_child_keys(instance, env = None, path = locations.cgn_lmdb):
    env = open_lmdb(env, path)
    start_prefix, end_prefix = lmdb_key.instance_to_child_time_scan_keys(instance)
    with env.begin() as txn:
        cur = txn.cursor()
        if not cur.set_range(start_prefix):
            return
        for k in cur.iternext(keys=True, values=False):
            if k > end_prefix:
                break
            yield k  # or (k, v)

def instance_to_children(instance, env = None, path = locations.cgn_lmdb):
    env = open_lmdb(env, path)
    start_prefix, end_prefix = lmdb_key.instance_to_child_time_scan_keys(instance)
    with env.begin() as txn:
        cur = txn.cursor()
        if not cur.set_range(start_prefix):
            return
        for k, v in cur.iternext(keys=True, values=True):
            if k > end_prefix:
                break
            yield k, v

def open_lmdb(path=locations.cgn_lmdb, map_size=1024**4):
     
    '''
    env : lmdb.Environment or None
    path : str
    map_size : int

    lmdb.Environment    The LMDB environment ready for use.
    '''

    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    env = lmdb.open(str(path), map_size = map_size)

    with env.begin(write=not read_only) as txn:
        db = {
            'main': env.open_db(b'main', txn=txn),
            'speaker_audio': env.open_db(b'speaker_audio', txn=txn),
        }

    return env, db


def write(key, value, env=None, path=locations.cgn_lmdb, overwrite = False):
    '''Write byte value to LMDB under byte key.
    key : byte
        Key under which the object is stored. 
    value : byte
        struct made in struct_value.
    env : lmdb.Environment or None
        Existing LMDB environment; if None, a default one is opened.
    path : str
        Path to the LMDB directory (only used when env is None).
    '''
    env = open_lmdb(env, path)
    if not overwrite:
        exists= key_exists(key, env=env)
        if exists:
            m = f'Key {key} already exists in LMDB store at {path}. '
            m += f'Use overwrite=True to overwrite.'
            raise KeyError(m)
    with env.begin(write=True) as txn:
        txn.put(key, value)  # overwrite=True by default

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
            txn.put(key, value)


def check_any_key_exist(keys, env=None, path=locations.cgn_lmdb):
    db_keys = all_keys(env, path)
    for key in keys:
        if key in db_keys: return True
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

    with env.begin() as txn:
        raw = txn.get(key)
        if raw is None:
            return None
    return raw

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

    objs = [[] for _ in range(len(keys))]
    with env.begin() as txn:
        for index, key in enumerate(keys):
            objs[index] = txn.get(key)
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
    with env.begin() as txn:
        return txn.get(key) is not None

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
    if not key_exists(k, env=env): return

    with env.begin(write=True) as txn:
        txn.delete(key)

def delete_many(keys, env=None, path=locations.cgn_lmdb):
    env = open_lmdb(env, path)
    batch_size = 10_000
    i = 0
    with env.begin(write=True) as txn:
        for k in progressbar(keys):
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


