import lmdb
import lmdb_key
import locations
import pickle
from pathlib import Path
from progressbar import progressbar

default_db_name = 'main'

class DB:
    def __init__(self, path=locations.cgn_lmdb, map_size=1024**4,
        db_names = ['main', 'speaker_audio']):
        self.path = path
        self.map_size = map_size
        self.db_names = db_names
        self.max_dbs = len(db_names)
        self._open_env()

    def _open_env(self):
        env, db = open_lmdb(self.path, self.map_size, self.db_names, 
            self.max_dbs)
        self.env = env
        self.db = db

    def all_keys(self, db_name = 'main'):
        '''Return a list of all keys in the LMDB store for a named db.
        db_name:   Name of the LMDB database to use (default 'main').
        '''
        keys = []
        db = self.db[db_name]
        with self.env.begin() as txn:
            cursor = txn.cursor(db = db)
            for k in cursor.iternext(keys=True, values=False):
                keys.append(k)
        return keys

    def all_links(self, db_name = 'speaker_audio'):
        return self.all_keys(db_name = db_name)

    def key_exists(self, key, db_name = 'main'):
        '''Check whether a key exists in the LMDB store.
        key:       Key to retrieve. 
        db_name:   Name of the LMDB database to use (default 'main').
        '''
        db = self.db[db_name]
        with self.env.begin() as txn:
            return txn.get(key, db = db) is not None

    def check_any_key_exist(self, keys, db_name = 'main'):
        db_keys = set(self.all_keys(db_name = db_name))
        for key in keys:
            if key in db_keys: return True
        return False

    def load(self, key, db_name = 'main'):
        '''load an object from LMDB under key.
        key:       Key to retrieve. 
        db_name:   Name of the LMDB database to use (default 'main').
        '''
        db = self.db[db_name]
        with self.env.begin() as txn:
            raw = txn.get(key, db= db)
            if raw is None: return None
        return raw

    def load_many(self, keys, db_name = 'main'):
        """
        Load multiple LMDB values in a single read transaction.
        keys:       List of keys (bytes) to retrieve.
        db_name:   Name of the LMDB database to use (default 'main').
        """

        objs = [[] for _ in range(len(keys))]
        db = self.db[db_name]
        with self.env.begin() as txn:
            for index, key in enumerate(keys):
                objs[index] = txn.get(key, db = db)
        return objs

    def write(self, key, value, db_name = 'main', overwrite = False):
        '''Write byte value to LMDB under byte key.
        key:       Key to write value under. 
        value:     struct made in struct_value.  
        db_name:   Name of the LMDB database to use (default 'main').
        overwrite:  If False, raises an error if the key already exists. 
                    If True, overwrites existing value.
        '''
        if not overwrite:
            exists= self.key_exists(key, db_name = db_name)
            if exists:
                m = f'Key {key} already exists in LMDB store at {path}. '
                m += f'Use overwrite=True to overwrite.'
                raise KeyError(m)
        db = self.db[db_name]
        with self.env.begin(write=True) as txn:
            txn.put(key, value, db = db)  

    def write_many(self, keys, values, db_name = 'main', overwrite = False):
        '''
        key:       Key to write value under. 
        value:     struct made in struct_value.  
        db_name:   Name of the LMDB database to use (default 'main').
        overwrite:  If False, raises an error if the key already exists. 
                    If True, overwrites existing value.
        '''
        #fail early if any key exists and overwrite is False
        if self.check_any_key_exist(keys, db_name) and not overwrite:
            m = f'At least one key already exists in LMDB store at {path}. '
            m += f'Use overwrite=True to overwrite.'
            m += f'written nothing.'
            raise KeyError(m)

        db = self.db[db_name]
        with self.env.begin(write=True) as txn:
            for k, v in progressbar(zip(keys, values), max_value=len(keys)):
                txn.put(k, v, db = db)

    def audio_id_to_child_keys(self, audio_id, child_class = 'Phrase'):
        db = self.db['main']
        prefix = lmdb_key.audio_id_to_scan_prefix(audio_id, child_class)
        with self.env.begin() as txn:
            cur = txn.cursor(db = db)
            if not cur.set_range(prefix):
                return
            for k in cur.iternext(keys=True, values=False):
                if not k.startswith(prefix):
                    break
                yield k  # or (k, v)

    def instance_to_child_keys(self, instance):
        db = self.db['main']
        f = lmdb_key.instance_to_child_time_scan_keys
        start_prefix, end_prefix = f(instance)
        with self.env.begin() as txn:
            cur = txn.cursor(db = db)
            if not cur.set_range(start_prefix):
                return
            for k in cur.iternext(keys=True, values=False):
                if k > end_prefix:
                    break
                yield k  # or (k, v)

    def object_type_to_keys_dict(self):
        db = self.db['main']
        d = {}
        with self.env.begin() as txn:
            cursor = txn.cursor(db = db)
            for key, _ in cursor:
                object_type = lmdb_key.key_to_object_type(key)
                d.setdefault(object_type, []).append(key)
        return d

    def all_object_type_keys(self, object_type, d = None):
        db = self.db['main']
        if d is None: d = self.object_type_to_keys_dict()
        if object_type not in d:
            raise ValueError(f'No keys found for object type: {object_type}')
        selected_keys = d[object_type]
        return selected_keys

    def all_audio_keys(self):
        return self.all_object_type_keys('Audio')

    def all_phrase_keys(self):
        return self.all_object_type_keys('Phrase')

    def all_word_keys(self):
        return self.all_object_type_keys('Word')

    def all_syllable_keys(self):
        return self.all_object_type_keys('Syllable')

    def all_phone_keys(self):
        return self.all_object_type_keys('Phone')

    def all_speaker_keys(self):
        return self.all_object_type_keys('Speaker')

    def delete(self, key, db_name = 'main'):
        db = self.db[db_name]
        if not self.key_exists(key): return
        with self.env.begin(write=True) as txn:
            txn.delete(key, db = db)

    def delete_many(self, keys, db_name = 'main'):
        db = self.db[db_name]
        batch_size = 10_000
        i = 0
        txn = self.env.begin(write=True)
        try:
            for k in progressbar(keys):
                i += 1
                txn.delete(k, db = db)
                if i % batch_size == 0:
                    txn.commit()
                    txn = self.env.begin(write=True)
            txn.commit()
        except Exception as e:
            print(f'Error {e}, while deleting key: {k}')
            txn.abort()
            raise e

    def delete_main(self):
        """Delete all keys in the main LMDB database."""
        keys_to_delete = self.all_keys(db_name = 'main')
        print(f"Deleting all {len(keys_to_delete)} keys in db main.")
        self.delete_many(keys_to_delete, db_name = 'main')

    def delete_all_speaker_audio(self):
        """Delete all keys in the speaker_audio LMDB database."""
        keys_to_delete = self.all_links()
        print(f"Deleting all {len(keys_to_delete)} keys in db speaker_audio.")
        self.delete_many(keys_to_delete, db_name = 'speaker_audio')

    def delete_all(self) :
        """Delete all keys in the LMDB store."""
        self.delete_main()
        self.delete_all_speaker_audio()

    def write_speaker_audio_link(self, speaker, audio):
        link= lmdb_key.speaker_audio_link(speaker, audio)
        self.write(link, b'', db_name = 'speaker_audio', overwrite = True)

    def delete_speaker_audio_link(self, speaker, audio):
        link = lmdb_key.speaker_audio_link(speaker, audio)
        self.delete(link, db_name = 'speaker_audio')

    def _speaker_audio_links(self, speaker):
        db = self.db['speaker_audio']
        prefix = lmdb_key.speaker_id_to_scan_prefix(speaker.identifier)
        with self.env.begin() as txn:
            cur = txn.cursor(db = db)
            if not cur.set_range(prefix):
                return
            for k in cur.iternext(keys=True, values=False):
                if not k.startswith(prefix):
                    break
                yield k

    def speaker_to_audio_keys(self, speaker):
        links = self._speaker_audio_links(speaker)
        z = b'\x00'
        audio_keys = [z + k[-8:] + z for k in links]
        return audio_keys

        


def open_lmdb(path=locations.cgn_lmdb, map_size=1024**4, 
    db_names = ['main', 'speaker_audio'], max_dbs = 2):
     
    '''
    env : lmdb.Environment or None
    path : str
    map_size : int

    lmdb.Environment    The LMDB environment ready for use.
    '''

    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    env = lmdb.open(str(path), map_size = map_size, max_dbs = max_dbs)

    db = {}
    with env.begin(write = True) as txn:
        for name in db_names:
            db[name] = env.open_db(name.encode(), txn=txn)
    return env, db

