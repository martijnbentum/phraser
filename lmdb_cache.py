import lmdb_helper
import lmdb_key
import locations
import pickle


class Cache:
    """
    Barebones LMDB-backed store with:
    - safe caching
    - no nested pickles
    - resolver for parent, children, speaker, audio
    """

    def __init__(self, env = None, path = locations.cgn_lmdb, vebose = False ):
        self.env = lmdb_helper.open_lmdb(env, path)
        self._cache = {}      # key:str → object
        self.CLASS_MAP = {}   # filled externally (Audio, etc.)
        self.save_counter = {}
        self.load_counter = {}
        self.save_key_counter = {}
        self.vebose = vebose

    def register(self, cls):
        self.CLASS_MAP[cls.__name__] = cls
        if cls.__name__ not in self.save_counter:
            self.save_counter[cls.__name__] = 0
        if cls.__name__ not in self.load_counter:
            self.load_counter[cls.__name__] = 0

    def save(self, obj, overwrite = False, fail_gracefully = False):
        key = lmdb_key.item_to_key(obj)
        d = obj.to_dict()
        fail_message = f"Object with key {key} already exists. "
        fail_message += "Skipping save."
        try: lmdb_helper.write(key = key, value = d, env = self.env, 
            overwrite = overwrite)
        except KeyError as e:
            if fail_gracefully: print(fail_message)
            else: raise e
        self._cache[key] = obj
        self.save_counter[obj.object_type] += 1
        if key not in self.save_key_counter:
            self.save_key_counter[key] = 1
        else: self.save_key_counter[key] += 1


    def load(self, key, with_links = False):
        if isinstance(key, bytes): key = key.decode()
            
        if self.verbose: print(f"Loading key: {key}")
        if key in self._cache:
            if self.verbose: print(' Found in cache.')
            return self._cache[key]
        if self.verbose: print(' Not in cache. Loading from LMDB...')
        object_type = lmdb_key.key_to_object_type(key)
        cls = self.CLASS_MAP[object_type]
        obj = cls(key = key)
        self._cache[key] = obj
        self.load_counter[object_type] += 1
        return obj

    def load_many(self, keys, with_links = False):
        objs = []
        found_in_cache = []
        not_found_in_cache = []
        for key in keys:
            if key in self._cache:
                found_in_cache.append(key)
                objs.append(self._cache[key])
            else:
                not_found_in_cache.append(key)
        if len(not_found_in_cache) == 0: return objs
        if len(not_found_in_cache) == 1: 
            objs.append(self.load(not_found_in_cache[0], with_links))
        results = lmdb_helper.read_many(keys = not_found_in_cache, 
            env = self.env)
        self._cache.update(results)
        objs += [results[k] for k in not_found_in_cache]
        objs = sorted(objs, key = lambda x: keys.index(x.key))
        return objs

    def delete(self, key):
        lmdb_helper.delete(key = key, env = self.env)
        if key in self._cache: del self._cache[key]

    def update(self, old_key, obj):
        '''delete old_key and save obj with new key'''
        self.delete(old_key)
        self.save(obj, overwrite=True)
        if old_key in self._cache: del self._cache[old_key]
            
    def object_type_to_keys_dict(self, update = False):
        if not update:
            if hasattr(self, '_object_type_to_keys_dict'):
                return self._object_type_to_keys_dict
        d = lmdb_helper.object_type_to_keys_dict(env = self.env)
        self._object_type_to_keys_dict = d
        return self._object_type_to_keys_dict

    



