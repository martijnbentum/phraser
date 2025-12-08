import gc
import lmdb_helper
import lmdb_key
import locations
import pickle
import time


class Cache:
    """
    Barebones LMDB-backed store with:
    - safe caching
    - no nested pickles
    - resolver for parent, children, speaker, audio
    """

    def __init__(self, env = None, path = locations.cgn_lmdb, verbose= False ):
        self.env = lmdb_helper.open_lmdb(env, path)
        self._cache = {}      # key:str → object
        self.CLASS_MAP = {}   # filled externally (Audio, etc.)
        self.save_counter = {}
        self.load_counter = {}
        self.save_key_counter = {}
        self.verbose = verbose

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
        data = lmdb_helper.load(key = key, env = self.env)
        obj = data_dict_to_instance(cls, data)
        self._cache[key] = obj
        self.load_counter[object_type] += 1
        return obj

    def load_many(self, keys, with_links = False):
        if len(keys) == 0: return []
        if len(keys) == 1: return [self.load(keys[0], with_links)]
        start = time.time()
        objs = [ [] for _ in range(len(keys)) ]
        key_to_index = {key: i for i, key in enumerate(keys)}
        print(time.time() - start, 'objs initialized')
        found_in_cache = []
        not_found_in_cache = []
        for index, key in enumerate(keys):
            if key in self._cache:
                found_in_cache.append(key)
                objs[index] = self._cache[key]
            else:
                not_found_in_cache.append(key)
        print(time.time() - start, 'cache checked')
        if len(not_found_in_cache) == 0: return objs
        if len(not_found_in_cache) == 1: 
            index = key_to_index[not_found_in_cache[0]]
            objs[index] = self.load(not_found_in_cache[0], with_links)
        if len(not_found_in_cache) > 100_000: gc.disable()
        try:
            results = lmdb_helper.load_many(keys = not_found_in_cache, 
                env = self.env)
            print(time.time() - start, 'lmdb data loaded')
            for key, data in zip(not_found_in_cache, results):
                index = key_to_index[key]
                object_type = lmdb_key.key_to_object_type(key)
                cls = self.CLASS_MAP[object_type]
                obj = data_dict_to_instance(cls, data)
                self._cache[key] = obj
                objs[index] = obj
                self.load_counter[object_type] += 1
            print(time.time() - start, 'objs created')
        finally:
            if len(not_found_in_cache) > 100_000: gc.enable()
        print('gc enabled', time.time() - start)
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


def data_dict_to_instance(cls, data):
    obj = cls.__new__(cls)
    data['object_type'] = cls.__name__
    extra = data.pop('extra')
    obj.__dict__.update(data)
    obj.__dict__.update(extra)
    return obj
    

class Objects:
    def __init__(self, cls, cache):
        self.cls = cls
        self.cache = cache
        self.object_type = cls.__name__


    def all(self, update = False):
        d = self.cache.object_type_to_keys_dict(update = update)
        m = f'No keys found for object type: {self.object_type}'
        if self.object_type not in d:
            raise ValueError(m)
        selected_keys = d[self.object_type]
        objs = self.cache.load_many(selected_keys)
        return objs

    def filter(self, filter_func, update = False):
        all_objs = self.all(update = update)
        selected_objs = [obj for obj in all_objs if filter_func(obj)]
        return selected_objs

