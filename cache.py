import gc
import lmdb_helper
import lmdb_key
import locations
import pickle
import random
import struct_key
import struct_value
import time
import utils

R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"


class Cache:
    """
    Barebones LMDB-backed store with:
    - safe caching
    - no nested pickles
    - resolver for parent, children, speaker, audio
    """

    def __init__(self, path = locations.cgn_lmdb, verbose = False):
        self.DB = lmdb_helper.DB(path = path)
        self.path = path
        self._cache = {}      # key:str → object
        self.CLASS_MAP = {}   # filled externally (Audio, etc.)
        self.save_counter = {}
        self.load_counter = {}
        self.save_key_counter = {}
        self.verbose = verbose
        self._classes_loaded = {}
        self.fraction = None
        self.db_saving_allowed = True

    def __repr__(self):
        m = f'<{R}Cache{RE} {B}path{RE} {self.path} | '
        m += f'{B}cached objects{RE} {len(self._cache)} | '
        m += f'{B}db objects{RE} {len(self.all_keys())}>'
        return m

    def __str__(self):
        d = self.object_type_to_keys_dict()
        m = self.__repr__() + '\n'
        m += f'{G}cached objects per class:{RE}\n'
        for class_name, count in self.load_counter.items():
            m += f'  {B}{class_name:<9}{RE} {count}\n'
        m += f'{G}db objects per class:{RE}\n'
        for class_name, keys in d.items():
            m += f'  {B}{class_name:<9}{RE} {len(keys)}\n'
        m += f'{G}saved objects per class (this session):{RE}\n'
        for class_name, count in self.save_counter.items():
            m += f'  {B}{class_name:<9}{RE} {count}\n'
        m += f'{G}fully cached classes:{RE} '
        x = [name for name, loaded in self._classes_loaded.items() if loaded]
        m += f'  {", ".join(x)}\n'
        if self.fraction is not None:
            m += f'using part of db {B}sampling fraction{RE} {self.fraction}\n'
            m += f'do {R}models.load_cache(){RE} to gain full access to db\n'
        else: m += f'{GR}using full database (no sampling fraction){RE}\n'
        return m

    def register(self, cls):
        '''Register a class for loading/saving.
        also initializes load and save counters for the class.
        '''
        self.CLASS_MAP[cls.__name__] = cls
        if cls.__name__ not in self.save_counter:
            self.save_counter[cls.__name__] = 0
        if cls.__name__ not in self.load_counter:
            self.load_counter[cls.__name__] = 0

    def save(self, obj, overwrite = False, fail_gracefully = False):
        '''save an object to LMDB.
        overwrite: if True, overwrite existing object with same key.
                   if False, raise KeyError if key exists and not 
                        fail_gracefully.
        fail_gracefully : if True, print a message and skip saving if object
                          exists in database
        '''
        if not self.db_saving_allowed: return
        key = lmdb_key.instance_to_key(obj)
        value = struct_value.pack_instance(obj)
        fail_message = f"Object with key {key} already exists. "
        fail_message += "Skipping save."
        try: self.DB.write(key = key, value = value, overwrite = overwrite)
            
        except KeyError as e:
            if fail_gracefully: print(fail_message)
            else: raise e
        self._cache[key] = obj
        self.save_counter[obj.object_type] += 1
        if key not in self.save_key_counter:
            self.save_key_counter[key] = 1
        else: self.save_key_counter[key] += 1

    def save_many(self, objs, overwrite = False, fail_gracefully = False):
        if not self.db_saving_allowed: return
        start = time.time()
        itk = lmdb_key.instance_to_key
        pi = struct_value.pack_instance
        cache_update = {itk(obj): pi(obj) for obj in objs}
        # print('update dict done', time.time() - start)
        try: self.DB.write_many(cache_update.keys(), cache_update.values(),
            overwrite = overwrite)

        except KeyError as e:
            
            print('failed', time.time() - start)
            if fail_gracefully: 
                print(e)
                return
            else: raise e

        # print('succes', time.time() - start)

        self._cache.update(cache_update)
        for key in cache_update.keys():
            if key not in self.save_key_counter:
                self.save_key_counter[key] = 1
            else: self.save_key_counter[key] += 1
        # print('done', time.time() - start)

    def load(self, key, with_links = False):
        '''load an object from LMDB by key.
        key: to load the object from the database.
        with_links: if True, load linked objects (e.g.children, audio, speaker)
                    rarely used because it requires multiple LMDB hits.
        '''
            
        if self.verbose: print(f"Loading key: {key}")
        if key in self._cache:
            if self.verbose: print(' Found in cache.')
            return self._cache[key]
        if self.verbose: print(' Not in cache. Loading from LMDB...')
        
        value = self.DB.load(key = key) 
        obj = value_key_to_instance(self, value, key)
        self._cache[key] = obj
        self.load_counter[obj.object_type] += 1
        return obj

    def load_many(self, keys, with_links = False):
        '''load many objects from LMDB by keys in bulk.
        keys: list of str or bytes to load the objects from the database.
        with_links: should be removed because it is not used in bulk loading.
        
        bulk loading is much faster than loading one by one because it
        minimizes LMDB hits.
        garbage collection is disabled if loading more than 100,000 objects
        to speed up loading further
        '''
        if len(keys) == 0: return []
        if len(keys) == 1: return [self.load(keys[0], with_links)]
        # preparation phase to return objects in same order as keys
        start = time.time()
        objs = [ [] for _ in range(len(keys)) ]
        key_to_index = {key: i for i, key in enumerate(keys)}
        if self.verbose: print(time.time() - start, 'objs initialized')

        # cache phase to load all objects that are already in cache
        found_in_cache = []
        not_found_in_cache = []
        for index, key in enumerate(keys):
            if key in self._cache:
                found_in_cache.append(key)
                objs[index] = self._cache[key]
            else:
                not_found_in_cache.append(key)
        if self.verbose: print(time.time() - start, 'cache checked')

        # if all found in cache or only one not found, return early
        if len(not_found_in_cache) == 0: return objs
        if len(not_found_in_cache) == 1: 
            index = key_to_index[not_found_in_cache[0]]
            objs[index] = self.load(not_found_in_cache[0], with_links)
            return objs

        # lmdb loading phase to bulk load all objects not found in cache
        # disable garbage collection for large loads to speed up loading
        if len(not_found_in_cache) > 100_000: gc.disable()
        try:
            results = self.DB.load_many(keys = not_found_in_cache) 
                
            if self.verbose: print(time.time() - start, 'lmdb data loaded')
            for key, data in zip(not_found_in_cache, results):
                index = key_to_index[key]
                value = self.DB.load(key = key)
                obj = value_key_to_instance(self, value, key)
                self._cache[key] = obj
                objs[index] = obj
                self.load_counter[obj.object_type] += 1
            # print(time.time() - start, 'objs created')
        finally:
            # reenable garbage collection (also in case of error)
            if len(not_found_in_cache) > 100_000: gc.enable()
        if self.verbose: print('gc enabled', time.time() - start)
        return objs

    def delete(self, key):
        '''delete an object from LMDB by key'''
        self.DB.delete(key = key)
        if key in self._cache: del self._cache[key]

    def delete_many(self, keys):
        '''delete many objects from LMDB by keys'''
        self.DB.delete_many(keys = keys)
        for key in keys:
            if key in self._cache: del self._cache[key]

    def update(self, old_key, obj):
        '''delete old_key and save obj with new key'''
        self.delete(old_key)
        self.save(obj, overwrite=True)
        if old_key in self._cache: del self._cache[old_key]
            
    def object_type_to_keys_dict(self, update = False):
        '''return a dict mapping object_type (class name) to list of keys'''
        if not update:
            if hasattr(self, '_object_type_to_keys_dict'):
                return self._object_type_to_keys_dict
        d = self.DB.object_type_to_keys_dict()
        self._object_type_to_keys_dict = d
        return self._object_type_to_keys_dict

    def all_keys(self):
        return self.DB.all_keys()

    def all_links(self):
        return self.DB.all_links()

    def preload_class_instances(self,cls = None, class_name = None):
        if cls is None and class_name is None:
            raise ValueError('Either cls or class_name must be provided.')
        if cls is None:
            cls = self.CLASS_MAP[class_name]
        start = time.time()
        class_name = cls.__name__
        if class_name in self._classes_loaded: return
        keys = self.object_type_to_keys_dict().get(class_name, [])
        self.load_many(keys)
        duration = time.time() - start
        m = f'Loaded all objects of class: {cls.__name__}'
        m += f', in {duration:.2f} seconds.'
        if self.verbose: print(m) 
        self._classes_loaded[name] = True

    def _preload_sampled_fraction(self, fraction):
        '''preload a fraction of all objects per class'''
        self.fraction = fraction
        if self.fraction is None: return
        phrases = sample_instances_from_class(self, 'Phrase', self.fraction)
        d = load_hierarchy_from_phrases(self, phrases['instances'])
        d['Phrase'] = phrases
        for key in self.CLASS_MAP.keys():
            if key in d: self._classes_loaded[key] = True

    def turn_on_db_saving(self):
        '''allow saving to LMDB'''
        self.db_saving_allowed = True

    def turn_off_db_saving(self):
        '''disallow saving to LMDB'''
        self.db_saving_allowed = False

    def is_db_saving_allowed(self):
        '''return True if saving to LMDB is allowed'''
        return self.db_saving_allowed
            
            



def value_key_to_instance(cache, value, key):
    '''convert value, key loaded from LMDB to an instance of cls
    this speeds up loading by avoiding __init__ calls
    '''

    info = lmdb_key.key_to_info(key)
    object_type = info['object_type']
    cls = cache.CLASS_MAP[object_type]
    obj = cls.__new__(cls)
    data = struct_value.unpack_instance(object_type, value)
    data.update(info)
    obj.__dict__.update(data)
    return obj


    



    


def collect_attribute_keys(objs, attr_name):
    '''Given a list of objects, return a flat list of the specified attribute keys
    e.g., 'audio_key', 'speaker_key'.
    '''
    keys = []
    append = keys.append  # local binding for speed
    for obj in objs:
        key = getattr(obj, attr_name, None)
        if key is not None:
            append(key)
    return keys

def load_phrase_descendants(cache, phrases):
    '''
    load all descendants of given parent objects in order of classes.
    all descendants are bulk loaded per class to minimize LMDB hits.
    
    parents: list of root objects
    Returns a dict [class_name] = {'keys':keys, 'instances':objects}.
    '''
    results = {}
    word_keys, syllable_keys, phone_keys = [], [], []  
    for phrase in phrases:
        k = cache.DB.instance_to_child_keys(phrase, 'Word')
        if k: word_keys.extend(k)
        k = cache.DB.instance_to_child_keys(phrase, 'Syllable')
        if k: syllable_keys.extend(k)
        k = cache.DB.instance_to_child_keys(phrase, 'Phone')
        if k: phone_keys.extend(k)
    items=[('Word',word_keys),('Syllable',syllable_keys),('Phone',phone_keys)]
    for child_class_name, child_keys in items:
        instances = cache.load_many(child_keys)
        results[child_class_name] = {'keys': child_keys,'instances':instances}
    return results


def load_linked_audio_and_speakers(cache, objs):
    '''based on a list of objects with audio_key and speaker_key attributes,
    load all linked audio and speaker objects in bulk.
    '''
    audio_keys   = collect_attribute_keys(objs, 'audio_key')
    speaker_keys = collect_attribute_keys(objs, 'speaker_key')

    # dedupe (optional)
    audio_keys   = list(set(audio_keys))
    speaker_keys = list(set(speaker_keys))

    audios   = cache.load_many(audio_keys)
    speakers = cache.load_many(speaker_keys)
    d = {}
    d['Audio']   = {'keys': audio_keys,   'instances': audios}
    d['Speaker'] = {'keys': speaker_keys, 'instances': speakers}
    return d


def load_hierarchy_from_phrases(cache, phrases):
    '''load words, syllables, phones, audios, and speakers linked to 
    the list of phrases in bulk per class
    this avoids repeated LMDB hits when loading linked objects per phrase
    '''
    instances_dict =  load_phrase_descendants(cache, phrases)
    audio_speaker_dict = load_linked_audio_and_speakers(cache, phrases)
    instances_dict.update(audio_speaker_dict)
    return instances_dict

def sample_instances_from_class(cache, class_name = 'Phrase', fraction = 0.1):
    '''randomly sample a fraction of all objects of the given class'''
    keys = cache.object_type_to_keys_dict(update=False).get(class_name, [])
    n_sample = max(1, int(len(keys) * fraction))
    sampled_keys = random.sample(keys, n_sample)
    sampled_objects = cache.load_many(sampled_keys)
    return {'keys': sampled_keys, 'instances': sampled_objects} 

