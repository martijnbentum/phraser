import gc
import pickle
import random
import time

from . import key_helper
from . import lmdb_helper
from . import locations
from . import struct_value
from . import utils
from .struct_helper import RANK_CLASS_MAP

R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"


class UnboundStoreError(RuntimeError):
    pass


class Store:
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
        m = f'<{R}Store{RE} {B}path{RE} {self.path} | '
        m += f'{B}cached objects{RE} {len(self._cache)}>'
        return m

    def __str__(self):
        d = self.rank_to_keys_dict()
        m = self.__repr__() + '\n'
        m += f'{G}cached objects per class:{RE}\n'
        for class_name, count in self.load_counter.items():
            m += f'  {B}{class_name:<9}{RE} {count}\n'
        m += f'{G}db objects per class:{RE}\n'
        for rank, keys in d.items():
            class_name = RANK_CLASS_MAP.get(rank, str(rank))
            m += f'  {B}{class_name:<9}{RE} {len(keys)}\n'
        m += f'{G}saved objects per class (this session):{RE}\n'
        for class_name, count in self.save_counter.items():
            m += f'  {B}{class_name:<9}{RE} {count}\n'
        m += f'{G}fully cached classes:{RE} '
        x = [name for name, loaded in self._classes_loaded.items() if loaded]
        m += f'  {", ".join(x)}\n'
        if self.fraction is not None:
            m += f'using part of db {B}sampling fraction{RE} {self.fraction}\n'
            m += f'do {R}models.open_store(){RE} to gain full access to db\n'
        else: m += f'{GR}using full database (no sampling fraction){RE}\n'
        return m

    def register(self, cls):
        '''Register a class so the store can load and save its instances.
        Initializes per-class load and save counters.
        Must be called for every domain class (Audio, Phrase, Word, Syllable,
        Phone, Speaker) before using the store.
        '''
        self.CLASS_MAP[cls.__name__] = cls
        if cls.__name__ not in self.save_counter:
            self.save_counter[cls.__name__] = 0
        if cls.__name__ not in self.load_counter:
            self.load_counter[cls.__name__] = 0

    def attach_query_roots(self):
        '''Attach store-scoped query roots for each registered domain class.
        Creates self.audios, self.phrases, self.words, self.syllables,
        self.phones, and self.speakers as Query objects bound to this store.
        Called once during store initialisation (see models.open_store) and
        again by refresh_query_roots when the db changes.
        '''
        from . import query
        self.audios = query.get_class_object(self.CLASS_MAP['Audio'], self)
        self.phrases = query.get_class_object(self.CLASS_MAP['Phrase'], self)
        self.words = query.get_class_object(self.CLASS_MAP['Word'], self)
        self.syllables = query.get_class_object(
            self.CLASS_MAP['Syllable'], self)
        self.phones = query.get_class_object(self.CLASS_MAP['Phone'], self)
        self.speakers = query.get_class_object(self.CLASS_MAP['Speaker'], self)

    def query_for_class(self, cls):
        '''Return the store-scoped Query object for the given class.
        Equivalent to accessing self.audios, self.phrases, etc. directly,
        but accepts a class reference instead of a fixed attribute name.
        Used by model methods that need to look up instances by class at
        runtime (e.g. get_or_none lookups in Audio, Phrase, Word, etc.).
        '''
        class_name = cls.__name__
        attr = class_name.lower()
        if class_name == 'Audio': attr = 'audios'
        elif class_name == 'Phrase': attr = 'phrases'
        elif class_name == 'Word': attr = 'words'
        elif class_name == 'Syllable': attr = 'syllables'
        elif class_name == 'Phone': attr = 'phones'
        elif class_name == 'Speaker': attr = 'speakers'
        return getattr(self, attr)

    def refresh_query_roots(self):
        '''Invalidate the cached rank-to-keys dict and re-attach query roots.
        Call after adding or removing objects to ensure queries see the
        updated db.
        '''
        if hasattr(self, '_rank_to_keys_dict'):
            del self._rank_to_keys_dict
        self.attach_query_roots()

    def create_audio(self, *args, **kwargs):
        return self._create('Audio', *args, **kwargs)

    def create_phrase(self, *args, **kwargs):
        return self._create('Phrase', *args, **kwargs)

    def create_word(self, *args, **kwargs):
        return self._create('Word', *args, **kwargs)

    def create_syllable(self, *args, **kwargs):
        return self._create('Syllable', *args, **kwargs)

    def create_phone(self, *args, **kwargs):
        return self._create('Phone', *args, **kwargs)

    def create_speaker(self, *args, **kwargs):
        return self._create('Speaker', *args, **kwargs)

    def _create(self, class_name, *args, **kwargs):
        kwargs.setdefault('store', self)
        return self.CLASS_MAP[class_name](*args, **kwargs)

    def attach(self, obj, force=False):
        '''Bind an object to this store.
        force:  allow rebinding an object from a different store
        '''
        existing = getattr(obj, '_store', None)
        if existing is not None and existing is not self and not force:
            message = 'object is already bound to a different Store'
            raise ValueError(message)
        obj._store = self
        return obj

    def _bind(self, obj):
        return self.attach(obj)

    def save(self, obj, overwrite = False, fail_gracefully = False):
        '''save an object to LMDB.
        overwrite: if True, overwrite existing object with same key.
                   if False, raise KeyError if key exists and not 
                        fail_gracefully.
        fail_gracefully : if True, print a message and skip saving if object
                          exists in database
        '''
        if not self.db_saving_allowed: return
        self._bind(obj)
        key = key_helper.instance_to_key(obj)
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
        self._handle_label_links([obj])

    def save_many(self, objs, overwrite = False, fail_gracefully = False):
        if not self.db_saving_allowed: return
        start = time.time()
        objs = list(objs)
        for obj in objs:
            self._bind(obj)
        itk = key_helper.instance_to_key
        pi = struct_value.pack_instance
        keys = [itk(obj) for obj in objs]
        values = [pi(obj) for obj in objs]
        # print('update dict done', time.time() - start)
        try: self.DB.write_many(keys, values,
            overwrite = overwrite)

        except KeyError as e:
            
            print('failed', time.time() - start)
            if fail_gracefully: 
                print(e)
                return
            else: raise e

        # print('succes', time.time() - start)
        cache_update = {key: obj for key, obj in zip(keys, objs)}
        self._cache.update(cache_update)
        for key in cache_update.keys():
            if key not in self.save_key_counter:
                self.save_key_counter[key] = 1
            else: self.save_key_counter[key] += 1
        # print('done', time.time() - start)
        self._handle_label_links(objs)

    def _handle_label_links(self, objs):
        '''after saving objects, write label index links for all objects with
        label_index_key attributes (e.g., Phrase, Word)'''
        label_index_keys = items_to_label_index_keys(objs)
        if label_index_keys:
            self.DB.write_many_label_index_links(label_index_keys)

    def load(self, key):
        '''load an object from LMDB by key.
        key: to load the object from the database.
        '''
        try: return self._bind(self._cache[key])
        except KeyError: pass
        value = self.DB.load(key = key) 
        obj = value_key_to_instance(self, value, key)
        self._bind(obj)
        self._cache[key] = obj
        self.load_counter[obj.object_type] += 1
        return obj

    def load_many(self, keys):
        '''load many objects from LMDB by keys in bulk.
        keys: list of str or bytes to load the objects from the database.
        
        bulk loading is much faster than loading one by one because it
        minimizes LMDB hits.
        garbage collection is disabled if loading more than 100,000 objects
        to speed up loading further
        '''
        if len(keys) == 0: return []
        if len(keys) == 1: return [self.load(keys[0])]
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
                objs[index] = self._bind(self._cache[key])
            else:
                not_found_in_cache.append(key)
        if self.verbose: print(time.time() - start, 'cache checked')

        # if all found in cache or only one not found, return early
        if len(not_found_in_cache) == 0: return objs
        if len(not_found_in_cache) == 1: 
            index = key_to_index[not_found_in_cache[0]]
            objs[index] = self.load(not_found_in_cache[0])
            return objs

        # lmdb loading phase to bulk load all objects not found in cache
        # disable garbage collection for large loads to speed up loading
        if len(not_found_in_cache) > 100_000: gc.disable()
        try:
            results = self.DB.load_many(keys = not_found_in_cache) 
                
            if self.verbose: print(time.time() - start, 'lmdb data loaded')
            for key, value in zip(not_found_in_cache, results):
                index = key_to_index[key]
                obj = value_key_to_instance(self, value, key)
                self._bind(obj)
                self._cache[key] = obj
                objs[index] = obj
                self.load_counter[obj.object_type] += 1
            # print(time.time() - start, 'objs created')
        finally:
            # reenable garbage collection (also in case of error)
            if len(not_found_in_cache) > 100_000: gc.enable()
        if self.verbose: print('gc enabled', time.time() - start)
        return objs

    def label_to_instances(self, label, object_type):
        '''Return all instances of object_type whose label matches label.
        label:       the surface form to look up (e.g. "the")
        object_type: class name string (e.g. "Word", "Phrase")
        Uses the label index written during save, so no full scan is needed.
        Example: store.label_to_instances("the", "Word")
        '''
        keys = list(self.DB.label_to_segment_keys(label, object_type))
        instances = self.load_many(keys)
        return instances

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
            
    def rank_to_keys_dict(self, update = False):
        '''return a dict mapping object_type (class name) to list of keys'''
        if not update:
            if hasattr(self, '_rank_to_keys_dict'):
                return self._rank_to_keys_dict
        d = self.DB.rank_to_keys_dict()
        self._rank_to_keys_dict = d
        return self._rank_to_keys_dict

    def all_keys(self):
        return self.DB.all_keys()

    def all_links(self):
        return self.DB.all_links()

    def preload_class_instances(self,cls = None, class_name = None):
        if cls is None and class_name is None:
            raise ValueError('Either cls or class_name must be provided.')
        if cls is None:
            cls = self.CLASS_MAP[class_name]
        class_name = cls.__name__
        rank = key_helper.CLASS_RANK_MAP[class_name]
        start = time.time()
        class_name = cls.__name__
        if class_name in self._classes_loaded: return
        keys = self.rank_to_keys_dict().get(rank, [])
        self.load_many(keys)
        duration = time.time() - start
        m = f'Loaded all objects of class: {cls.__name__}'
        m += f', in {duration:.2f} seconds.'
        if self.verbose: print(m) 
        self._classes_loaded[class_name] = True

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
        self.enable_writes()

    def turn_off_db_saving(self):
        '''disallow saving to LMDB'''
        self.disable_writes()

    def enable_writes(self):
        '''Allow saving to LMDB.'''
        self.db_saving_allowed = True

    def disable_writes(self):
        '''Disallow saving to LMDB.'''
        self.db_saving_allowed = False

    def is_db_saving_allowed(self):
        '''return True if saving to LMDB is allowed'''
        return self.db_saving_allowed
            
            



def value_key_to_instance(store, value, key):
    '''convert value, key loaded from LMDB to an instance of cls
    this speeds up loading by avoiding __init__ calls
    '''

    info = key_helper.key_to_info(key)
    object_type = info['object_type']
    cls = store.CLASS_MAP[object_type]
    obj = cls.__new__(cls)
    data = struct_value.unpack_instance(object_type, value)
    data.update(info)
    data['_key'] = key
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

def load_phrase_descendants(store, phrases):
    '''
    load all descendants of given parent objects in order of classes.
    all descendants are bulk loaded per class to minimize LMDB hits.
    
    parents: list of root objects
    Returns a dict [class_name] = {'keys':keys, 'instances':objects}.
    '''
    results = {}
    word_keys, syllable_keys, phone_keys = [], [], []  
    for phrase in phrases:
        k = store.DB.instance_to_child_keys(phrase, 'Word')
        if k: word_keys.extend(k)
        k = store.DB.instance_to_child_keys(phrase, 'Syllable')
        if k: syllable_keys.extend(k)
        k = store.DB.instance_to_child_keys(phrase, 'Phone')
        if k: phone_keys.extend(k)
    items=[('Word',word_keys),('Syllable',syllable_keys),('Phone',phone_keys)]
    for child_class_name, child_keys in items:
        instances = store.load_many(child_keys)
        results[child_class_name] = {'keys': child_keys,'instances':instances}
    return results


def load_linked_audio_and_speakers(store, objs):
    '''based on a list of objects with audio_key and speaker_key attributes,
    load all linked audio and speaker objects in bulk.
    '''
    audio_keys   = collect_attribute_keys(objs, 'audio_key')
    speaker_keys = collect_attribute_keys(objs, 'speaker_key')

    # dedupe (optional)
    audio_keys   = list(set(audio_keys))
    speaker_keys = list(set(speaker_keys))

    audios   = store.load_many(audio_keys)
    speakers = store.load_many(speaker_keys)
    d = {}
    d['Audio']   = {'keys': audio_keys,   'instances': audios}
    d['Speaker'] = {'keys': speaker_keys, 'instances': speakers}
    return d


def load_hierarchy_from_phrases(store, phrases):
    '''load words, syllables, phones, audios, and speakers linked to 
    the list of phrases in bulk per class
    this avoids repeated LMDB hits when loading linked objects per phrase
    '''
    instances_dict =  load_phrase_descendants(store, phrases)
    audio_speaker_dict = load_linked_audio_and_speakers(store, phrases)
    instances_dict.update(audio_speaker_dict)
    return instances_dict

def sample_instances_from_class(store, class_name = 'Phrase', fraction = 0.1):
    '''randomly sample a fraction of all objects of the given class'''
    rank = key_helper.CLASS_RANK_MAP[class_name]
    try: keys = store.rank_to_keys_dict()[rank]
    except KeyError: keys = []
    n_sample = max(1, int(len(keys) * fraction))
    sampled_keys = random.sample(keys, n_sample)
    sampled_objects = store.load_many(sampled_keys)
    return {'keys': sampled_keys, 'instances': sampled_objects} 

def items_to_label_index_keys(items):
    label_index_keys = []
    for item in items:
        try: label_index_keys.append(item.label_index_key)
        except AttributeError: pass
    return label_index_keys
