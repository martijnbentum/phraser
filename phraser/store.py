import gc
import pickle
import random
import time

from . import key_helper
from . import lmdb_helper
from . import locations
from . import struct_value
from . import utils
from .struct_helper import CLASS_RANK_MAP, RANK_CLASS_MAP

R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"


class UnboundStoreError(RuntimeError):
    pass


class ClosedStoreError(RuntimeError):
    pass


class Store:
    """
    Barebones LMDB-backed store with:
    - safe caching
    - no nested pickles
    - resolver for parent, children, speaker, audio

    Query roots such as store.words are snapshots for the read/query phase.
    Write/build first, then call refresh_query_roots() or reopen the store
    before relying on store-level query roots.
    """

    def __init__(self, path = locations.cgn_lmdb, fraction = None,
        verbose = False):
        t = time.time()
        self.DB = lmdb_helper.DB(path = path)
        self.path = path
        self._cache = {}      # key:str → object
        self.CLASS_MAP = {}
        self.save_counter = {}
        self.load_counter = {}
        self.save_key_counter = {}
        self.verbose = verbose
        self._classes_loaded = {}
        self.fraction = None
        self.closed = False
        self._register_default_classes()
        if fraction is not None:
            self._preload_sampled_fraction(fraction)
        print(f'Store loaded in {time.time() - t:.2f} seconds')

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
            m += f'construct {R}Store(path){RE} to gain full access to db\n'
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

    def register_all(self, *classes):
        '''Register several classes and (re)attach query roots in one call.'''
        for cls in classes:
            self.register(cls)
        self.attach_query_roots()

    def _register_default_classes(self):
        '''Register the domain classes listed in CLASS_RANK_MAP and attach
        query roots. The models import is deferred to avoid an import cycle
        (models imports store at module load; see attach_query_roots for the
        same pattern with query).
        '''
        from . import models
        classes = [getattr(models, name) for name in CLASS_RANK_MAP]
        self.register_all(*classes)

    def attach_query_roots(self):
        '''Attach a store-scoped query root for each registered class and
        build relations_to_class_map (plural attr name -> class).
        Creates self.audios, self.phrases, etc. as Query objects bound to this
        store. Called by register_all and by refresh_query_roots when the db
        changes.
        '''
        from . import query
        self.relations_to_class_map = {}
        self._query_roots = {}
        for class_name, cls in self.CLASS_MAP.items():
            attr = class_name.lower() + 's'  # Audio->audios, Phrase->phrases
            root = query.get_class_object(cls, self)
            setattr(self, attr, root)
            self.relations_to_class_map[attr] = cls
            self._query_roots[cls] = root

    def query_for_class(self, cls):
        '''Return the store-scoped Query object for the given class.
        Equivalent to accessing self.audios, self.phrases, etc. directly,
        but accepts a class reference instead of a fixed attribute name.
        Used by model methods that need to look up instances by class at
        runtime (e.g. get_or_none lookups in Audio, Phrase, Word, etc.).
        '''
        return self._query_roots[cls]

    def refresh_query_roots(self):
        '''Invalidate the cached rank-to-keys dict and re-attach query roots.
        This is the explicit boundary between a write/build phase and a
        read/query phase. Saves and deletes intentionally do not update query
        roots live.
        '''
        if hasattr(self, '_rank_to_keys_dict'):
            del self._rank_to_keys_dict
        self.attach_query_roots()

    def create(self, cls, **kwargs):
        '''Create an instance of cls bound to this store.
        e.g. store.create(Audio, filename='x.wav')
        '''
        kwargs.setdefault('store', self)
        return cls(**kwargs)

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

    def _validate_for_save(self, obj):
        validate = getattr(obj, '_validate_for_save', None)
        if validate is not None: validate()

    def save(self, obj, overwrite = False, fail_gracefully = False):
        '''save an object to LMDB.
        overwrite: if True, overwrite existing object with same key.
                   if False, raise KeyError if key exists and not 
                        fail_gracefully.
        fail_gracefully : if True, print a message and skip saving if object
                          exists in database
        '''
        self._ensure_open()
        self._validate_for_save(obj)
        self._bind(obj)
        key = key_helper.instance_to_key(obj)
        value = struct_value.pack_instance(obj)
        fail_message = f"Object with key {key} already exists. "
        fail_message += "Skipping save."
        try: self.DB.write(key = key, value = value, overwrite = overwrite)
            
        except KeyError as e:
            if fail_gracefully:
                print(fail_message)
                return
            else: raise e
        obj._key = key
        self._cache[key] = obj
        self.save_counter[obj.object_type] += 1
        if key not in self.save_key_counter:
            self.save_key_counter[key] = 1
        else: self.save_key_counter[key] += 1
        self._handle_label_links([obj])

    def save_many(self, objs, overwrite = False, fail_gracefully = False):
        self._ensure_open()
        start = time.time()
        objs = list(objs)
        for obj in objs:
            self._validate_for_save(obj)
        for obj in objs:
            self._bind(obj)
        itk = key_helper.instance_to_key
        pi = struct_value.pack_instance
        keys = [itk(obj) for obj in objs]
        self._check_intra_batch_keys(objs, keys)
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
        for obj, key in zip(objs, keys):
            obj._key = key
        cache_update = {key: obj for key, obj in zip(keys, objs)}
        self._cache.update(cache_update)
        for key in cache_update.keys():
            if key not in self.save_key_counter:
                self.save_key_counter[key] = 1
            else: self.save_key_counter[key] += 1
        # print('done', time.time() - start)
        self._handle_label_links(objs)

    def _check_intra_batch_keys(self, objs, keys):
        '''Reject duplicate keys within one batch. DB.write_many only
        checks keys against the database; within a transaction a
        repeated key silently keeps the last value written, so one
        object would overwrite the other without an error.'''
        seen = {}
        for obj, key in zip(objs, keys):
            other = seen.get(key)
            if other is None:
                seen[key] = obj
                continue
            m = 'duplicate key in batch: '
            m += f'{type(obj).__name__} {obj.identifier.hex()} '
            m += 'appears twice (the same object listed twice, or two '
            m += 'loaded copies of one persisted object); written nothing.'
            raise ValueError(m)

    def save_phrase_trees(self, phrases, overwrite = False):
        '''Persist staged phrase trees (each phrase and its descendants).

        phrases:    Phrase objects with staged, linked descendants
        overwrite:  if True, overwrite existing objects with same keys

        Every tree is validated via Phrase.validate_tree (one speaker
        across the whole tree, no persisted segment changing its
        audio), no two phrases in the batch may share the same
        (audio_id, speaker_id, start) identity, and no phrase may
        overlap a same-speaker phrase, in the batch or already
        persisted on the same audio (a phrase's own persisted row is
        exempt, so overwrite re-saves pass). The trees are flattened
        via Phrase.items and written through save_many; nothing is
        written when any validation fails.
        '''
        phrases = list(phrases)
        if not phrases: return
        self._validate_phrase_trees(phrases)
        segments = []
        for phrase in phrases:
            segments.extend(phrase.items)
        self.save_many(segments, overwrite = overwrite)

    def _validate_phrase_trees(self, phrases):
        from .models import Phrase
        seen = set()
        for phrase in phrases:
            if not isinstance(phrase, Phrase):
                m = 'save_phrase_trees expects Phrase objects, '
                m += f'got {type(phrase).__name__}.'
                raise TypeError(m)
            phrase.validate_tree()
            if phrase in seen:
                m = 'duplicate phrase identity in batch: '
                m += f'(audio_id, speaker_id, start) = ({phrase.audio_id}, '
                m += f'{phrase.speaker_id}, {phrase.start})'
                raise ValueError(m)
            seen.add(phrase)
        self._check_same_speaker_overlap(phrases)

    def _check_same_speaker_overlap(self, phrases):
        '''One speaker, one phrase at a time: reject a phrase that
        overlaps a same-speaker phrase, in the batch or already
        persisted on the same audio. A phrase's own persisted row
        (identical key) is exempt, so overwrite re-saves pass. Two
        persisted rows overlapping each other are legacy data and do
        not block an unrelated save.'''
        groups = {}
        for phrase in phrases:
            group_key = (phrase.audio_id, phrase.speaker_id)
            groups.setdefault(group_key, []).append(phrase)
        persisted = self._persisted_phrases_by_group(groups)
        for group_key, group in groups.items():
            entries = [(p, True) for p in group]
            entries += [(p, False) for p in persisted.get(group_key, [])]
            entries.sort(key=lambda entry: entry[0].start)
            widest, widest_in_batch = entries[0]
            for phrase, in_batch in entries[1:]:
                overlaps = phrase.start < widest.end
                if overlaps and (in_batch or widest_in_batch):
                    m = 'same-speaker overlapping phrases: '
                    m += f'{phrase.label!r} [{phrase.start}, {phrase.end}] '
                    m += f'overlaps {widest.label!r} '
                    m += f'[{widest.start}, {widest.end}]; written nothing.'
                    raise ValueError(m)
                if phrase.end > widest.end:
                    widest, widest_in_batch = phrase, in_batch

    def _persisted_phrases_by_group(self, groups):
        '''Load the persisted phrases that could overlap the batch,
        grouped by (audio_id, speaker_id). Only phrases starting
        before the audio's last batch end can overlap (keys scan in
        start order); the batch phrases' own rows are skipped by key.'''
        own_keys = set()
        max_end_by_audio = {}
        for (audio_id, _), group in groups.items():
            for phrase in group:
                own_key = key_helper.instance_to_key(phrase)
                own_keys.add(own_key)
            end = max(phrase.end for phrase in group)
            if end > max_end_by_audio.get(audio_id, 0):
                max_end_by_audio[audio_id] = end
        persisted = {}
        for audio_id, max_end in max_end_by_audio.items():
            keys = []
            for key in self.DB.audio_id_to_child_keys(audio_id, 'Phrase'):
                if key_helper.key_to_start(key) >= max_end: break
                if key in own_keys: continue
                keys.append(key)
            for phrase in self.load_many(keys):
                group_key = (audio_id, phrase.speaker_id)
                persisted.setdefault(group_key, []).append(phrase)
        return persisted

    def _handle_label_links(self, objs):
        '''after saving objects, write label index links for all objects with
        label_index_key attributes (e.g., Phrase, Word)'''
        label_index_keys = items_to_label_index_keys(objs)
        if label_index_keys:
            self.DB.write_many_label_index_links(label_index_keys)

    def get_cached(self, key):
        '''Return the already-loaded object for key, or None.
        Never reads the database.
        '''
        return self._cache.get(key)

    def load(self, key):
        '''load an object from LMDB by key.
        key: to load the object from the database.
        '''
        self._ensure_open()
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
        self._ensure_open()
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
        self._ensure_open()
        keys = list(self.DB.label_to_segment_keys(label, object_type))
        instances = self.load_many(keys)
        return instances

    def delete(self, key):
        '''delete an object from LMDB by key'''
        self._ensure_open()
        self.DB.delete(key = key)
        if key in self._cache: del self._cache[key]

    def delete_many(self, keys):
        '''delete many objects from LMDB by keys'''
        self._ensure_open()
        self.DB.delete_many(keys = keys)
        for key in keys:
            if key in self._cache: del self._cache[key]

    def update(self, old_key, obj):
        '''delete old_key and save obj with new key'''
        self._validate_for_save(obj)
        self.delete(old_key)
        self.save(obj, overwrite=True)
        if old_key in self._cache: del self._cache[old_key]
            
    def rank_to_keys_dict(self, update = False):
        '''return a dict mapping object_type (class name) to list of keys'''
        if not update:
            if hasattr(self, '_rank_to_keys_dict'):
                return self._rank_to_keys_dict
        self._ensure_open()
        d = self.DB.rank_to_keys_dict()
        self._rank_to_keys_dict = d
        return self._rank_to_keys_dict

    def all_keys(self):
        self._ensure_open()
        return self.DB.all_keys()

    def all_links(self):
        self._ensure_open()
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

    def open(self):
        '''(Re)open the underlying LMDB environment.
        Use after close() to make the store usable again.
        '''
        self.DB.open()
        self.closed = False

    def close(self):
        '''Close the underlying LMDB environment and clear the cache.
        After closing, the store can no longer load or save objects.
        '''
        self.DB.close()
        self._cache.clear()
        self.closed = True

    def is_open(self):
        '''return True if the store's LMDB env is open'''
        return not self.closed

    def _ensure_open(self):
        '''raise ClosedStoreError if the store has been closed'''
        if self.closed:
            raise ClosedStoreError(
                'Store is closed. Call store.open() to reopen it.')

    def __del__(self):
        '''Close the LMDB environment when the store is garbage collected.'''
        try: self.close()
        except Exception: pass
            
            



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
