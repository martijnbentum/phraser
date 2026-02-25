import uuid
import cache as cache_module
import lmdb_helper 
import lmdb_key
import model_helper
from model_helper import EMPTY_ID
import query
from ssh_audio_play import play
import struct_value
import time
import utils

R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"

object_type_to_ljust_label = {'Phrase': 40, 'Word': 15, 
    'Syllable': 12, 'Phone': 3}



class Segment:
    IDENTITY_FIELDS= {'label', 'start', 'end', 'audio_key'}
    DB_FIELDS = {'identifier', 'label', 'start', 'end', 'parent_id',
        'parent_start', 'audio_id', 'speaker_id'}
    METADATA_FIELDS = {}# subclasses override
    '''
    Base time-aligned segment with a unique ID and parent/child links.
    '''
    allowed_child_type = []# subclasses override
    overlap_code = 9

    @classmethod
    def get_default_cache(cls):
        if hasattr(cls, 'objects'): 
            return cls.objects.cache

    @classmethod
    def get_or_create(cls, **kwargs):
        lookup = {k: kwargs[k] for k in cls.IDENTITY_FIELDS if k in kwargs}
        if not lookup:
            raise ValueError('No identity fields provided')
        missing = [k for k in cls.IDENTITY_FIELDS if k not in kwargs]
        if missing:
            raise ValueError(f'Missing identity fields: {missing}')
        instance = cls.objects.get_or_none(**lookup)
        if instance is None:
            instance = cls(**kwargs)
            return instance, True
        return instance, False

    @property
    def exists_in_db(self):
        cls = self.__class__
        lookup = {k: getattr(self, k) for k in cls.IDENTITY_FIELDS}
        existing = cls.objects.get_or_none(**lookup)
        return existing is not None

    def __init__(self, label = None, start = None, end = None, 
        parent_id=EMPTY_ID, audio_id= EMPTY_ID, 
        speaker_id= EMPTY_ID, parent_start = 0,  
        save = True, overwrite = False, **kwargs):
        
        self.object_type = self.__class__.__name__
        self.label = label
        self.start = int(start)
        self.end = int(end)
        self.identifier = lmdb_key.make_identifier()

        self.parent_id= parent_id
        self.parent_start = parent_start
        self.audio_id = audio_id
        self.speaker_id = speaker_id
        self.overwrite = overwrite
            

        # Extra metadata
        for k, v in kwargs.items():
            setattr(self, k, v)

        if save: self.save(overwrite=overwrite)
        self._save_status = None

    def __repr__(self):
        n = object_type_to_ljust_label.get(self.object_type, 15)
        if len(self.label) > n: label = self.label[:n-3] + '...'
        else: label = self.label
        m = f'{R}{self.object_type}{RE} '
        m += f'{B}label {RE}{label:<{n}} | '
        m += f'{B}duration {RE}{self.duration} | '
        m += f'{GR}ID {self.identifier.hex()}{RE} '
        return m

    def __str__(self):
        m = self.__repr__() + '\n'
        m += utils.pretty_print_object_dict(self.__dict__)
        return m

    def __eq__(self, other):
        if not isinstance(other, Segment):
            return False
        if self.object_type != other.object_type:
            return False
        if self.object_type == 'Phrase':
            self.filename = other.filename
        for field in self.IDENTITY_FIELDS:
            if getattr(self, field) != getattr(other, field):
                return False
        return True

    def __hash__(self):
        if self.object_type == 'Phrase':
            return hash(self.filename)
        values = tuple(getattr(self, field) for field in self.IDENTITY_FIELDS)
        return hash(values)

    def play(self, collar = None, wait = False):
        if collar is not None:
            if collar < 0: raise ValueError("collar must be non-negative.")
            if collor > self.audio.duration / 2:
                print('Warning: collar is larger than half the audio duration.')
            start = self.start - collar
            if start < 0: start = 0
            end = self.end + collar
            if end > self.audio.duration: end = self.audio.duration
        else: start = self.start; end = self.end
        start = utils.miliseconds_to_seconds(start)
        end = utils.miliseconds_to_seconds(end)
        play.play_audio(self.audio.filename, start=start, end=end, wait = wait)
        m = f'Playing {self.object_type} "{self.label}" '
        m += f'from {start:.2f}s to {end:.2f}s\n'
        m += f'(audio filename={self.audio.filename})'
        print(m)
    
    def play_children(self, collar = None):
        for child in self.children:
            child.play(collar=collar, wait = True)
            time.sleep(0.3)

    @property
    def child_class(self):
        if self.allowed_child_type:
            return self.allowed_child_type
    @property
    def child_class_name(self):
        if self.child_class is None: return None
        return self.child_class.__name__
         

    @property
    def key(self):
        """Return the LMDB key for this segment."""
        if hasattr(self, '_key'): return self._key
        return lmdb_key.instance_to_key(self)

    @property
    def key_info(self):
        return lmdb_key.key_to_info(self.key)

    @property
    def label_index_key(self):
        return lmdb_key.instance_to_label_index_key(self)


    @property
    def parent_class_name(self):
        if self.object_type == 'Phrase': return None
        return self.parent_class.__name__

    @property
    def parent_key(self):
        if self.object_type == "Phrase": return None
        if self.parent_id == EMPTY_ID: return None
        audio_id = self.audio_id
        return lmdb_key.audio_id_segment_id_class_to_key(self.audio_id, 
            self.parent_id, self.parent_class_name, self.parent_start)
            
            
    @property
    def phrase_key(self):
        if self.object_type == "Phrase": return self.key
        if self.object_type == 'Word': return self.parent_key
        if self.phrase_id == EMPTY_ID: return None
        return lmdb_key.audio_id_segment_id_class_to_key(self.audio_id, 
            self.phrase_id, 'Phrase', self.phrase_start)
            

    @property
    def parent(self):
        """Return the parent segment."""
        if self.object_type == "Phrase": return None
        if hasattr(self, '_parent'): return self._parent
        if self.parent_id == EMPTY_ID: return 
        self._parent = cache.load(self.parent_key)
        return self._parent

    @property
    def child_keys(self):
        if self.allowed_child_type is None: 
            self._child_keys = None
        return list(cache.DB.instance_to_child_keys(self))

    @property
    def children(self):
        """Return the list of child segments."""
        if self.allowed_child_type is None: return []
        if hasattr(self, '_children'): return self._children
        self._children, self._related = [], []
        if self.child_keys: 
            children = cache.load_many(self.child_keys) 
                
        else:
            children = []
        sid = self.speaker_id
        for child in children:
            if child.speaker_id == sid: self._children.append(child)
            else: self._related.append(child) 
        return self._children

    @property
    def related(self):
        if hasattr(self, '_related'): return self._related
        if self.allowed_child_type is None: return []
        _ = self.children
        return self._related
        

    @property
    def audio_key(self):
        if self.audio_id == EMPTY_ID: return None
        return lmdb_key.audio_id_to_key(self.audio_id)

    @property
    def audio(self):
        """Return the associated Audio object."""
        if self.audio_key is None: return None
        if hasattr(self, '_audio') and self._audio is not None: 
            return self._audio
        self._audio = cache.load(self.audio_key) 
        return self._audio
        
    @property
    def speaker_key(self):
        if self.speaker_id == EMPTY_ID: return None
        return lmdb_key.speaker_id_to_key(self.speaker_id)

    @property
    def speaker(self):
        """Return the associated Speaker object."""
        if self.speaker_key is None: return None 
        if hasattr(self, '_speaker'): return self._speaker
        self._speaker = cache.load(self.speaker_key)
        return self._speaker

    @property
    def phrase(self):
        if self.object_type == "Phrase": return self
        if self.object_type == 'Word': return self.parent
        if hasattr(self, '_phrase'): return self._phrase
        if self.phrase_key is None: return None
        self._phrase = cache.load(self.phrase_key)
        return self._phrase


    @property
    def duration(self):
        return self.end - self.start
            
    def save(self, overwrite = None, fail_gracefully = False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)

    def add_audio(self, audio, update_database = True, propagate = True):
        ''' Link this segment (and by default its family) to an Audio object.
        '''
        audio_id= audio.identifier
        self._audio = audio
        all_segments = [self]
        # family starts with self
        if propagate: all_segments += list(self.iter_family())[1:]

        for segment in all_segments:
            segment._apply_audio_id(audio_id)

        if update_database:  
            model_helper.write_changes_to_db(all_segments, cache)

    def _apply_audio_id(self, audio_id):
        old_key = self.key
        self.audio_id= audio_id
        new_key = self.key
        if old_key != new_key: 
            self._save_status = 'update'
        self._old_key = old_key

    def add_speaker(self, speaker, update_database = True, propagate = True):
        speaker_id = speaker.identifier
        self._speaker = speaker
        all_segments = [self]
        if propagate:
            all_segments += list(self.iter_family())[1:]
        for segment in all_segments:
            segment._apply_speaker_id(speaker_id, update_database)
        if update_database:  
            model_helper.write_changes_to_db(all_segments, cache)

    def _apply_speaker_id(self, speaker_id, update_database):
        if self.speaker_id == speaker_id: return
        self.speaker_id = speaker_id
            

    # ------------------ hierarchy helpers ------------------
    def add_parent(self, parent, update_database = True):
        if self.object_type == 'Phrase':
            raise TypeError("Phrase cannot have a parent segment.")
        if self.__class__ != parent.allowed_child_type:
            m = f'{parent.object_type} cannot contain '
            m += f'{self.object_type} as child.'
            raise TypeError(m)
        self.parent_id = parent.identifier
        self.parent_start = parent.start
        self._parent = parent
        model_helper.ensure_consistent_link(self, parent, 'audio_id',
            'add_audio', update_database=update_database)
        model_helper.ensure_consistent_link(self, parent, 'speaker_id',
            'add_speaker', update_database=update_database)
        if update_database: self.save(overwrite = True)

    def add_child(self, child, update_database = True):
        child.add_parent(self, update_database=update_database)

    # ------------------ serialization ------------------

    def delete(self):
        cache.delete(self.key)

    def has_extra(self):
        if hasattr(self, 'extra') and self.extra:
            return True
        return False


    def to_struct_value(self):
        '''Serialize to a struct value (for LMDB storage).
        '''
        return struct_value.pack_segment(self)

    @property
    def metadata_present(self):
        names = []
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                names.append(name)
        return names

    @property
    def overlap(self):
        if hasattr(self, 'overlap_code'):
            overlap = utils.reverse_overlap_dict[self.overlap_code]
            if overlap is None: return self.overlap_items != []
            return overlap
        return self.overlap_items != []
        

    @property
    def overlap_items(self):
        if hasattr(self, '_overlap_items'): return self._overlap_items
        if self.object_type == 'Phrase': 
            if not self.audio: return []
            items = self.audio.phrases
        else:
            items =  self.parent.related
        if items is None: return []
        overlapping = []
        for item in items:
            if item == self: continue
            if item.start > self.end: break
            if item.end < self.start: continue
            if utils.overlap(self, item):
                overlapping.append(item)
        self._overlap_items = overlapping
        return self._overlap_items

    @property
    def siblings(self):
        if self.object_type == 'Phrase': 
            return self.audio.phrases
        if self.parent is None: return
        return self.parent.children

    @property
    def next_sibling(self):
        """Return the next segment at the same level (same parent)."""
        siblings = self.siblings
        if siblings is None: return None
        try:
            idx = siblings.index(self)
        except ValueError:
            return None
        if idx + 1 < len(siblings):
            return siblings[idx + 1]
        return None

    @property
    def prev_sibling(self):
        """Return the previous segment at the same level."""
        siblings = self.siblings
        if siblings is None: return None
        try:
            idx = siblings.index(self)
        except ValueError:
            return None

        if idx - 1 >= 0:
            return siblings[idx - 1]
        return None

    def iter_descendants_of_type(self, cls):
        """
        Yield all descendant segments of the given class type.
        Works regardless of depth (e.g. Word → Syllable → Phone).
        """
        for child in self.children:
            if isinstance(child, cls):
                yield child
            # recurse
            yield from child.iter_descendants_of_type(cls)

    def iter_ancestors_of_type(self, cls):
        """
        Yield all ancestor segments of the given class type.
        Walks upward (child → parent → parent → ...).
        """
        if cls == Phrase and self.phrase is not None: yield self.phrase
        parent = self.parent
        while parent is not None:
            if isinstance(parent, cls):
                yield parent
            parent = parent.parent

    @property
    def descendant_keys(self):
        return cache.DB.instance_to_descendant_keys(self)

    @property
    def descendants(self):
        #if hasattr(self, '_descendants'): return self._descendants
        keys = self.descendant_keys
        return cache.load_many(keys)


    def iter_descendants(self):
        for child in self.children:
            yield child
            yield from child.iter_descendants()

    def iter_ancestors(self):
        parent = self.parent
        while parent is not None:
            yield parent
            parent = parent.parent

    def iter_family(self):
        yield self
        yield from self.iter_ancestors()
        yield from self.iter_descendants()



class Phrase(Segment):
    METADATA_FIELDS = {'filename','language', 'speech_style', 
        'channel_index', 'overlap','version'}
    filename = ''

    @property
    def all_objects(self):
        objs = [self]
        if self.words: objs += self.words 
        if self.syllables: objs += self.syllables
        if self.phones: objs += self.phones 
        return objs

    @property
    def all_keys(self):
        keys = [x.key for x in self.all_objects]
        return keys

    def delete(self, do_reconnect_db = True):
        """ Delete this phrase and all its descendants from the database.
        """
        all_keys = self.all_keys
        cache.delete_many(all_keys)
        if do_reconnect_db:
            reconnect_db()

    @property
    def phrase_start(self):
        return self.start

    @property
    def phrase_id(self):
        return self.identifier

    @property
    def words(self):
        """Return all words in this phrase."""
        return self.children

    @property
    def syllables(self):
        """Return all syllables in this phrase."""
        return list(self.iter_descendants_of_type(Syllable))

    @property
    def phones(self):
        """Return all phones in this phrase."""
        return list(self.iter_descendants_of_type(Phone))

    @property
    def words_query(self):
        """Return a query object for all words for this speaker."""
        return query.queryset_from_items(self.words, cache)
    @property
    def syllables_query(self):
        """Return a query object for all syllables for this speaker."""
        return query.queryset_from_items(self.syllables, cache)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, cache)


    def apply_phrase_id_and_start(self, update_database = True):
        for syllable in self.syllables:
            syllable._add_phrase(self, update_database=update_database)
        for phone in self.phones:
            phone._add_phrase(self, update_database=update_database)

        


class Word(Segment):
    METADATA_FIELDS = {'pos', 'overlap', 'sos',
        'eos','freq', 'ipa'}
    ipa = ''

    @property
    def phrase_start(self):
        return self.parent_start

    @property
    def phrase_id(self):
        return self.parent_id

    @property
    def syllables(self):
        """Return all syllables in this word."""
        return list(self.iter_descendants_of_type(Syllable))

    @property
    def phones(self):
        """Return all phones in this word."""
        return list(self.iter_descendants_of_type(Phone))

    @property
    def syllables_query(self):
        """Return a query object for all syllables for this speaker."""
        return query.queryset_from_items(self.syllables, cache)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, cache)

def _add_phrase(self, phrase, update_database = True):
    if self.phrase_id == phrase.identifier: return
    if self.phrase_id != EMPTY_ID and self.phrase_id != phrase.identifier:
        m = f"This {self.object_type} is already linked to a different phrase."
        raise ValueError(m)
    self.phrase_id = phrase.identifier
    self.phrase_start = phrase.start
    if update_database: self.save(overwrite = True)


class Syllable(Segment):
    METADATA_FIELDS = {'stress_code', 'tone'}
    phrase_id = EMPTY_ID
    phrase_start = 0
    stress_code = 9

    _add_phrase = _add_phrase


    @property
    def stress(self):
        if self.stress_code == 0: return 'unstressed'
        if self.stress_code == 1: return 'primary'
        if self.stress_code == 2: return 'secondary'
        return 'unknown'

    @property
    def phones(self):
        """Return all phones in this syllable."""
        return self.children

    @property
    def word(self):
        """Return the parent word of this syllable."""
        return self.parent

    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, cache)

class Phone(Segment):
    METADATA_FIELDS = {'features'}

    phrase_id = EMPTY_ID
    phrase_start = 0

    _add_phrase = _add_phrase

    @property
    def position_code(self):
        if hasattr(self, 'position'):
            if self.position == 'onset': return 1
            if self.position == 'nucleus': return 2
            if self.position == 'coda': return 3
        return 9

    @property
    def syllable(self):
        """Return the parent syllable of this phone."""
        return self.parent

    @property
    def word(self):
        """Return the parent word of this phone."""
        if self.parent is None:
            return None
        if self.parent.object_type == "Word":
            return self.parent
        return self.parent.parent


class Audio:
    IDENTITY_FIELDS= {'filename'}
    METADATA_FIELDS = {'sample_rate', 'duration', 'n_channels', 'dataset',
        'language', 'dialect'}
    DB_FIELDS = {'filename', 'identifier'}

    dialect = ''
    language = ''
    dataset = ''
    n_channels = 0
    sample_rate = 0

    @classmethod
    def get_default_cache(cls):
        if hasattr(cls, 'objects'): 
            return cls.objects.cache

    @property
    def exists_in_db(self):
        cls = self.__class__
        lookup = {k: getattr(self, k) for k in cls.IDENTITY_FIELDS}
        existing = cls.objects.get_or_none(**lookup)
        return existing is not None
        
    def __init__(self, filename = None,  save=True, overwrite=False, **kwargs):
        self.object_type = self.__class__.__name__
        self.filename = filename
        self.identifier = lmdb_key.make_identifier()
        self.overwrite = overwrite

        self._set_kwargs(**kwargs)

        if save:
            self.save(overwrite=overwrite)

    def __repr__(self):
        if len(self.filename) > 20: 
            filename = self.filename.split('/')[-1] 
            if len(filename) > 20: filename = '...' + filename[-17:]
        else: filename = self.filename
        m = f'{R}Audio{RE} {B}filename {RE}{filename} '
        if hasattr(self, 'duration'):
            m += f'{B}duration {RE}{self.duration} | '
        m += f'{GR}ID={self.identifier.hex()}{RE}'
        return m

    def __eq__(self, other):
        if not isinstance(other, Audio):
            return False
        return self.filename == other.filename

    def __hash__(self):
        return hash(self.filename)

    def _set_kwargs(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def add_speaker(self, speaker):
        speaker.add_audio(self)

    def has_extra(self):
        if hasattr(self, 'extra') and self.extra:
            return True
        return False


    @property
    def speakers(self):
        if hasattr(self, '_speakers'): return self._speakers
        speakers = [x.speaker for x in self.phrases]
        unique_speakers = set(speakers)
        self._speakers = list(unique_speakers)
        return self._speakers

    @property
    def phrase_keys(self):
        if hasattr(self, '_phrase_keys'): return self._phrase_keys
        self._phrase_keys = list(cache.DB.audio_id_to_child_keys(
            self.identifier))
        return self._phrase_keys

    @property
    def phrases(self):
        if hasattr(self, '_phrases'): return self._phrases
        self._phrases= cache.load_many(self.phrase_keys)
        return self._phrases

    @property
    def words(self):
        """Return all words across all phrases for this speaker."""
        words = []
        for phrase in self.phrases:
            for word in phrase.words:
                words.append(word)
        return words

    @property
    def syllables(self):
        """Return all syllables across all phrases for this speaker."""
        syllables = []
        for phrase in self.phrases:
            for syllable in phrase.syllables:
                syllables.append(syllable)
        return syllables

    @property
    def phones(self):
        """Return all phones across all phrases for this speaker."""
        phones = []
        for phrase in self.phrases:
            for phone in phrase.phones:
                phones.append(phone)
        return phones
    
    @property
    def speakers_query(self):
        """Return a query object for all phrases for this speaker."""
        return query.queryset_from_items(self.phrases, cache)
    @property
    def words_query(self):
        """Return a query object for all words for this speaker."""
        return query.queryset_from_items(self.words, cache)
    @property
    def syllables_query(self):
        """Return a query object for all syllables for this speaker."""
        return query.queryset_from_items(self.syllables, cache)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, cache)
    
    @property
    def key(self):
        """Return the LMDB key for this segment."""
        return lmdb_key.instance_to_key(self)

    def save(self, overwrite=None, fail_gracefully=False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)



class Speaker:
    IDENTITY_FIELDS= {'name', 'dataset'}
    DB_FIELDS = {'name', 'dataset','identifier'}
    METADATA_FIELDS = {'gender', 'age', 'language', 'dialect', 'region', 
        'channel'}
    FIELDS = DB_FIELDS.union(METADATA_FIELDS)

    gender_code = 9
    age = 0
    dataset = ''
    dialect = ''
    region = ''
    language = ''

    @classmethod
    def get_default_cache(cls):
        if hasattr(cls, 'objects'): 
            return cls.objects.cache

    @property
    def exists_in_db(self):
        cls = self.__class__
        lookup = {k: getattr(self, k) for k in cls.IDENTITY_FIELDS}
        existing = cls.objects.get_or_none(**lookup)
        return existing is not None
        
    def __init__(self, name =None, dataset = None, save=True, overwrite=False, 
        **kwargs):
        self.object_type = self.__class__.__name__
        self.name = name
        self.dataset = dataset
        self.identifier = lmdb_key.make_identifier()
        self.overwrite = overwrite

        # Extra metadata
        extra = {}
        if 'extra' in kwargs:
            e = kwargs.pop('extra')
            if not isinstance(v, dict):
                raise ValueError("extra must be a dict")
            extra.update( e )
        for k, v in kwargs.items():
            if k in self.FIELDS:
                setattr(self, k, v)
            elif not k in extra: 
                extra[k] = v
        self.extra = extra

        if save:
            self.save(overwrite=overwrite)

    def __repr__(self):
        if len(self.name) > 12: name = self.name[:9] + '...'
        else: name = self.name
            
        m = f'{R}Speaker{RE} {B}name {RE}{name:<20} | '
        m += f'{GR}ID={self.identifier.hex()}{RE}'
        return m

    def __eq__(self, other):
        if not isinstance(other, Speaker):
            return False
        return self.identifier == other.identifier

    def __hash__(self):
        return hash(self.identifier)

    def __contains__(self, key):
        return hasattr(self, key) or key in self.extra

    def add_audio(self, audio):
        if not hasattr(self, '_audios'): self._audios = []
        if audio not in self._audios:
            self._audios.append(audio)
        cache.DB.write_speaker_audio_link(self, audio) 

    def has_extra(self):
        if hasattr(self, 'extra') and self.extra:
            return True
        return False

    def gender(self):
        return utils.reverse_gender_dict[self.gender_code]

    def overlap(self):
        return utils.reverse_overlap_dict[self.overlap_code]

    @property
    def audios(self):
        if hasattr(self, '_audios'): return self._audios 
        audio_keys = cache.DB.speaker_to_audio_keys(self)
        self._audios = cache.load_many(audio_keys)
        return self._audios

    @property
    def phrases(self):
        """Return all phrases across all audios for this speaker."""
        if hasattr(self, '_phrases'): return self._phrases 
        self._phrases = cache.load_many(self.phrase_keys)
        return self._phrases

    @property
    def words(self):
        """Return all words across all phrases for this speaker."""
        words = []
        for phrase in self.phrases:
            for word in phrase.words:
                words.append(word)
        return words

    @property
    def syllables(self):
        """Return all syllables across all phrases for this speaker."""
        syllables = []
        for phrase in self.phrases:
            for syllable in phrase.syllables:
                syllables.append(syllable)
        return syllables

    @property
    def phones(self):
        """Return all phones across all phrases for this speaker."""
        phones = []
        for phrase in self.phrases:
            for phone in phrase.phones:
                phones.append(phone)
        return phones
    
    @property
    def phrases_query(self):
        """Return a query object for all phrases for this speaker."""
        return query.queryset_from_items(self.phrases, cache)
    @property
    def words_query(self):
        """Return a query object for all words for this speaker."""
        return query.queryset_from_items(self.words, cache)
    @property
    def syllables_query(self):
        """Return a query object for all syllables for this speaker."""
        return query.queryset_from_items(self.syllables, cache)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, cache)

    @property
    def key(self):
        """Return the LMDB key for this segment."""
        return lmdb_key.instance_to_key(self)
            
    def save(self, overwrite=None, fail_gracefully=False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)

    def delete(self, do_reconnect_db = True):
        cache.delete(self.key)
        if do_reconnect_db:
            reconnect_db()

    @property
    def metadata_present(self):
        names = []
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                names.append(name)
        return names



Phrase.allowed_child_type = Word
Word.allowed_child_type = Syllable
Word.parent_class = Phrase
Syllable.allowed_child_type = Phone
Syllable.parent_class = Word
Phone.allowed_child_type = None
Phone.parent_class = Syllable


def load_cache(fraction = None):
    global cache
    t = time.time()
    cache = cache_module.Cache()
    print(f'cache created in {time.time() - t:.2f} seconds')

    cache.register(Audio)
    print(f'Audio registered in {time.time() - t:.2f} seconds')
    cache.register(Phrase)
    cache.register(Word)
    cache.register(Syllable)
    cache.register(Phone)
    cache.register(Speaker)
    print(f'Classes registered in {time.time() - t:.2f} seconds')


    Audio.objects = query.get_class_object(Audio, cache)
    print(f'Audio.objects created in {time.time() - t:.2f} seconds')
    Phrase.objects = query.get_class_object(Phrase, cache)
    Word.objects = query.get_class_object(Word, cache)
    Syllable.objects = query.get_class_object(Syllable, cache)
    Phone.objects = query.get_class_object(Phone, cache)
    Speaker.objects = query.get_class_object(Speaker, cache)
    print(f'Class objects created in {time.time() - t:.2f} seconds')

    cache.relations_to_class_map = {
        'audios': Audio, 
        'speakers': Speaker,
        'phrases': Phrase,
        'words': Word,
        'syllables': Syllable,
        'phones': Phone,
    }
    print(f'relations_to_class_map created in {time.time() - t:.2f} seconds')
    if fraction is not None:
        cache._preload_sampled_fraction(fraction)
    print(f'Cache loaded in {time.time() - t:.2f} seconds')
            
load_cache()

def touch_db():
    load_cache()

def reconnect_db():
    load_cache()

def turn_off_db_saving():
    cache.turn_off_db_saving()

def turn_on_db_saving():
    cache.turn_on_db_saving()

