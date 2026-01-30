import uuid
import lmdb_helper 
import cache as cache_module
import lmdb_key
import model_helper
import query

R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"

object_type_to_ljust_label = {'Phrase': 40, 'Word': 15, 
    'Syllable': 12, 'Phone': 3}


class Segment:
    IDENTITY_FIELDS= {'label', 'start', 'end', 'audio_key'}
    DB_FIELDS = {'identifier', 'label', 'start', 'end', 'parent_key',
        'child_keys', 'audio_key', 'speaker_key'}
    METADATA_FIELDS = {}# subclasses override
    '''
    Base time-aligned segment with a unique ID and parent/child links.
    '''
    allowed_child_types = []# subclasses override

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

    def __init__(self, label = None, start = None, end = None, 
        parent_key=None, child_keys=None, audio_key = 'EMPTY', 
        speaker_key = 'EMPTY', save = True, overwrite = False, **kwargs):
        
        self.object_type = self.__class__.__name__
        self.label = label
        self.start = float(start)
        self.end = float(end)
        self.identifier = lmdb_key.make_item_identifier(self)

        self.parent_key = parent_key
        self.child_keys = child_keys or []
        self.audio_key = audio_key
        self.speaker_key = speaker_key
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
        m += f'{B}duration {RE}{self.duration:.3f} | '
        m += f'{GR}ID {self.identifier}{RE} '
        return m

    def __eq__(self, other):
        if not isinstance(other, Segment):
            return False
        return self.key == other.key and \
            self.audio_key == other.audio_key and \
            self.object_type == other.object_type


    @property
    def key(self):
        """Return the LMDB key for this segment."""
        return lmdb_key.item_to_key(self)

    @property
    def parent(self):
        """Return the parent segment."""
        if hasattr(self, '_parent'): return self._parent
        if self.parent_key is None:return None
        self._parent = cache.load(self.parent_key, with_links=False)
        return self._parent

    @property
    def children(self):
        """Return the list of child segments."""
        if not hasattr(self, '_children'):
            if self.child_keys: 
                self._children = cache.load_many(self.child_keys, 
                    with_links=False)
            else:
                self._children = []
        return self._children

    @property
    def audio(self):
        """Return the associated Audio object."""
        if self.audio_key is None or self.audio_key == 'EMPTY': return None
        if hasattr(self, '_audio'): return self._audio
        self._audio = cache.load(self.audio_key, with_links=False)
        return self._audio
        
    @property
    def speaker(self):
        """Return the associated Speaker object."""
        if self.speaker_key is None or self.speaker_key == 'EMPTY': return None
        if hasattr(self, '_speaker'): return self._speaker
        self._speaker = cache.load(self.speaker_key, with_links=False)
        return self._speaker

    @property
    def phrase(self):
        if self.object_type == "Phrase": return self
        if hasattr(self, '_phrase'): return self._phrase
        for segment in self.iter_ancestors():
            if segment.object_type == "Phrase":
                self._phrase = segment
                return self._phrase

    @property
    def duration(self):
        return self.end - self.start
            
    def save(self, overwrite = None, fail_gracefully = False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)

    def add_audio(self, audio = None, audio_key = None, reverse_link = True,
        update_database = True, propagate = True):
        ''' Link this segment (and by default its family) to an Audio object.
        '''
        if audio is None and audio_key is None:
            raise ValueError("Provide audio or audio_key.")

        if audio_key is None:
            audio_key = audio.key

        self._audio = audio
        all_segments = [self]

        # family starts with self
        if propagate: all_segments += list(self.iter_family())[1:]

        for segment in all_segments:
            segment._apply_audio_key(audio_key)
        for segment in all_segments:
            model_helper.fix_references(segment, segment._old_key, segment.key)

        if update_database:  
            model_helper.write_changes_to_db(all_segments, cache)

        if reverse_link and audio is not None:
            if self.object_type == 'Phrase':
                audio.add_phrase(self, reverse_link=False, 
                    update_database=update_database)

        '''
        if update_database and old_key != new_key:
            cache.update(old_key, self)
        '''

    def _apply_audio_key(self, audio_key):
        old_key = self.key
        self.audio_key = audio_key
        new_key = self.key
        if old_key != new_key: 
            self._save_status = 'update'
        self._old_key = old_key

    def add_speaker(self, speaker = None, speaker_key = None, 
        reverse_link = True, update_database = True, propagate = True):
        if speaker is None and speaker_key is None:
            raise ValueError("Either speaker or speaker_key must be provided.")
        if speaker_key is None:
            speaker_key = speaker.key
        self.speaker_key = speaker_key
        self._speaker = speaker
        all_segments = [self]
        if propagate:
            all_segments += list(self.iter_family())[1:]
        for segment in all_segments:
            segment._apply_speaker_key(speaker_key, update_database)
        if reverse_link and speaker is not None:
            if self.object_type == 'Phrase':
                speaker.add_phrase(self, reverse_link=False, 
                    update_database=update_database)

    def _apply_speaker_key(self, speaker_key, update_database):
        if self.speaker_key == speaker_key: return
        self.speaker_key = speaker_key
        if update_database: self.save(overwrite = True)
            

    # ------------------ hierarchy helpers ------------------

    def add_child(self, child= None, child_key = None, reverse_link = True,
        update_database = True):
        """    Add a child segment, enforcing the declared hierarchy.
        """
        if self.allowed_child_types is []:
            raise TypeError(f"{self.object_type} cannot have children.")
        if child is None and child_key is None:
            raise ValueError("Either child or child_key must be provided.")
        if child_key is None:
            child_key = child.key
        if child_key in self.child_keys: return
        if child is None:
            child = cache.load(child_key, with_links=True)
        if not child.__class__ in self.allowed_child_types:
            m = f'{self.object_type} can only contain {self.allowed_child_types},'
            m += f'not {child.__class__}'
            raise TypeError(m)
        model_helper.ensure_consistent_link(self, child, 'audio_key',
            'add_audio', update_database=update_database)
        model_helper.ensure_consistent_link(self, child, 'speaker_key',
            'add_speaker', update_database=update_database)
        self.child_keys.append(child.key)
        if not hasattr(self, '_children'):
            self._children = []
        self._children.append(child)
        if reverse_link:
            child.add_parent(self, reverse_link=False, 
                update_database=update_database)
        if update_database: self.save(overwrite = True)

    def add_parent(self, parent = None, parent_key = None, reverse_link = True,
        update_database = True):
        if self.object_type == 'Phrase':
            raise TypeError("Phrase cannot have a parent segment.")
        if parent is None and parent_key is None:
            raise ValueError("Either parent or parent_key must be provided.")
        if parent_key is None:
            parent_key = parent.key
        if parent is None:
            parent = cache.load(parent_key, with_links=True)
        if self.__class__ not in parent.allowed_child_types:
            m = f'{parent.object_type} cannot contain '
            m += f'{self.object_type} as child.'
            raise TypeError(m)
        self.parent_key = parent_key
        self._parent = parent
        model_helper.ensure_consistent_link(self, parent, 'audio_key',
            'add_audio', update_database=update_database)
        model_helper.ensure_consistent_link(self, parent, 'speaker_key',
            'add_speaker', update_database=update_database)
        if reverse_link:
            parent.add_child(self, reverse_link=False, 
                update_database=update_database)
        if update_database: self.save(overwrite = True)
        

    # ------------------ serialization ------------------

    def delete(self):
        cache.delete(self.key)

    def to_dict(self):
        """
        Serialize to a clean dict (for LMDB storage).
        """
        base = {}
        for name in self.DB_FIELDS:
            base[name] = getattr(self, name)
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                base[name] = getattr(self, name)

        # Extra metadata
        reserved = set(base.keys()) | {'overwrite','extra', 'object_type',
            'children', 'parent','audio', 'speaker'}
        extra = {} 
        if hasattr(self, 'extra'):
            extra.update(self.extra)
        for k, v in self.__dict__.items():
            if k.startswith('_'): continue
            if k in reserved: continue
            if k in extra: continue
            extra[k] = v
        base['extra'] = extra
        return base

    @property
    def metadata_present(self):
        names = []
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                names.append(name)
        return names

    @property
    def next_sibling(self):
        """Return the next segment at the same level (same parent)."""
        if self.parent is None:
            return None

        siblings = self.parent.children
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
        if self.parent is None:
            return None

        siblings = self.parent.children
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
        parent = self.parent
        while parent is not None:
            if isinstance(parent, cls):
                yield parent
            parent = parent.parent

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
    METADATA_FIELDS = {'language', 'speech_style', 'channel_index', 'overlap',
        'version'}

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


class Word(Segment):
    METADATA_FIELDS = {'pos', 'overlap', 'sos',
        'eos','freq', 'ipa'}

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

class Syllable(Segment):
    METADATA_FIELDS = {'stress', 'stress_level','tone'}

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
    METADATA_FIELDS = {'sample_rate', 'duration', 'n_channels', 'dataset',
        'language', 'dialect'}
    DB_FIELDS = {'filename', 'identifier','speaker_keys', 'phrase_keys'}

    @classmethod
    def get_default_cache(cls):
        if hasattr(cls, 'objects'): 
            return cls.objects.cache
        
    def __init__(self, filename = None,  save=True, overwrite=False, **kwargs):
        self.object_type = self.__class__.__name__
        self.filename = filename
        self.identifier = lmdb_key.make_item_identifier(self)
        self.overwrite = overwrite

        self.speaker_keys = []
        self.phrase_keys = []
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
            m += f'{B}duration {RE}{self.duration:.1f} | '
        m += f'{GR}ID={self.identifier}{RE}'
        return m

    def __eq__(self, other):
        if not isinstance(other, Audio):
            return False
        return self.identifier == other.identifier 

    def _set_kwargs(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def add_phrase(self, phrase=None, phrase_key=None, reverse_link=True,
        update_database=True):
        if phrase is None and phrase_key is None:
            raise ValueError("Either phrase or phrase_key must be provided.")
        if phrase_key is None:
            phrase_key = phrase.key
        if phrase_key in self.phrase_keys: return
        if phrase is None: 
            phrase = cache.load(phrase_key, with_links=True)
            if not hasattr(self, '_phrases'):self._phrases = []
            self._phrases.append(phrase)
        phrase.add_audio(self, reverse_link=False, 
            update_database=update_database)
        self.phrase_keys.append(phrase.key)
        if update_database:
            self.save(overwrite=True)

    def add_speaker(self, speaker=None, speaker_key=None, reverse_link=True,
        update_database=True):
        if speaker is None and speaker_key is None:
            raise ValueError("Either speaker or speaker_key must be provided.")
        if speaker_key is None:
            speaker_key = speaker.key
        if speaker_key in self.speaker_keys: return
        self.speaker_keys.append(speaker_key)
        if speaker is None: 
            speaker = cache.load(speaker_key, with_links=True)
            if not hasattr(self, '_speakers'):self._speakers = []
            self._speakers.append(speaker)
        if reverse_link:
            speaker.add_audio(self, reverse_link=False, 
                update_database=update_database)
        if update_database:
            self.save(overwrite=True)


    @property
    def speakers(self):
        if hasattr(self, '_speakers'): return self._speakers
        self._speakers= cache.load_many(self.speaker_keys, with_links=False)
        return self._speakers

    @property
    def phrases(self):
        if hasattr(self, '_phrases'): return self._phrases
        self._phrases= cache.load_many(self.phrase_keys, with_links=False)
        return self._speakers

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
        return lmdb_key.item_to_key(self)
    

    def save(self, overwrite=None, fail_gracefully=False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)

    def to_dict(self):
        """
        Serialize to a clean dict (for LMDB storage).
        """
        base = {}
        for name in self.DB_FIELDS:
            base[name] = getattr(self, name)
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                base[name] = getattr(self, name)

        # Extra metadata
        reserved = set(base.keys()) | {'overwrite', 'object_type'}
        extra = {} 
        if hasattr(self, 'extra') and self.extra is not None:
            extra.update(self.extra)
        for k, v in self.__dict__.items():
            if k.startswith('_'): continue
            if k == 'extra': continue
            if k in reserved: continue
            if k in extra: continue
            extra[k] = v
        base['extra'] = extra
        return base


class Speaker:
    DB_FIELDS = {'name', 'identifier', 'audio_keys', 'phrase_keys'}
    METADATA_FIELDS = {'gender', 'age', 'language', 'dialect', 'region', 
        'channel'}
    FIELDS = DB_FIELDS.union(METADATA_FIELDS)

    @classmethod
    def get_default_cache(cls):
        if hasattr(cls, 'objects'): 
            return cls.objects.cache
        
    def __init__(self, name =None, save=True, overwrite=False, **kwargs):
        self.object_type = self.__class__.__name__
        self.name = name
        self.identifier = lmdb_key.make_item_identifier(self)
        self.overwrite = overwrite
        self.audio_keys = []
        self.phrase_keys = []

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
        m += f'{GR}ID={self.identifier}{RE}'
        return m

    def __eq__(self, other):
        if not isinstance(other, Speaker):
            return False
        return self.key == other.key

    def __contains__(self, key):
        return hasattr(self, key) or key in self.extra

    def add_audio(self, audio=None, audio_key=None, reverse_link=True,
        update_database = True, propagate = None):
        if audio is None and audio_key is None:
            raise ValueError("Either audio or audio_key must be provided.")
        if audio_key is None:
            audio_key = audio.key
        if audio_key in self.audio_keys: return
        if audio is None:
            audio = cache.load(audio_key, with_links=False)
        if not hasattr(self, '_audios'): self._audios = []
        if audio not in self._audios:
            self._audios.append(audio)
        self.audio_keys.append(audio.key)
        if reverse_link:
            audio.add_speaker(self, reverse_link=False, 
                update_database=update_database)
        if update_database:
            self.save(overwrite=True)

    def add_phrase(self, phrase=None, phrase_key=None, reverse_link=True,
        update_database=True):
        if phrase is None and phrase_key is None:
            raise ValueError("Either phrase or phrase_key must be provided.")
        if phrase_key is None:
            phrase_key = phrase.key
        if phrase_key in self.phrase_keys: return
        self.phrase_keys.append(phrase_key)
        if phrase is None:
            phrase = cache.load(phrase_key, with_links=True)
            if not hasattr(self, '_phrases'):self._phrases = []
            self._phrases.append(phrase)
        model_helper.ensure_consistent_link(self, phrase, 'audio_key',
            'add_audio', update_database=update_database)
        if reverse_link:
            phrase.add_speaker(self, reverse_link=False, 
                update_database=update_database)
        if update_database:
            self.save(overwrite=True)

    @property
    def audios(self):
        if hasattr(self, '_audios'): return self._audios 
        self._audios = cache.load_many(self.audio_keys, with_links=False)
        return self._audios

    @property
    def phrases(self):
        """Return all phrases across all audios for this speaker."""
        if hasattr(self, '_phrases'): return self._phrases 
        self._phrases = cache.load_many(self.phrase_keys, with_links=False)
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
        return lmdb_key.item_to_key(self)
            
    def save(self, overwrite=None, fail_gracefully=False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)

    def delete(self):
        cache.delete(self.key)

    def to_dict(self):
        """
        Serialize to a clean dict (for LMDB storage).
        """
        base = {}
        for name in self.DB_FIELDS:
            base[name] = getattr(self, name)
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                base[name] = getattr(self, name)

        # Extra metadata
        reserved = set(base.keys()) | {'overwrite','extra', 'object_type'}
        extra = {} 
        if hasattr(self, 'extra'):
            extra.update(self.extra)
        for k, v in self.__dict__.items():
            if k.startswith('_'): continue
            if k in reserved: continue
            if k in extra: continue
            extra[k] = v
        base['extra'] = extra
        return base

    @property
    def metadata_present(self):
        names = []
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                names.append(name)
        return names



Phrase.allowed_child_types = [Word]
Word.allowed_child_types = [Syllable, Phone]
Syllable.allowed_child_types = [Phone]
Phone.allowed_child_types = []


def load_cache(fraction = None):
    global cache
    cache = cache_module.Cache()

    cache.register(Audio)
    cache.register(Phrase)
    cache.register(Word)
    cache.register(Syllable)
    cache.register(Phone)
    cache.register(Speaker)


    Audio.objects = query.get_class_object(Audio, cache)
    Phrase.objects = query.get_class_object(Phrase, cache)
    Word.objects = query.get_class_object(Word, cache)
    Syllable.objects = query.get_class_object(Syllable, cache)
    Phone.objects = query.get_class_object(Phone, cache)
    Speaker.objects = query.get_class_object(Speaker, cache)

    cache.relations_to_class_map = {
        'audios': Audio, 
        'speakers': Speaker,
        'phrases': Phrase,
        'words': Word,
        'syllables': Syllable,
        'phones': Phone,
    }
    if fraction is not None:
        cache._preload_sampled_fraction(fraction)
            
load_cache()

def touch_db():
    load_cache()

def reconnect_db():
    load_cache()

def turn_off_db_saving():
    cache.turn_off_db_saving()

def turn_on_db_saving():
    cache.turn_on_db_saving()

