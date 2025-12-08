import uuid
import lmdb_helper 
import lmdb_cache 
import lmdb_key
import model_helper


class Segment:
    '''
    Base time-aligned segment with a unique ID and parent/child links.
    '''
    allowed_child_types = []# subclasses override

    def __init__(self, label = None, start = None, end = None, 
        key = None,parent_key=None, child_keys=None, save = True, 
        overwrite = False, object_data = None, 
        **kwargs):
        
        if label is None and key is None and object_data is None:
            raise ValueError("Either label, key or data must be provided.")
        if label is None and key:
            if object_data: self._create_from_data(object_data)
            self._create_from_lmdb(key)
            return
        self.object_type = self.__class__.__name__
        self.label = label
        self.start = float(start)
        self.end = float(end)
        self.identifier = lmdb_key.make_item_identifier(self)

        self.parent_key = parent_key
        self.child_keys = child_keys or []
        self.audio_key = 'EMPTY'
        self.speaker_key = 'EMPTY'
        self.overwrite = overwrite

        # Extra metadata
        for k, v in kwargs.items():
            setattr(self, k, v)

        if save: self.save(overwrite=overwrite)
        self._save_status = None

    def __repr__(self):
        m = f'{self.object_type}( '
        m += f'label={self.label}, '
        m += f'start={self.start}, end={self.end} | '
        m += f'ID={self.identifier} '
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
        if not hasattr(self, '_children'): self._children = []
        self._children = cache.load_many(self.child_keys, with_links=False)
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

    def _create_from_lmdb(self, key):
        instance = self.from_dict(lmdb_helper.load(key))
        self.__dict__.update(instance.__dict__)

    def _create_from_data(self, data):
        instance = self.from_dict(data)
        self.__dict__.update(instance.__dict__)

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

        model_helper.write_changes_to_db(all_segments, cache)

        if reverse_link and audio is not None:
            if self.object_type == 'Phrase':
                audio.add_phrase(self, reverse_link=False)

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
                speaker.add_phrase(self, reverse_link=False)

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

    def to_dict(self):
        """
        make a clean dict (for LMDB storage).
        """
        base = {
            "identifier": self.identifier,
            "label": self.label,
            "start": self.start,
            "end": self.end,
            "parent_key": self.parent_key,
            "child_keys": list(self.child_keys),
        }

        # Extra metadata
        reserved = set(base.keys()) | {'children', 'parent','audio', 'speaker',
            'object_type','overwrite'}
        extras = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'): continue
            if k in reserved: continue
            extras[k] = v
        base["extra"] = extras
        return base

    @classmethod
    def from_dict(cls, data):
        """
        Create an instance from a dict with unresolved links.
        """
        extra = data.get("extra", {})
        obj = cls(
            label=data["label"],
            start=data["start"],
            end=data["end"],
            identifier=data["identifier"],
            parent_key=data.get("parent_key"),
            child_keys=data.get("child_keys", []),
            save = False,
            **extra,
        )
        return obj

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

class Word(Segment):

    @property
    def syllables(self):
        """Return all syllables in this word."""
        return self.children

    @property
    def phones(self):
        """Return all phones in this word."""
        return list(self.iter_descendants_of_type(Phone))

class Syllable(Segment):

    @property
    def phones(self):
        """Return all phones in this syllable."""
        return self.children

    @property
    def word(self):
        """Return the parent word of this syllable."""
        return self.parent


class Phone(Segment):

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

    def __init__(self, filename = None, key = None, save=True, 
        overwrite=False, **kwargs):
        self.object_type = self.__class__.__name__
        if filename is None and key:
            self._create_from_lmdb(key)
            return
        self.filename = filename
        self.identifier = lmdb_key.make_item_identifier(self)
        self.overwrite = overwrite

        self.speaker_keys = []
        self.phrase_keys = []
        self._set_kwargs(**kwargs)

        if save:
            self.save(overwrite=overwrite)

    def __repr__(self):
        m = f'Audio( filename={self.filename} | '
        m += f'ID={self.identifier} )'
        return m

    def __eq__(self, other):
        if not isinstance(other, Audio):
            return False
        return self.identifier == other.identifier 

    def _set_kwargs(self, **kwargs):
        self._metadata_attr_names = []
        for k, v in kwargs.items():
            setattr(self, k, v)
            if k in self._metadata_attr_names: continue 
            if k in ["speaker_keys", "phrase_keys"]: continue
            self._metadata_attr_names.append(k)

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
        if reverse_link:
            phrase.add_audio(self, reverse_link=False, 
                update_database=update_database)
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
    def key(self):
        """Return the LMDB key for this segment."""
        return lmdb_key.item_to_key(self)
    

    def save(self, overwrite=None, fail_gracefully=False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)


    def to_dict(self):
        """
        Serialize to a clean dict (for LMDB storage).
        """
        base = {
            "identifier": self.identifier,
            "filename": str(self.filename),
            "speaker_keys": list(self.speaker_keys),
            "phrase_keys": list(self.phrase_keys),
        }

        # Extra metadata
        reserved = set(base.keys()) | {'object_type','overwrite'}
        extras = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'): continue
            if k in reserved: continue
            extras[k] = v
        base["extra"] = extras
        return base

    @classmethod
    def from_dict(cls, data):
        """
        Create an instance from a dict with unresolved links.
        """
        extra = data.get("extra", {})
        obj = cls(
            filename=data["filename"],
            identifier=data["identifier"],
            speaker_keys=data.get('speaker_keys', []),
            phrase_keys=data.get('phrase_keys', []),
            save = False,
            **extra,
        )
        return obj

    def _create_from_lmdb(self, key):
        instance = self.from_dict(lmdb_helper.load(key))
        self.__dict__.update(instance.__dict__)


class Speaker:

    def __init__(self, name =None, key = None, save=True, 
        overwrite=False, **kwargs):
        if name is None and key:
            self._create_from_lmdb(key)
            return
        self.object_type = self.__class__.__name__
        self.name = name
        self.identifier = lmdb_key.make_item_identifier(self)
        self.overwrite = overwrite
        self.audio_keys = []
        self.phrase_keys = []

        # Extra metadata
        for k, v in kwargs.items():
            setattr(self, k, v)

        if save:
            self.save(overwrite=overwrite)

    def __repr__(self):
        m = f'Speaker( name={self.name} | '
        m += f'ID={self.identifier} )'
        return m

    def __eq__(self, other):
        if not isinstance(other, Speaker):
            return False
        return self.key == other.key

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
            audio.add_speaker(self, reverse_link=False)
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
            phrase.add_speaker(self, reverse_link=False)
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
        for phrase in self.phrases:
            for word in phrase.words:
                yield word

    @property
    def syllables(self):
        """Return all syllables across all phrases for this speaker."""
        for word in self.words:
            for syllable in word.syllables:
                yield syllable

    @property
    def phones(self):
        """Return all phones across all phrases for this speaker."""
        for word in self.word:
            for phone in syllable.phones:
                yield phone

            
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
        base = {
            "identifier": self.identifier,
            "name": self.name,
        }

        # Extra metadata
        reserved = set(base.keys()) | {'overwrite'}
        extras = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'): continue
            if k in reserved: continue
            extras[k] = v
        base["extra"] = extras
        return base

    @classmethod
    def from_dict(cls, data):
        """
        Create an instance from a dict with unresolved links.
        """
        extra = data.get("extra", {})
        obj = cls(
            name=data["name"],
            identifier=data["identifier"],
            save = False,
            **extra,
        )
        return obj

    def _create_from_lmdb(self, key):
        instance = self.from_dict(lmdb_helper.load(key))
        self.__dict__.update(instance.__dict__)


Phrase.allowed_child_types = [Word]
Word.allowed_child_types = [Syllable, Phone]
Syllable.allowed_child_types = [Phone]
Phone.allowed_child_types = []


cache = lmdb_cache.Cache()

cache.register(Audio)
cache.register(Phrase)
cache.register(Word)
cache.register(Syllable)
cache.register(Phone)
cache.register(Speaker)


Audio.objects = lmdb_cache.Objects(Audio, cache)
Phrase.objects = lmdb_cache.Objects(Phrase, cache)
Word.objects = lmdb_cache.Objects(Word, cache)
Syllable.objects = lmdb_cache.Objects(Syllable, cache)
Phone.objects = lmdb_cache.Objects(Phone, cache)
Speaker.objects = lmdb_cache.Objects(Speaker, cache)
    




