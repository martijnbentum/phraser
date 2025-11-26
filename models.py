import uuid
import lmdb_helper 
import lmdb_cache 
import lmdb_key


class Segment:
    '''
    Base time-aligned segment with a unique ID and parent/child links.
    '''

    allowed_child_type = None    # subclasses override

    def __init__(self, label = None, start = None, end = None, 
        key = None,parent_id=None, child_ids=None, save = False, 
        overwrite = False,**kwargs):
        
        if label is None and key is None:
            raise ValueError("Either label or key must be provided.")
        if label is None and key:
            self._create_from_lmdb(key)
            return
        self.object_type = self.__class__.__name__
        self.label = label
        self.start = float(start)
        self.end = float(end)
        self.identifier = lmdb_key.make_item_identifier(self)

        self.parent_id = parent_id
        self.child_ids = child_ids or []
        self.audio_id = 'EMPTY'
        self.speaker_id = 'EMPTY'
        self.overwrite = overwrite

        # Extra metadata
        for k, v in kwargs.items():
            setattr(self, k, v)

        if save: self.save(overwrite=overwrite)

    def __repr__(self):
        m = f'{self.object_type}( '
        m += f'label={self.label}, '
        m += f'start={self.start}, end={self.end} | '
        m += f'ID={self.identifier} '
        return m

    def __eq__(self, other):
        if not isinstance(other, Segment):
            return False
        return self.identifier == other.identifier and \
            self.audio_id == other.audio_id and \
            self.object_type == other.object_type

    @property
    def key(self):
        """Return the LMDB key for this segment."""
        return lmdb_key.item_to_key(self)

    @property
    def parent(self):
        """Return the parent segment."""
        if hasattr(self, '_parent'): return self._parent
        if self.parent_id is None:return None
        self._parent = cache.load(self.parent_id, with_links=False)
        return self._parent

    @property
    def children(self):
        """Return the list of child segments."""
        if hasattr(self, '_children'): return self._children
        self._children = []
        for cid in self.child_ids:
            child = cache.load(cid, with_links=False)
            if child is None: 
                raise ValueError(f"Child with ID {cid} not found in LMDB.")
            self._children.append(child)
        return self._children

    @property
    def audio(self):
        """Return the associated Audio object."""
        if hasattr(self, '_audio'): return self._audio
        if self.audio_id is None:
            return None
        self._audio = cache.load(self.audio_id, with_links=False)
        return self._audio
        
    @property
    def speaker(self):
        """Return the associated Speaker object."""
        if hasattr(self, '_speaker'): return self._speaker
        if self.speaker_id is None: return None
        self._speaker = cache.load(self.speaker_id, with_links=False)
        return self._speaker

    @property
    def duration(self):
        return self.end - self.start
            
    def save(self, overwrite = None, fail_gracefully = False):
        cache.save(self, overwrite=overwrite, fail_gracefully=fail_gracefully)

    def _create_from_lmdb(self, key):
        instance = self.from_dict(lmdb_helper.lmdb_load(key))
        self.__dict__.update(instance.__dict__)

    def add_audio(self, audio = None, audio_id = None, reverse_link = True,
        update_database = True):
        if audio is None and audio_id is None:
            raise ValueError("Either audio or audio_id must be provided.")
        old_key = self.key
        if audio_id is None:
            audio_id = audio.identifier
        self.audio_id = audio_id
        new_key = self.key
        if update_database and old_key != new_key:
            cache.update(old_key, self) 
        self._audio = audio
        if reverse_link and self.object_type == 'Phrase':
            audio.add_phrase(self, reverse_link=False)

    def add_speaker(self, speaker = None, speaker_id = None, 
        update_database = True):
        if speaker is None and speaker_id is None:
            raise ValueError("Either speaker or speaker_id must be provided.")
        if speaker_id is None:
            speaker_id = speaker.identifier
        self.speaker_id = speaker_id
        self._speaker = speaker
        if update_database: self.save(overwrite = True)

    # ------------------ hierarchy helpers ------------------

    def add_child(self, child= None, child_id = None, reverse_link = True,
        update_database = True):
        """    Add a child segment, enforcing the declared hierarchy.
        """
        if self.allowed_child_type is None:
            raise TypeError(f"{self.object_type} cannot have children.")
        if child is None and child_id is None:
            raise ValueError("Either child or child_id must be provided.")
        if child_id is None:
            child_id = child.identifier
        if child_id in self.child_ids: return
        if child is None:
            child = cache.load(child_id, with_links=True)
        if not isinstance(child, self.allowed_child_type):
            m = f'{self.object_type} can only contain '
            m += f'{self.allowed_child_type.__name__}, '
            m += f'not {child.__class__.__name__}'
            raise TypeError(m)
        self.child_ids.append(child.identifier)
        if not hasattr(self, '_children'):
            self._children = []
        self._children.append(child)
        if reverse_link:
            child.add_parent(self, reverse_link=False, 
                update_database=update_database)
        if update_database: self.save(overwrite = True)

    def add_parent(self, parent = None, parent_id = None, reverse_link = True,
        update_database = True):
        if self.object_type == 'Phrase':
            raise TypeError("Phrase cannot have a parent segment.")
        if parent is None and parent_id is None:
            raise ValueError("Either parent or parent_id must be provided.")
        if parent_id is None:
            parent_id = parent.identifier
        if parent is None:
            parent = cache.load(parent_id, with_links=True)
        if parent.allowed_child_type != self.object_type:
            m = f'{parent.object_type} cannot contain '
            m += f'{self.object_type} as child.'
            raise TypeError(m)
        self.parent_id = parent_id
        self._parent = parent
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
            "parent_id": self.parent_id,
            "child_ids": list(self.child_ids),
        }

        # Extra metadata
        reserved = set(base.keys()) | {'children', 'parent','audio', 'speaker'}
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
            parent_id=data.get("parent_id"),
            child_ids=data.get("child_ids", []),
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

    @property
    def phrase(self):
        """Return the parent phrase of this word."""
        return self.parent

class Syllable(Segment):

    @property
    def phones(self):
        """Return all phones in this syllable."""
        return self.children

    @property
    def word(self):
        """Return the parent word of this syllable."""
        return self.parent

    @property
    def phrase(self):
        """Return the parent phrase of this syllable."""
        if self.parent is None:
            return None
        return self.parent.parent

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
        return self.parent.parent

    @property
    def phrase(self):
        """Return the parent phrase of this phone."""
        if self.parent is None or self.parent.parent is None:
            return None
        return self.parent.parent.parent



class Audio:
    def __init__(self, filename = None, identifier = None, save=False, 
        overwrite=False, **kwargs):
        self.object_type = self.__class__.__name__
        if filename is None and identifier:
            self._create_from_lmdb(identifier)
            return
        self.filename = filename
        self.identifier = lmdb_key.make_item_identifier(self)
        self.overwrite = overwrite

        self.speaker_ids = []
        self.phrase_ids = []
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
            if k in ["speaker_ids", "phrase_ids"]: continue
            self._metadata_attr_names.append(k)

    def add_phrase(self, phrase=None, phrase_id=None, reverse_link=True,
        update_database=True):
        if phrase is None and phrase_id is None:
            raise ValueError("Either phrase or phrase_id must be provided.")
        if phrase_id is None:
            phrase_id = phrase.identifier
        if phrase_id in self.phrase_ids: return
        self.phrase_ids.append(phrase_id)
        if phrase is None: 
            phrase = cache.load(phrase_id, with_links=True)
            if not hasattr(self, '_phrases'):self._phrases = []
            self._phrases.append(phrase)
        if reverse_link:
            phrase.add_audio(self, reverse_link=False, 
                update_database=update_database)
        if update_database:
            self.save(overwrite=True)

    def add_speaker(self, speaker=None, speaker_id=None, reverse_link=True,
        update_database=True):
        if speaker is None and speaker_id is None:
            raise ValueError("Either speaker or speaker_id must be provided.")
        if speaker_id is None:
            speaker_id = speaker.identifier
        if speaker_id in self.speaker_ids: return
        self.speaker_ids.append(speaker_id)
        if speaker is None: 
            speaker = cache.load(speaker_id, with_links=True)
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
        self._speakers = []
        for sid in self.speaker_ids:
            speaker = cache.load(sid, with_links=False)
            self._speakers.append(speaker)
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
            "speaker_ids": list(self.speaker_ids),
            "phrase_ids": list(self.phrase_ids),
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
            speaker_ids=data.get(speaker_ids, []),
            phrase_ids=data.get(phrase_ids, []),
            **extra,
        )
        return obj

    def _create_from_lmdb(self, identifier):
        instance = self.from_dict(lmdb_helper.lmdb_load(identifier))
        self.__dict__.update(instance.__dict__)


class Speaker:
    def __init__(self, name =None, identifier = None, save=False, 
        overwrite=False, **kwargs):
        if name is None and identifier:
            self._create_from_lmdb(identifier)
            return
        self.object_type = self.__class__.__name__
        self.name = name
        self.identifier = lmdb_key.make_item_identifier(self)
        self.overwrite = overwrite
        self.audio_ids = []

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
        return self.identifier == other.identifier 

    def add_audio(self, audio=None, audio_id=None, reverse_link=True):
        if audio is None and audio_id is None:
            raise ValueError("Either audio or audio_id must be provided.")
        if audio_id is None:
            audio_id = audio.identifier
        if audio_id in self.audio_ids: return
        if audio is None:
            audio = cache.load(audio_id, with_links=True)
        self.audios.append(audio)
        self.audio_ids.append(audio.identifier)
        if reverse_link:
            audio.add_speaker(self, reverse_link=False)
        

    @property
    def audios(self):
        if hasattr(self, '_audios'): return self._audios
        self._audios = []
        for aid in self.audio_ids:
            audio = cache.load(aid, with_links=False)
            self._audios.append(audio)

    @property
    def phrases(self):
        """Return all phrases across all audios for this speaker."""
        for audio in self.audios:
            for phrase in audio.phrases:
                yield phrase

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
            "audio_ids": list(self.speaker_ids),
        }

        # Extra metadata
        reserved = set(base.keys()) | {}
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
            audio_ids=data.get("audio_ids", []),
            **extra,
        )
        return obj

    def _create_from_lmdb(self, identifier):
        instance = self.from_dict(lmdb_helper.lmdb_load(identifier))
        self.__dict__.update(instance.__dict__)


Phrase.allowed_child_type = Word
Word.allowed_child_type = Syllable
Syllable.allowed_child_type = Phone
Phone.allowed_child_type = None

cache = lmdb_cache.Cache()

cache.register(Audio)
cache.register(Phrase)
cache.register(Word)
cache.register(Syllable)
cache.register(Phone)
cache.register(Speaker)


