import uuid
from lmdb_helper import lmdb_write, lmdb_load
import lmdb_cache 

def make_identifier(segment):
    return f"{segment.object_type}:{uuid.uuid4().hex}"

class Segment:
    '''
    Base time-aligned segment with a unique ID and parent/child links.
    '''

    allowed_child_type = None# subclasses override
    object_type = "segment"      # subclasses override

    def __init__(self, label = None, start = None, end = None, identifier=None, 
        parent_id=None, child_ids=None, save = False, overwrite = False,**kwargs):
        if label is None and identifier is None:
            raise ValueError("Either label or identifier must be provided.")
        if label is None and identifier:
            self._create_from_lmdb(identifier)
            return
        self.label = label
        self.start = float(start)
        self.end = float(end)
        self.identifier = identifier or make_identifier(self)

        self.parent_id = parent_id
        self.child_ids = child_ids or []
        self.audio_id = None
        self.audio = None
        self.speaker_id = None
        self.speaker = None
        self.overwrite = overwrite

        self.parent = None
        self.children = []

        # Extra metadata
        for k, v in kwargs.items():
            setattr(self, k, v)

        if save: self.save(overwrite=overwrite)

    def save(self, overwrite = None, fail_gracefully = False):
        if overwrite is None: overwrite = self.overwrite
        try:lmdb_write(self.identifier, self.to_dict(), overwrite=overwrite)
        except KeyError as e:
            if fail_gracefully:
                m = f"Segment with ID {self.identifier} already exists. "
                m += "Skipping save."
                print(m)
            else:raise e
                


    def _create_from_lmdb(self, identifier):
        instance = self.from_dict(lmdb_load(identifier))
        self.__dict__.update(instance.__dict__)

    @property
    def duration(self):
        return self.end - self.start

    # ------------------ hierarchy helpers ------------------

    def add_child(self, child):
        """    Add a child segment, enforcing the declared hierarchy.
        """
        if self.allowed_child_type is None:
            raise TypeError(f"{self.object_type} cannot have children.")

        if not isinstance(child, self.allowed_child_type):
            m = f'{self.object_type} can only contain '
            m += f'{self.allowed_child_type.__name__}, '
            m += f'not {child.__class__.__name__}'
            raise TypeError(m)

        self.children.append(child)
        self.child_ids.append(child.identifier)
        child.parent = self
        child.parent_id = self.identifier

    # ------------------ serialization ------------------

    def to_dict(self):
        """
        Serialize to a clean dict (for LMDB storage).
        """
        base = {
            "type": self.object_type,
            "identifier": self.identifier,
            "label": self.label,
            "start": self.start,
            "end": self.end,
            "parent_id": self.parent_id,
            "child_ids": list(self.child_ids),
        }

        # Extra metadata
        reserved = set(base.keys()) | {'children', 'parent','audio', 'speaker'}
        extras = {k: v for k, v in self.__dict__.items() if k not in reserved}
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

    def __repr__(self):
        m = f'{self.object_type}(id={self.identifier}, '
        m += f'label={self.label}, '
        m += f'start={self.start}, end={self.end})'
        return m

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
    object_type = 'phrase'

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
    object_type = 'word'

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
    object_type = 'syllable'

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
    object_type = 'phone'

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

Phrase.allowed_child_type = Word
Word.allowed_child_type = Syllable
Syllable.allowed_child_type = Phone
Phone.allowed_child_type = None

store = lmdb_cache.Cache()

store.register(Phrase)
store.register(Word)
store.register(Syllable)
store.register(Phone)
