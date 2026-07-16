import time

from ssh_audio_play import play

from . import key_helper
from . import phone_features
from . import query
from . import struct_value
from . import utils
from .model_helper import EMPTY_ID
from .store import ClosedStoreError, UnboundStoreError
from .utils import R, B, GR, RE, object_type_to_ljust_label


class Segment:
    '''
    Base time-aligned segment with a unique ID and parent/child links.

    Only save and delete touch the database; every other method and
    property (add_parent, add_children, replace_children, ...) works
    in memory. Tree persistence goes through store.save_phrase_trees.
    '''
    IDENTITY_FIELDS= {'label', 'start', 'end', 'audio_key'}
    DB_FIELDS = {'identifier', 'label', 'start', 'end', 'parent_id',
        'parent_start', 'audio_id', 'speaker_id'}
    METADATA_FIELDS = {}# subclasses override
    allowed_child_type = []# subclasses override
    overlap_code = 9

    def __init__(self, label, start, end, audio_id, speaker_id,
        parent_id=EMPTY_ID, parent_start=0,
        save = False, overwrite = False, store = None, **kwargs):

        self.object_type = self.__class__.__name__
        if audio_id is None or audio_id == EMPTY_ID:
            m = f'{self.object_type} requires an audio_id at construction.'
            raise ValueError(m)
        if speaker_id is None or speaker_id == EMPTY_ID:
            m = f'{self.object_type} requires a speaker_id at construction.'
            raise ValueError(m)
        self.label = label
        self.start = int(start)
        self.end = int(end)
        self.identifier = key_helper.make_identifier()

        self.parent_id= parent_id
        self.parent_start = parent_start
        self.audio_id = audio_id
        self.speaker_id = speaker_id
        self.overwrite = overwrite
        self._store = store


        # Extra metadata
        for k, v in kwargs.items():
            setattr(self, k, v)

        if save: self.save(overwrite=overwrite)

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
        for field in self.IDENTITY_FIELDS:
            if getattr(self, field) != getattr(other, field):
                return False
        return True

    def __hash__(self):
        values = tuple(getattr(self, field) for field in self.IDENTITY_FIELDS)
        return hash(values)

    def play(self, collar = None, wait = False):
        if collar is not None:
            if collar < 0: raise ValueError("collar must be non-negative.")
            if collar > self.audio.duration / 2:
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
        return key_helper.instance_to_key(self)

    @property
    def key_info(self):
        return key_helper.key_to_info(self.key)

    @property
    def label_index_key(self):
        return key_helper.instance_to_label_index_key(self)

    def embedding(self, model_name, layer, collar=500, store=None,
        fallback=False):
        '''Load the stored hidden-state Embedding for this segment.

        Uses the echoframe store bound to this segment's phraser store
        (via echoframe_store.attach_phraser_store), or an explicit
        store=... override. Returns an echoframe Embedding object.

        fallback:  if nothing is stored for this segment and fallback=True,
                   walk ancestors (e.g. phone -> syllable -> word -> phrase)
                   and return the first ancestor embedding sliced to this
                   segment as a SlicedEmbedding.
        '''
        store = store or getattr(self.store, 'echoframe_store', None)
        if store is None:
            raise UnboundStoreError(
                'no echoframe store bound; call '
                'echoframe_store.attach_phraser_store(source_id, '
                'phraser_store) or pass store=...')
        try:
            return store.phraser_key_to_embedding(self.key, model_name, layer,
                collar=collar)
        except ValueError as e:
            if not fallback:
                raise ValueError(
                    f'no stored embedding for {self.object_type} '
                    f'(model_name={model_name}, layer={layer}, '
                    f'collar={collar}); try fallback=True to use the '
                    'nearest ancestor embedding') from e
        return self._ancestor_embedding(model_name, layer, collar, store)

    def _ancestor_embedding(self, model_name, layer, collar, store):
        '''Return the nearest ancestor's stored embedding sliced to this
        segment, or raise if no ancestor has one.'''
        for ancestor in self.iter_ancestors():
            try:
                parent = store.phraser_key_to_embedding(ancestor.key,
                    model_name, layer, collar=collar)
            except ValueError:
                continue
            return parent.sub_embedding(self)
        raise ValueError(
            f'no stored embedding for {self.object_type} or its ancestors '
            f'(model_name={model_name}, layer={layer}, collar={collar})')


    @property
    def parent_class_name(self):
        if self.object_type == 'Phrase': return None
        return self.parent_class.__name__

    @property
    def parent_key(self):
        if self.object_type == 'Phrase': return None
        if self.parent_id == EMPTY_ID: return None
        return key_helper.audio_id_segment_id_class_to_key(self.audio_id,
            self.parent_id, self.parent_class_name, self.parent_start)


    @property
    def phrase_key(self):
        if self.phrase_id == EMPTY_ID: return None
        return key_helper.audio_id_segment_id_class_to_key(self.audio_id,
            self.phrase_id, 'Phrase', self.phrase_start)


    @property
    def parent(self):
        """Return the parent segment."""
        if self.object_type == 'Phrase': return None
        if hasattr(self, '_parent'): return self._parent
        if self.parent_id == EMPTY_ID: return
        self._parent = self.store.load(self.parent_key)
        return self._parent

    @property
    def _candidate_child_keys(self):
        '''Keys from a time-range candidate scan: segments of the child
        class in the same audio within [start, end]. NOT an ownership
        list — a candidate may belong to another parent. Only the
        children property consumes this, classifying the loaded
        candidates.'''
        if self.allowed_child_type is None: return []
        return list(self.store.DB.instance_to_child_keys(self))

    @property
    def children(self):
        """Return the list of child segments this segment owns."""
        if self.allowed_child_type is None: return []
        if hasattr(self, '_children'): return self._children
        self._children, self._overlapping = [], []
        # unbound segments have no DB children to merge with
        if getattr(self, '_store', None) is None: return self._children
        candidate_keys = self._candidate_child_keys
        if candidate_keys:
            candidates = self.store.load_many(candidate_keys)
        else:
            candidates = []
        for candidate in candidates:
            if candidate.parent_id == self.identifier:
                self._children.append(candidate)
            else: self._overlapping.append(candidate)
        return self._children

    @property
    def overlapping(self):
        '''Segments of the child class in this segment's time range that
        belong to another parent, e.g. another speaker's overlapping
        speech.'''
        if hasattr(self, '_overlapping'): return self._overlapping
        if self.allowed_child_type is None: return []
        _ = self.children
        return self._overlapping


    @property
    def audio_key(self):
        if self.audio_id == EMPTY_ID: return None
        return key_helper.audio_id_to_key(self.audio_id)

    @property
    def audio(self):
        """Return the associated Audio object."""
        if self.audio_key is None: return None
        if hasattr(self, '_audio') and self._audio is not None:
            return self._audio
        self._audio = self.store.load(self.audio_key)
        return self._audio

    @property
    def speaker_key(self):
        if self.speaker_id == EMPTY_ID: return None
        return key_helper.speaker_id_to_key(self.speaker_id)

    @property
    def speaker(self):
        """Return the associated Speaker object."""
        if self.speaker_key is None: return None
        if hasattr(self, '_speaker'): return self._speaker
        self._speaker = self.store.load(self.speaker_key)
        return self._speaker

    @property
    def phrase(self):
        if self.object_type == 'Phrase': return self
        if self.object_type == 'Word': return self.parent
        if hasattr(self, '_phrase'): return self._phrase
        if self.phrase_key is None: return None
        self._phrase = self.store.load(self.phrase_key)
        return self._phrase


    @property
    def duration(self):
        return self.end - self.start

    @property
    def start_seconds(self):
        return utils.miliseconds_to_seconds(self.start)

    @property
    def end_seconds(self):
        return utils.miliseconds_to_seconds(self.end)

    def duration_seconds(self):
        return utils.miliseconds_to_seconds(self.duration)

    def save(self, overwrite = False, fail_gracefully = False):
        self.store.save(self, overwrite=overwrite,
            fail_gracefully=fail_gracefully)

    def _validate_for_save(self):
        self._validate_audio_assignment(self.audio_id)

    def _validate_audio_assignment(self, audio_id):
        if not hasattr(self, '_key'): return
        stored_audio_id = key_helper.key_to_audio_identifier(self._key)
        if stored_audio_id == audio_id: return
        message = f'{self.object_type}.audio_id cannot change after '
        message += 'persistence; assign audio before saving.'
        raise ValueError(message)

    # ------------------ hierarchy helpers ------------------
    # Linking is staging-only: it never writes to the database. Only the
    # upward link (parent_id, parent_start, phrase_id, phrase_start) is
    # persisted on save; the database derives children by key scan. The
    # downward link lives only in the in-memory _children cache, kept
    # complete here so staged trees are navigable from the root.

    def _validate_parent_link(self, parent):
        if self.object_type == 'Phrase':
            raise TypeError('Phrase cannot have a parent segment.')
        if self.__class__ != parent.allowed_child_type:
            m = f'{parent.object_type} cannot contain '
            m += f'{self.object_type} as child.'
            raise TypeError(m)
        if (self.object_type in ('Syllable', 'Phone') and
            parent.phrase_id != EMPTY_ID and
            self.phrase_id not in (EMPTY_ID, parent.phrase_id)):
            m = f'This {self.object_type} is already linked to a different '
            m += 'phrase.'
            raise ValueError(m)
        for attr in ('audio_id', 'speaker_id'):
            own, other = getattr(self, attr), getattr(parent, attr)
            if own != other:
                raise ValueError(f'{attr} mismatch: {own} vs {other}')

    def add_parent(self, parent):
        self._validate_parent_link(parent)
        old_parent = self._known_parent()
        if old_parent is not None and old_parent is not parent:
            old_parent._uncache_child(self)
        self.parent_id = parent.identifier
        self.parent_start = parent.start
        self._parent = parent
        parent._cache_child(self)
        self._inherit_phrase_from(parent)

    def _known_parent(self):
        '''The in-memory parent, if any: the staged _parent or an already
        loaded instance in the store cache. Never reads the database.'''
        parent = getattr(self, '_parent', None)
        if parent is not None: return parent
        store = getattr(self, '_store', None)
        if store is None: return None
        parent_key = self.parent_key
        if parent_key is None: return None
        return store.get_cached(parent_key)

    def add_child(self, child):
        child.add_parent(self)

    def add_children(self, children):
        '''Link children to this segment, validating all before linking any.'''
        children = list(children)
        for child in children:
            child._validate_parent_link(self)
        for child in children:
            child.add_parent(self)

    def replace_children(self, children):
        '''Displace all current children — staged or persisted — and
        link these instead. Staging-only, like the whole linking
        family: persisted former children remain on disk; delete the
        old tree separately before saving a rebuilt one.'''
        children = list(children)
        # validate before displacing, so a bad batch leaves the
        # current staged view intact
        for child in children:
            child._validate_parent_link(self)
        self._children, self._overlapping = [], []
        self.add_children(children)

    def _cache_child(self, child):
        children = self.children
        if any(known is child for known in children): return
        children.append(child)
        children.sort(key=lambda segment: (segment.start, segment.end))

    def _uncache_child(self, child):
        children = getattr(self, '_children', None)
        if children is None: return
        self._children = [known for known in children if known is not child]

    def _inherit_phrase_from(self, parent):
        if parent.phrase_id == EMPTY_ID: return
        self._set_phrase_refs(parent.phrase_id, parent.phrase_start)

    def _set_phrase_refs(self, phrase_id, phrase_start):
        '''Set phrase refs here and push them through the staged
        children, so trees built bottom-up (phrase linked last) get
        refs everywhere. A Word stores no refs of its own (its phrase
        IS its parent link), but its staged descendants do.'''
        if self.object_type in ('Syllable', 'Phone'):
            self.phrase_id = phrase_id
            self.phrase_start = phrase_start
        for child in getattr(self, '_children', []):
            child._set_phrase_refs(phrase_id, phrase_start)

    # ------------------ serialization ------------------

    def delete(self):
        self.store.delete(self.key)

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
        overlap = utils.reverse_overlap_dict[self.overlap_code]
        if overlap is None: return self.overlap_items != []
        return overlap


    @property
    def overlap_items(self):
        if hasattr(self, '_overlap_items'): return self._overlap_items
        if self.object_type == 'Phrase':
            if not self.audio: return []
            items = self.audio.phrases
        else:
            items = self.parent.overlapping
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
        return self.store.DB.instance_to_descendant_keys(self)

    @property
    def descendants(self):
        #if hasattr(self, '_descendants'): return self._descendants
        keys = self.descendant_keys
        return self.store.load_many(keys)


    def iter_descendants(self):
        for child in self.children:
            yield child
            yield from child.iter_descendants()

    def iter_ancestors(self):
        parent = self.parent
        while parent is not None:
            yield parent
            parent = parent.parent

    @property
    def store(self):
        store = getattr(self, '_store', None)
        if store is None:
            name = self.__class__.__name__
            m = f'{name} is not bound to a Store. Create it with store=... '
            m += f'or Store.create_{name}().'
            raise UnboundStoreError(m)
        if store.closed:
            name = self.__class__.__name__
            m = f'{name} is bound to a closed Store. '
            m += 'Call store.open() to reopen it.'
            raise ClosedStoreError(m)
        return store

    @property
    def exists_in_db(self):
        cls = self.__class__
        lookup = {k: getattr(self, k) for k in cls.IDENTITY_FIELDS}
        existing = self.store.query_for_class(cls).get_or_none(**lookup)
        return existing is not None



class Phrase(Segment):
    IDENTITY_FIELDS = {'audio_id', 'speaker_id', 'start'}
    METADATA_FIELDS = {'filename', 'overlap'}
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

    @property
    def items(self):
        '''The flattened tree: this phrase followed by all its
        descendants in depth-first order. Works on staged trees.'''
        return (self, *self.iter_descendants())

    def validate_tree(self):
        '''Check this phrase and its staged descendants are saveable:
        one speaker across the whole tree, and no persisted segment
        changing its audio.'''
        for item in self.items:
            item._validate_for_save()
            if item.speaker_id != self.speaker_id:
                m = f'{item.object_type} speaker_id does not match its '
                m += 'phrase; reassign the speaker on the whole tree.'
                raise ValueError(m)

    def delete(self):
        """ Delete this phrase and all its descendants from the database.
        """
        all_keys = self.all_keys
        self.store.delete_many(all_keys)

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
        return query.queryset_from_items(self.words, self.store)
    @property
    def syllables_query(self):
        """Return a query object for all syllables for this speaker."""
        return query.queryset_from_items(self.syllables, self.store)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, self.store)


class Word(Segment):
    METADATA_FIELDS = {'overlap', 'ipa'}
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
        return query.queryset_from_items(self.syllables, self.store)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, self.store)

class Syllable(Segment):
    METADATA_FIELDS = {'stress_code'}
    phrase_id = EMPTY_ID
    phrase_start = 0
    stress_code = 9

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

    def _phones_at(self, position):
        return [p for p in self.phones if p.position == position]

    @property
    def onset(self):
        '''Phones in the syllable onset (empty until positions assigned).'''
        return self._phones_at('onset')

    @property
    def nucleus(self):
        '''Phones in the syllable nucleus (empty until positions assigned).'''
        return self._phones_at('nucleus')

    @property
    def coda(self):
        '''Phones in the syllable coda (empty until positions assigned).'''
        return self._phones_at('coda')

    @property
    def word(self):
        """Return the parent word of this syllable."""
        return self.parent

    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, self.store)

class Phone(Segment):
    METADATA_FIELDS = {}

    phrase_id = EMPTY_ID
    phrase_start = 0
    position_code = 9      # stored int: 1=onset 2=nucleus 3=coda, 9=unassigned

    @property
    def position(self):
        '''Syllable position string from the stored position_code
        ('unknown' if unassigned).'''
        codes = {1: 'onset', 2: 'nucleus', 3: 'coda', 9: 'unknown'}
        return codes.get(self.position_code, 'unknown')

    @position.setter
    def position(self, value):
        codes = {'onset': 1, 'nucleus': 2, 'coda': 3}
        self.position_code = codes.get(value, 9)

    @property
    def linguistic_features(self):
        '''Static IPA reference info for this phone's label (articulatory
        descriptors + binary distinctive-feature matrix), or None if the
        label is not a known IPA symbol.

        NB: the binary distinctive-feature matrix is verified against the
        panphon reference (~98.4% agreement); a few contested features
        (laryngeals, central/low vowels) reflect convention choices - see
        phone_features. The 'type' field and articulatory descriptors are
        reliable.'''
        return phone_features.get_phone_features(self.label)

    @property
    def type(self):
        '''Phone type ('vowel' or 'consonant') for this phone's label, or
        None if the label is not a known IPA symbol.'''
        info = phone_features.get_phone_features(self.label)
        return info['type'] if info else None

    @property
    def linguistic_features_vector(self):
        '''The binary distinctive-feature matrix of ``linguistic_features``
        (its ``features`` sub-dict, not the articulatory descriptors) as a
        numeric tuple (+1/-1/0, 0 = not applicable) in
        linguistic_features_names order, or None for an unknown label.
        Distinct from neural embeddings (see ``embedding``).'''
        return phone_features.get_feature_vector(self.label)

    @property
    def linguistic_features_names(self):
        '''Canonical feature names, positionally aligned with
        linguistic_features_vector.'''
        return phone_features.FEATURE_ORDER

    @property
    def stress(self):
        '''Suprasegmental stress for this phone, derived from its parent
        syllable ('unstressed'/'primary'/'secondary'/'unknown'). It is a
        syllable-level property, so every phone in the syllable - including
        consonants - reports the same value.'''
        syllable = self.syllable
        return syllable.stress if syllable else 'unknown'

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


Phrase.allowed_child_type = Word
Word.allowed_child_type = Syllable
Word.parent_class = Phrase
Syllable.allowed_child_type = Phone
Syllable.parent_class = Word
Phone.allowed_child_type = None
Phone.parent_class = Syllable
