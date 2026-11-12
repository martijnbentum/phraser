from .. import overlap as speech_overlap
from .. import utils
from ..storage import key_helper
from ..storage import struct_value
from ..storage.key_helper import EMPTY_ID
from ..utils import R, B, GR, RE, object_type_to_ljust_label
from . import segment_flags
from . import store_access


class Segment:
    '''
    Base time-aligned segment with a unique ID and parent/child links.

    Only save and delete touch the database; every other method and
    property (add_parent, add_children, replace_children, ...) works
    in memory. Tree persistence goes through store.save_phrase_trees.
    '''
    IDENTITY_FIELDS = {'label', 'start', 'end', 'audio_key'}
    DB_FIELDS = {'identifier', 'label', 'start', 'end', 'parent_id',
        'parent_start', 'audio_id', 'speaker_id'}
    METADATA_FIELDS = {'overlap_code'}# stored fields beyond DB_FIELDS
    child_class = None# subclasses override
    flags = 0
    overlap_code = 9
    allow_empty_speaker = False

    def __init__(self, label, start, end, audio_id, speaker_id,
        parent_id=EMPTY_ID, parent_start=0,
        save=False, overwrite=False, store=None, **kwargs):

        self.object_type = self.__class__.__name__
        if audio_id is None or audio_id == EMPTY_ID:
            m = f'{self.object_type} requires an audio_id at construction.'
            raise ValueError(m)
        missing_speaker = speaker_id is None
        if speaker_id == EMPTY_ID and not self.allow_empty_speaker:
            missing_speaker = True
        if missing_speaker:
            m = f'{self.object_type} requires a speaker_id at construction.'
            raise ValueError(m)
        self.label = label
        self.start = int(start)
        self.end = int(end)
        self.identifier = key_helper.make_identifier()

        self.parent_id = parent_id
        self.parent_start = parent_start
        self.audio_id = audio_id
        self.speaker_id = speaker_id
        self.overwrite = overwrite
        self._store = store

        utils.check_init_fields(self, kwargs, self.METADATA_FIELDS)
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

    def play(self, collar=None, wait=False):
        '''Play this segment; see audio.playback.play_segment.'''
        from ..audio.playback import play_segment
        play_segment(self, collar=collar, wait=wait)

    def play_children(self, collar=None, pause=0.3):
        '''Play each child in order; see audio.playback.play_children.'''
        from ..audio.playback import play_children
        play_children(self, collar=collar, pause=pause)

    @property
    def key(self):
        '''Return the LMDB key for this segment.'''
        if hasattr(self, '_key'): return self._key
        return key_helper.instance_to_key(self)

    @property
    def key_info(self):
        return key_helper.key_to_info(self.key)

    @property
    def label_index_key(self):
        return key_helper.instance_to_label_index_key(self)

    @property
    def mfcc(self):
        '''Return the cached wav2vec2-aligned MFCC matrix for this segment.'''
        if hasattr(self, '_mfcc'): return self._mfcc
        from ..audio.mfcc import mfcc as compute_mfcc
        self._mfcc = compute_mfcc(self)
        return self._mfcc

    def embedding(self, model_name, layer, collar=500, store=None,
        fallback=False):
        '''Load the stored embedding for this segment.
        See segment_embeddings.get_stored_embedding (requires echoframe).
        '''
        from ..segment_embeddings import get_stored_embedding
        return get_stored_embedding(self, model_name, layer, collar=collar,
            store=store, fallback=fallback)

    @property
    def child_class_name(self):
        if self.child_class is None: return None
        return self.child_class.__name__

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
        '''Return the parent segment.'''
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
        if self.child_class is None: return []
        return list(self.store.DB.instance_to_child_keys(self))

    @property
    def children(self):
        '''Return the list of child segments this segment owns.'''
        if self.child_class is None: return []
        if hasattr(self, '_children'): return self._children
        self._children, self._overlapping = [], []
        # unbound segments have no DB children to merge with
        if getattr(self, '_store', None) is None: return self._children
        for candidate in self.store.load_many(self._candidate_child_keys):
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
        if self.child_class is None: return []
        _ = self.children
        return self._overlapping


    @property
    def audio_key(self):
        if self.audio_id == EMPTY_ID: return None
        return key_helper.audio_id_to_key(self.audio_id)

    @property
    def audio(self):
        '''Return the associated Audio object.'''
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
        '''Return the associated Speaker object.'''
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

    @property
    def duration_seconds(self):
        return utils.miliseconds_to_seconds(self.duration)

    def save(self, overwrite=False, fail_gracefully=False):
        self.store.save(self, overwrite=overwrite,
            fail_gracefully=fail_gracefully)

    def _validate_for_save(self):
        self._validate_audio_assignment(self.audio_id)
        self._validate_speaker_assignment(self.speaker_id)

    def _validate_audio_assignment(self, audio_id):
        if not hasattr(self, '_key'): return
        stored_audio_id = key_helper.key_to_audio_identifier(self._key)
        if stored_audio_id == audio_id: return
        message = f'{self.object_type}.audio_id cannot change after '
        message += 'persistence; assign audio before saving.'
        raise ValueError(message)

    def _validate_speaker_assignment(self, speaker_id):
        '''Mirror of the audio guard for the identity field the key
        does not carry: speaker_id lives only in the value, so the
        store stamps _persisted_speaker_id at load and save and this
        compares against it.'''
        if not hasattr(self, '_persisted_speaker_id'): return
        if self._persisted_speaker_id == speaker_id: return
        message = f'{self.object_type}.speaker_id cannot change after '
        message += 'persistence; rebuild and replace instead.'
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
        if self.__class__ != parent.child_class:
            m = f'{parent.object_type} cannot contain '
            m += f'{self.object_type} as child.'
            raise TypeError(m)
        self._validate_phrase_link(parent)
        for attr in ('audio_id', 'speaker_id'):
            own, other = getattr(self, attr), getattr(parent, attr)
            if own != other:
                raise ValueError(f'{attr} mismatch: {own} vs {other}')

    def _validate_phrase_link(self, parent):
        '''Reject a syllable or phone already tied to another phrase.'''
        if self.object_type not in ('Syllable', 'Phone'): return
        if parent.phrase_id == EMPTY_ID: return
        if self.phrase_id in (EMPTY_ID, parent.phrase_id): return
        m = f'This {self.object_type} is already linked to a different '
        m += 'phrase.'
        raise ValueError(m)

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

    def to_struct_value(self):
        '''Serialize to a struct value for LMDB storage.'''
        return struct_value.pack_instance(self)

    @property
    def duration_outlier_code(self):
        '''Duration assessment code stored in the flags byte (0 through 3).'''
        return segment_flags.get_duration_outlier_code(self)

    @duration_outlier_code.setter
    def duration_outlier_code(self, code):
        segment_flags.set_duration_outlier_code(self, code)

    @property
    def duration_outlier(self):
        '''This segment's unset, short, long, or within_range assessment.'''
        return segment_flags.get_duration_outlier(self)

    @duration_outlier.setter
    def duration_outlier(self, state):
        segment_flags.set_duration_outlier(self, state)

    @property
    def is_duration_outlier(self):
        '''Whether this segment itself has a short or long assessment.'''
        return segment_flags.is_duration_outlier(self)

    @property
    def timing_uncertain(self):
        '''Recorded timing concern; False does not mean timing was verified.'''
        return segment_flags.get_timing_uncertain(self)

    @timing_uncertain.setter
    def timing_uncertain(self, value):
        segment_flags.set_timing_uncertain(self, value)

    @property
    def shared_phone(self):
        '''For phones: recorded sharing across words, without extra links.'''
        return segment_flags.get_shared_phone(self)

    @shared_phone.setter
    def shared_phone(self, value):
        segment_flags.set_shared_phone(self, value)

    @property
    def missing_shared_phone(self):
        '''For words/syllables: a known shared phone is absent from links.'''
        return segment_flags.get_missing_shared_phone(self)

    @missing_shared_phone.setter
    def missing_shared_phone(self, value):
        segment_flags.set_missing_shared_phone(self, value)

    @property
    def inserted_phone(self):
        '''For phones: a source-confirmed inserted sound linking words.'''
        return segment_flags.get_inserted_phone(self)

    @inserted_phone.setter
    def inserted_phone(self, value):
        segment_flags.set_inserted_phone(self, value)

    @property
    def overlap(self):
        '''Whether this span intersects another speaker's non-silence word.

        Only words in the same audio count, and the intersection must have
        positive duration; touching boundaries do not count. A phrase's
        full span includes pauses, so it can overlap while its words do not.
        Phrase, syllable, and phone intersections do not determine status.

        Codes 0 and 1 return the stored result; code 9 computes from current
        persisted words without saving. Recompute stored codes explicitly
        after annotation changes.
        '''
        overlap = utils.reverse_overlap_dict[self.overlap_code]
        if overlap is None: return bool(self.overlap_items)
        return overlap


    @property
    def overlap_items(self):
        '''Return other-speaker speech words intersecting this object's span.

        These words are the evidence used to compute overlap status;
        this property is equivalent to overlap_words. Phrase spans include
        pauses, so a phrase can overlap while none of its own words does.
        Use overlap_phrases, overlap_syllables, or overlap_phones to inspect
        time intersections at other levels; those do not determine status.
        Stored overlap codes may be stale until explicitly recomputed.
        '''
        return self.overlap_words

    @property
    def overlap_words(self):
        '''Other-speaker speech words intersecting this span.'''
        return speech_overlap.overlap_items(self)

    @property
    def overlap_phrases(self):
        '''Other-speaker phrase spans intersecting this span, pauses included.'''
        return speech_overlap.overlap_at_level(self, 'Phrase')

    @property
    def overlap_syllables(self):
        '''Other-speaker syllable spans intersecting this span.'''
        return speech_overlap.overlap_at_level(self, 'Syllable')

    @property
    def overlap_phones(self):
        '''Other-speaker phone spans intersecting this span.'''
        return speech_overlap.overlap_at_level(self, 'Phone')

    def overlap_all_items(self):
        '''Return intersecting other-speaker items across the hierarchy.

        Results are grouped as phrases, words, syllables, then phones,
        ordered by start time and end time within each level. These are
        time intersections; only the non-silence words determine overlap
        status. A nonempty result therefore does not imply overlap=True.
        '''
        items = self.overlap_phrases + self.overlap_words
        items.extend(self.overlap_syllables)
        items.extend(self.overlap_phones)
        return items

    @property
    def siblings(self):
        if self.object_type == 'Phrase':
            return self.audio.phrases
        if self.parent is None: return
        return self.parent.children

    @property
    def next_sibling(self):
        '''Return the next segment at the same level (same parent).'''
        siblings = self.siblings or []
        if self not in siblings: return None
        idx = siblings.index(self) + 1
        return siblings[idx] if idx < len(siblings) else None

    @property
    def prev_sibling(self):
        '''Return the previous segment at the same level.'''
        siblings = self.siblings or []
        if self not in siblings: return None
        idx = siblings.index(self) - 1
        return siblings[idx] if idx >= 0 else None

    def iter_descendants_of_type(self, cls):
        '''Yield all descendant segments of the given class type,
        at any depth (e.g. Word -> Syllable -> Phone).'''
        for descendant in self.iter_descendants():
            if isinstance(descendant, cls): yield descendant

    def iter_ancestors_of_type(self, cls):
        '''Yield ancestors of the given class, walking upward through
        parents; works on staged trees.'''
        for ancestor in self.iter_ancestors():
            if isinstance(ancestor, cls): yield ancestor

    @property
    def descendants(self):
        '''Persisted descendants this segment owns, grouped by level and
        ordered by start time; one bulk load, staged links are ignored.'''
        keys = self.store.DB.instance_to_descendant_keys(self)
        candidates = self.store.load_many(keys)
        return keep_owned_descendants(candidates, {self.identifier})


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
    def store(self): return store_access.get_store(self)

    @property
    def exists_in_db(self): return store_access.exists_in_db(self)


def keep_owned_descendants(candidates, owner_ids):
    '''Keep candidates whose parent is a root or an already kept candidate.
    candidates must be ordered parents first (Word, Syllable, Phone);
    a child seen before its parent is dropped.
    '''
    owner_ids, owned = set(owner_ids), []
    for candidate in candidates:
        if candidate.parent_id not in owner_ids: continue
        owned.append(candidate)
        owner_ids.add(candidate.identifier)
    return owned
