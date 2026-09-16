'''Markers linked to audio and speakers, outside the phrase hierarchy.

Markers can reference a phrase without belonging to its segment tree.
'''

from . import utils
from .model_helper import EMPTY_ID
from .segment import Phrase, Segment
from progressbar import progressbar




class Marker(Segment):
    '''A time interval with audio, an optional speaker, and no relatives.'''

    DB_FIELDS = {'identifier', 'label', 'start', 'end', 'audio_id', 'speaker_id',
        'phrase_id', 'phrase_start'}
    allowed_child_type = None
    allow_empty_speaker = True
    phrase_id = EMPTY_ID
    phrase_start = 0

    def __init__(self, label, start, end, audio_id, speaker_id, store=None,
        phrase=None):
        '''Create a marker; start and end are recording-relative milliseconds.'''
        super().__init__(label, start, end, audio_id, speaker_id, store=store)
        if phrase is not None: self.attach_phrase(phrase)

    def attach_phrase(self, phrase):
        '''Link a phrase without changing its children; None clears the link.'''
        if phrase is not None:
            if not isinstance(phrase, Phrase):
                raise TypeError('Marker phrase must be a Phrase or None.')
            for name in ('audio_id', 'speaker_id'):
                if getattr(self, name) != getattr(phrase, name):
                    raise ValueError(f'Marker and phrase {name} must match.')
        self.phrase_id = EMPTY_ID if phrase is None else phrase.identifier
        self.phrase_start = 0 if phrase is None else phrase.start
        self._phrase = phrase

    @property
    def overlap(self):
        '''Whether this marker has a linked phrase.'''
        return self.phrase is not None

    def _overlapping_children(self, items):
        '''Collect overlapping children of the supplied higher-level items.'''
        matches = []
        for item in items:
            for child in item.children:
                if utils.overlap(self, child): matches.append(child)
        return matches

    @property
    def overlap_word(self):
        '''All overlapping words from the linked phrase.'''
        phrase = self.phrase
        if phrase is None or not utils.overlap(self, phrase): return []
        return self._overlapping_children([phrase])

    @property
    def overlap_syllable(self):
        '''All overlapping syllables from overlapping words.'''
        return self._overlapping_children(self.overlap_word)

    @property
    def overlap_phone(self):
        '''All overlapping phones from overlapping syllables.'''
        return self._overlapping_children(self.overlap_syllable)

    @property
    def overlap_items(self):
        '''Overlapping objects ordered by level, from phrase through phones.'''
        phrase = self.phrase
        if phrase is None or not utils.overlap(self, phrase): return []
        items, level = [phrase], [phrase]
        for _ in range(3):
            level = self._overlapping_children(level)
            items.extend(level)
        return items

    @property
    def parent(self):
        return None

    @property
    def parent_key(self):
        return None

    @property
    def parent_class_name(self):
        return None

    @property
    def descendant_keys(self):
        return []

    @property
    def descendants(self):
        return []

    def _validate_parent_link(self, parent):
        raise TypeError('Marker cannot have a parent segment.')





def make_markers(data, store, save=False):
    '''Return markers in input order, optionally saving them in bulk.

    data:     list of dictionaries with label, start, end, and audio;
              optional phrase and speaker are passed to make_marker
    store:    Store to bind to every marker
    save:     save all markers in one batch when True
    '''
    markers = []
    for item in progressbar(data):
        marker = make_marker(store=store, **item)
        markers.append(marker)
    if save and markers: store.save_many(markers)
    return markers



def make_marker(label, start, end, audio, store, phrase=None, speaker=None):
    '''Create an unsaved marker using a phrase's speaker and optional lookup.

    label:     marker label
    start:     recording-relative start in milliseconds
    end:       recording-relative end in milliseconds
    audio:     Audio object containing the marker
    store:     Store to bind to the marker
    phrase:    explicit phrase, or find the first overlap in audio.phrases
    speaker:   explicit speaker, or use the phrase's speaker ID

    Use EMPTY_ID if no overlapping phrase or explicit speaker is available.
    '''
    start, end = int(start), int(end)
    if phrase is None:
        for candidate in audio.phrases:
            if start < candidate.end and candidate.start < end:
                phrase = candidate
                break
    if speaker is not None: speaker_id = speaker.identifier
    elif phrase is not None: speaker_id = phrase.speaker_id
    else: speaker_id = EMPTY_ID
    return Marker(label, start, end, audio.identifier, speaker_id,
        store=store, phrase=phrase)


def bulk_delete_markers(label, store):
    '''Delete persisted markers with an exact label and return their count.

    label:    case-sensitive marker label to delete
    store:    Store containing the markers

    Commit batches of 10,000 with their label-index entries. Earlier batches
    remain deleted if a later batch fails. Refresh store query roots afterward;
    previously held query objects remain snapshots.
    '''
    store._ensure_open()
    keys = list(store.DB.label_to_segment_keys(label, 'Marker'))
    count = 0
    try:
        for start in progressbar(range(0, len(keys), 10_000)):
            batch = keys[start:start + 10_000]
            deleted = store.DB.delete_labelled_keys(batch, label, 'Marker')
            for key in deleted:
                store._cache.pop(key, None)
            count += len(deleted)
    finally:
        if keys: store.refresh_query_roots()
    return count
