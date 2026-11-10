'''Every persisted row of a store, decoded once from disk for the checks.

Rows are decoded straight from disk into fresh objects, never walked through
the cache or .children: that is a time-range scan, so a child starting
outside its parent would be invisible to the check meant to find it.
Children are grouped by their stored parent link instead. The speech helpers
give checks one definition of speech where speech filtering applies;
structural checks still see pauses and placeholder labels.
'''

from ..overlap import SILENCE_LABELS
from ..phonology.phone_types import PHONE_TYPES
from ..storage import key_helper
from ..storage.key_helper import EMPTY_ID
from ..storage.store import value_key_to_instance

SEGMENT_CLASSES = ('Phrase', 'Word', 'Syllable', 'Phone')


class AuditData:
    '''Every persisted row of one store, decoded once from disk.'''

    def __init__(self, path, rows, label_index_keys, speaker_audio_links):
        self.path = path
        self.audios = rows['Audio']
        self.speakers = rows['Speaker']
        self.phrases = rows['Phrase']
        self.words = rows['Word']
        self.syllables = rows['Syllable']
        self.phones = rows['Phone']
        self.segments = self.phrases + self.words + self.syllables + self.phones
        self.label_index_keys = label_index_keys
        self.speaker_audio_links = speaker_audio_links
        self.audio_by_id = {a.identifier: a for a in self.audios}
        self.speaker_by_id = {s.identifier: s for s in self.speakers}
        self.by_key = {s.key: s for s in self.segments}
        self.by_identifier = {s.identifier: s for s in self.segments}
        self.children = _children_by_parent(self.segments)

    def __repr__(self):
        counts = ' '.join(f'{k}={v}' for k, v in self.row_counts.items())
        return f'<AuditData {self.path} {counts}>'

    @property
    def row_counts(self):
        return {'audios': len(self.audios), 'speakers': len(self.speakers),
            'phrases': len(self.phrases), 'words': len(self.words),
            'syllables': len(self.syllables), 'phones': len(self.phones)}

    def parent_of(self, segment):
        '''Return the persisted parent row, or None when absent or unset.'''
        return self.by_key.get(segment.parent_key)


def load_audit_data(store):
    '''Decode every row of a store from disk, bypassing its cache.
    store:    open Store; it is read and left unchanged, markers are skipped
    '''
    store._ensure_open()
    keys_by_rank = store.DB.rank_to_keys_dict()
    rows = {}
    for rank, keys in keys_by_rank.items():
        class_name = key_helper.RANK_CLASS_MAP[rank]
        if class_name == 'Marker': continue
        rows[class_name] = _decode_rows(store, keys)
    label_index_keys = set(store.DB.all_label_index_keys())
    links = {(link[:8], link[8:]) for link in store.DB.all_links()}
    return AuditData(store.path, rows, label_index_keys, links)


def _decode_rows(store, keys):
    '''Decode rows into fresh objects that are never added to the cache.'''
    values = store.DB.load_many(keys)
    pairs = zip(values, keys, strict=True)
    return [value_key_to_instance(store, v, k) for v, k in pairs]


def _children_by_parent(segments):
    '''Group rows by parent_id, using the upward link and not time ranges.'''
    children = {}
    for segment in segments:
        if segment.object_type == 'Phrase': continue
        if segment.parent_id == EMPTY_ID: continue
        children.setdefault(segment.parent_id, []).append(segment)
    for group in children.values():
        group.sort(key=lambda segment: (segment.start, segment.end))
    return children


def _is_speech(segment):
    '''Exclude pause labels and the non-speech placeholder labels.'''
    if segment.label.strip() in SILENCE_LABELS: return False
    return PHONE_TYPES.get(segment.label) != 'other'


def _speech_phones(data):
    return [phone for phone in data.phones if _is_speech(phone)]


def _speech_words(data):
    return [word for word in data.words if _is_speech(word)]
