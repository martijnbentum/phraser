'''Required persisted rows, decoded fresh from disk for selected checks.

Rows are decoded straight from disk into fresh objects, never walked through
the cache or .children: that is a time-range scan, so a child starting
outside its parent would be invisible to the check meant to find it.
Children are grouped by their stored parent link instead. The speech helpers
give checks one definition of speech where speech filtering applies;
structural checks still see pauses and placeholder labels.
'''

from pathlib import Path

from ..overlap import SILENCE_LABELS
from ..phonology.phone_types import PHONE_TYPES
from ..storage import key_helper
from ..storage.key_helper import EMPTY_ID
from ..storage.store import value_key_to_instance
from .selection import Progress, required_data, select_checks

SEGMENT_CLASSES = ('Phrase', 'Word', 'Syllable', 'Phone')


class AuditData:
    '''Every persisted row of one store, decoded once from disk.'''

    def __init__(self, path, rows, label_index_keys, speaker_audio_links,
        audio_ids=None):
        self.loaded_dependencies = None
        self.transaction_id = None
        self.store_identity = None
        self.path = path
        self.audio_ids = audio_ids
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
        scope = '' if self.audio_ids is None else ' scoped'
        return f'<AuditData {self.path}{scope} {counts}>'

    @property
    def row_counts(self):
        return {'audios': len(self.audios), 'speakers': len(self.speakers),
            'phrases': len(self.phrases), 'words': len(self.words),
            'syllables': len(self.syllables), 'phones': len(self.phones)}

    def parent_of(self, segment):
        '''Return the persisted parent row, or None when absent or unset.'''
        return self.by_key.get(segment.parent_key)


def load_audit_data(store, audio_ids=None, checks=None, progress=False):
    '''Load fresh persisted rows and indexes required by selected checks.
    store:       open Store, assumed to have no concurrent writer
    audio_ids:   optional recording IDs; population checks are skipped
    checks:      check names; None loads dependencies of all applicable checks
    progress:    True to print phases, False for silence, or a callback

    Label-index checks scan the full index to detect stale entries. Other
    checks do not load it. Store construction and cache behavior are unchanged.
    '''
    store._ensure_open()
    names, _ = select_checks(checks, audio_ids is not None)
    required = required_data(names)
    emit = progress if isinstance(progress, Progress) else Progress(progress)
    transaction_id = store.DB.env.info()['last_txnid']
    emit('scanning main keys')
    if audio_ids is None:
        keys_by_class = _all_keys_by_class(store) if required else {}
    else:
        audio_ids = set(audio_ids)
        keys_by_class = _scoped_keys_by_class(store, audio_ids, required)
    rows = {}
    for name in ('Audio', 'Speaker') + SEGMENT_CLASSES:
        rows[name] = []
        if name not in required: continue
        keys = keys_by_class.get(name, [])
        emit(f'loading {name}: {len(keys):,} rows')
        rows[name] = _decode_rows(store, keys)
    label_index_keys, links = set(), set()
    if 'label_index' in required:
        emit('scanning label index')
        label_index_keys = set(store.DB.all_label_index_keys())
        if audio_ids is not None:
            label_index_keys = _scoped_index_keys(label_index_keys, audio_ids)
    if 'speaker_audio_links' in required:
        emit('scanning speaker/audio links')
        links = {(link[:8], link[8:]) for link in store.DB.all_links()}
        if audio_ids is not None:
            links = {link for link in links if link[1] in audio_ids}
    data = AuditData(store.path, rows, label_index_keys, links, audio_ids)
    data.loaded_dependencies = required
    data.transaction_id = transaction_id
    data.store_identity = store_identity(store.path)
    return data


def store_identity(path):
    '''Identify the database file without hashing or scanning its contents.'''
    path = Path(path)
    stat = (path / 'data.mdb').stat()
    return {'path': str(path.resolve()), 'device': stat.st_dev,
        'inode': stat.st_ino}


def _all_keys_by_class(store):
    keys_by_class = {}
    for rank, keys in store.DB.rank_to_keys_dict().items():
        class_name = key_helper.RANK_CLASS_MAP[rank]
        if class_name == 'Marker': continue
        keys_by_class[class_name] = keys
    return keys_by_class


def _scoped_keys_by_class(store, audio_ids, required):
    '''Return the keys of the given recordings, plus every speaker key.'''
    keys_by_class = {'Audio': [], 'Speaker': []}
    if 'Speaker' in required:
        keys_by_class['Speaker'] = store.DB.all_speaker_keys()
    for class_name in SEGMENT_CLASSES: keys_by_class[class_name] = []
    for audio_id in sorted(audio_ids):
        audio_key = key_helper.audio_id_to_key(audio_id)
        if 'Audio' in required and store.DB.key_exists(audio_key):
            keys_by_class['Audio'].append(audio_key)
        for class_name in SEGMENT_CLASSES:
            if class_name not in required: continue
            keys = store.DB.audio_id_to_child_keys(audio_id, class_name)
            keys_by_class[class_name].extend(keys)
    return keys_by_class


def _scoped_index_keys(label_index_keys, audio_ids):
    '''Keep index entries whose segment key lies in one of the recordings.'''
    scoped = set()
    for entry in label_index_keys:
        key = key_helper.label_index_key_to_instance_key(entry)
        if segment_key_audio_id(key) in audio_ids: scoped.add(entry)
    return scoped


def segment_key_audio_id(key):
    '''Return the audio id of an audio or segment key, else None.'''
    if len(key) not in (key_helper.AUDIO_LEN, key_helper.SEGMENT_LEN):
        return None
    return key[1:9]


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
