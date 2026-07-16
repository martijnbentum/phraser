'''Save-time validation and persisted-row collection for the Store.

Plain functions taking the store explicitly; Store methods delegate
here. Nothing in this module writes: the validators raise on a bad
batch before anything is packed, and persisted_tree_rows only reads.
'''

from . import key_helper
from . import struct_value


def check_intra_batch_keys(objs, keys):
    '''Reject duplicate keys within one batch. DB.write_many only
    checks keys against the database; within a transaction a
    repeated key silently keeps the last value written, so one
    object would overwrite the other without an error.'''
    seen = {}
    for obj, key in zip(objs, keys):
        other = seen.get(key)
        if other is None:
            seen[key] = obj
            continue
        m = 'duplicate key in batch: '
        m += f'{type(obj).__name__} {obj.identifier.hex()} '
        m += 'appears twice (the same object listed twice, or two '
        m += 'loaded copies of one persisted object); written nothing.'
        raise ValueError(m)


def validate_phrase_trees(store, phrases):
    '''Validate a save_phrase_trees batch: Phrase objects only, each
    tree coherent (Phrase.validate_tree), no duplicate phrase identity
    in the batch, no same-speaker phrase overlap.'''
    from .models import Phrase
    seen = set()
    for phrase in phrases:
        if not isinstance(phrase, Phrase):
            m = 'save_phrase_trees expects Phrase objects, '
            m += f'got {type(phrase).__name__}.'
            raise TypeError(m)
        phrase.validate_tree()
        if phrase in seen:
            m = 'duplicate phrase identity in batch: '
            m += f'(audio_id, speaker_id, start) = ({phrase.audio_id}, '
            m += f'{phrase.speaker_id}, {phrase.start})'
            raise ValueError(m)
        seen.add(phrase)
    check_same_speaker_overlap(store, phrases)


def check_same_speaker_overlap(store, phrases):
    '''One speaker, one phrase at a time: reject a phrase that
    overlaps a same-speaker phrase, in the batch or already
    persisted on the same audio. A phrase's own persisted row
    (identical key) is exempt, so overwrite re-saves pass. Two
    persisted rows overlapping each other are legacy data and do
    not block an unrelated save.'''
    groups = {}
    for phrase in phrases:
        group_key = (phrase.audio_id, phrase.speaker_id)
        groups.setdefault(group_key, []).append(phrase)
    persisted = persisted_phrases_by_group(store, groups)
    for group_key, group in groups.items():
        entries = [(p, True) for p in group]
        entries += [(p, False) for p in persisted.get(group_key, [])]
        entries.sort(key=lambda entry: entry[0].start)
        widest, widest_in_batch = entries[0]
        for phrase, in_batch in entries[1:]:
            overlaps = phrase.start < widest.end
            if overlaps and (in_batch or widest_in_batch):
                m = 'same-speaker overlapping phrases: '
                m += f'{phrase.label!r} [{phrase.start}, {phrase.end}] '
                m += f'overlaps {widest.label!r} '
                m += f'[{widest.start}, {widest.end}]; written nothing.'
                raise ValueError(m)
            if phrase.end > widest.end:
                widest, widest_in_batch = phrase, in_batch


def persisted_phrases_by_group(store, groups):
    '''Load the persisted phrases that could overlap the batch,
    grouped by (audio_id, speaker_id). Only phrases starting
    before the audio's last batch end can overlap (keys scan in
    start order); the batch phrases' own rows are skipped by key.'''
    own_keys = set()
    max_end_by_audio = {}
    for (audio_id, _), group in groups.items():
        for phrase in group:
            own_key = key_helper.instance_to_key(phrase)
            own_keys.add(own_key)
        end = max(phrase.end for phrase in group)
        if end > max_end_by_audio.get(audio_id, 0):
            max_end_by_audio[audio_id] = end
    persisted = {}
    for audio_id, max_end in max_end_by_audio.items():
        keys = []
        for key in store.DB.audio_id_to_child_keys(audio_id, 'Phrase'):
            if key_helper.key_to_start(key) >= max_end: break
            if key in own_keys: continue
            keys.append(key)
        for phrase in store.load_many(keys):
            group_key = (audio_id, phrase.speaker_id)
            persisted.setdefault(group_key, []).append(phrase)
    return persisted


def persisted_tree_rows(store, phrase):
    '''Every persisted row of this phrase's tree, as (key,
    label_index_key) pairs: the phrase row plus each Word, Syllable
    and Phone row in its time range attributed to the phrase
    (parent_id for words, phrase_id below), so other speakers' and
    other phrases' rows in range are left alone. Reads raw rows,
    never store.load: the staged tree occupies the cache under these
    same keys, and deletion wants disk truth. The scan runs to the
    PERSISTED row's end, so a re-save with a shrunk end still reaches
    stale descendants starting beyond the staged end.'''
    key = key_helper.instance_to_key(phrase)
    raw = store.DB.load(key)
    if raw is None: return []
    persisted = struct_value.unpack_instance('Phrase', raw)
    label_key = key_helper.label_to_label_index_key(
        persisted['label'], 'Phrase', key)
    rows = [(key, label_key)]
    for child_class in ('Word', 'Syllable', 'Phone'):
        key_iter = store.DB.time_range_keys(phrase.audio_id,
            child_class, phrase.start, persisted['end'])
        child_keys = list(key_iter)
        raw_values = store.DB.load_many(child_keys)
        for child_key, value in zip(child_keys, raw_values):
            fields = struct_value.unpack_instance(child_class, value)
            if child_class == 'Word': owner = fields['parent_id']
            else: owner = fields['phrase_id']
            if owner != phrase.identifier: continue
            label_key = key_helper.label_to_label_index_key(
                fields['label'], child_class, child_key)
            rows.append((child_key, label_key))
    return rows
