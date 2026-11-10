'''Save TextGrid segments, skipping or replacing phrases already stored.

A phrase is already stored when one with the same audio, speaker and start
exists; the `existing` policy decides whether to add, skip or replace it.
'''

from phraser import models
from phraser.storage.key_helper import EMPTY_ID


TEXTGRID_EXISTING_POLICIES = {'append', 'add_missing', 'replace', 'upsert'}

def require_store(store=None, items=None):
    if store is not None: return store
    if items: return items[0].store
    raise ValueError('store is required')

def save_textgrid_items(items, store=None, existing='append'):
    '''Persist staged TextGrid items according to an existence policy.

    `items` should be the staged objects returned by
    `textgrid_filename_to_database_objects(...)`.

    Supported `existing` values:
    - `append`: no existence check; save staged items directly. Use when the
      caller knows the phrase does not already exist.
    - `add_missing`: run an existence check; save when no matching phrase
      exists, skip when one matching phrase exists.
    - `replace`: run an existence check; require exactly one matching phrase,
      delete that phrase tree, then save the staged items.
    - `upsert`: run an existence check; replace when one matching phrase exists,
      otherwise save the staged items as new.

    Existence checks are scoped to phrases linked to the same audio, then matched
    by Phrase equality: `(audio_id, speaker_id, start)`. Multiple matches raise
    ValueError. The function returns `added`, `skipped`, or `replaced`.
    '''
    validate_textgrid_existing_mode(existing)
    items = list(items)
    store = require_store(store, items)
    if existing == 'append':
        store.save_many(items)
        return 'added'
    phrase = require_single_textgrid_phrase(items)
    matches = find_matching_textgrid_phrases(phrase, store=store)
    validate_textgrid_match_count(matches, phrase)
    if existing == 'add_missing':
        if matches: return 'skipped'
        store.save_many(items)
        return 'added'
    if existing == 'replace':
        if not matches:
            raise ValueError('replace requires one matching existing phrase')
        replace_textgrid_phrase_tree(matches[0], items, store=store)
        return 'replaced'
    if matches:
        replace_textgrid_phrase_tree(matches[0], items, store=store)
        return 'replaced'
    store.save_many(items)
    return 'added'

def validate_textgrid_existing_mode(existing):
    if existing in TEXTGRID_EXISTING_POLICIES: return
    values = ', '.join(sorted(TEXTGRID_EXISTING_POLICIES))
    raise ValueError(f'existing must be one of: {values}')

def require_single_textgrid_phrase(items):
    phrases = get_phrases_from_items(items)
    if len(phrases) == 1: return phrases[0]
    raise ValueError(f'TextGrid items must contain exactly one Phrase; '
        f'got {len(phrases)}')

def find_matching_textgrid_phrases(phrase, store=None):
    store = require_store(store, [phrase])
    if phrase.audio_id == EMPTY_ID:
        raise ValueError('TextGrid existence checks require phrase.audio_id')
    keys = list(store.DB.audio_id_to_child_keys(phrase.audio_id, 'Phrase'))
    phrases = store.load_many(keys)
    return [existing for existing in phrases if existing == phrase]

def validate_textgrid_match_count(matches, phrase):
    if len(matches) <= 1: return
    audio_id = phrase.audio_id.hex()
    speaker_id = phrase.speaker_id.hex()
    m = 'multiple matching phrases found for TextGrid identity '
    m += f'audio_id={audio_id}, speaker_id={speaker_id}, start={phrase.start}'
    raise ValueError(m)

def replace_textgrid_phrase_tree(existing_phrase, items, store=None):
    store = require_store(store, items)
    old_items = list(existing_phrase.items)
    delete_textgrid_items(old_items, store=store)
    store.save_many(items)

def delete_textgrid_items(items, store=None):
    store = require_store(store, items)
    label_index_keys = items_to_label_index_keys(items)
    if label_index_keys:
        store.DB.delete_many_label_index_links(label_index_keys)
    keys = [item.key for item in items]
    if keys: store.delete_many(keys)

def items_to_label_index_keys(items):
    label_index_keys = []
    for item in items:
        try: label_index_keys.append(item.label_index_key)
        except AttributeError: pass
    return label_index_keys

def get_phrases_from_items(items):
    phrases = [item for item in items if isinstance(item, models.Phrase)]
    return phrases
