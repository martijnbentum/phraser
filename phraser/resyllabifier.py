import copy
from dataclasses import dataclass

from . import key_helper
from .syllable_structure import assign_syllable_positions_to_phones


def apply_new_syllable_boundaries(word, phone_groups, phone_types=None,
        update_database=False):
    '''Rebuild a word's syllables from a new phone grouping (resyllabification).
    word             the Word whose syllables are being re-segmented
    phone_groups     list of phone lists: the SAME phone objects regrouped
    update_database  if True, persist; else mutate in memory only.

    Resyllabification moves consonants between onset and coda but never adds or
    drops a nucleus, so the count is invariant and new[i] is old[i]'s nucleus
    with moved boundaries. Each new syllable is a COPY of its old one (carrying
    stress_code/tone/speaker/... unchanged) under a fresh id; the word's child
    cache is repointed at them and — with update_database — the old rows and
    their label-index entries are deleted.
    Returns the new syllables. Raises ValueError on a multi-speaker word or a
    count mismatch.
    '''
    _assert_single_speaker(word)
    old_syllables = word.syllables
    if len(old_syllables) != len(phone_groups):
        raise ValueError('resyllabification must preserve the syllable count: '
            f'{len(old_syllables)} syllables vs {len(phone_groups)} groups')
    new_syllables = []
    for old_syllable, phones in zip(old_syllables, phone_groups):
        new_syllables.append(_rebuild_syllable(old_syllable, phones, phone_types))
    word._children, word._related = new_syllables, []   # pair is load-bearing
    if update_database:
        _save_new_syllables(new_syllables)
        _delete_old_syllables(old_syllables)
    return new_syllables


@dataclass
class ResyllabifyOutcome:
    '''What resyllabify_word saw and did.
    result   the analyse_word Result -- the PRE-rewrite analysis (its `current`
             is the old boundaries, the diagnosis of what was wrong).
    applied  whether the boundaries were actually rewritten.
    '''
    result: object
    applied: bool

    @property
    def ok(self):
        '''True if the word's boundaries are correct after this call: either
        they were already correct, or we applied the maximal-onset rewrite.'''
        return self.result.ok or self.applied

    @property
    def count_mismatch(self):
        '''Stored syllable count differs from the suggested count, so the 1:1
        rewrite could not be applied. None-safe.'''
        c, s = self.result.current, self.result.suggested
        return c is not None and s is not None and len(c) != len(s)


def resyllabify_word(word, phone_types=None, update_database=False):
    '''Re-segment a word's syllables by the Maximal Onset Principle, if needed.
    Returns a ResyllabifyOutcome with the analysis and whether it was applied.

    Boundaries are rewritten only when the word is analysable, its current split
    is wrong, and the syllable count is preserved (the 1:1 copy invariant). A
    count change is reported via outcome.count_mismatch and left unapplied
    rather than raising.
    '''
    from dutch_syllabifier import analyse_word
    result = analyse_word(word)
    applied = False
    if not result.ok and result.suggested_groups is not None \
            and len(word.syllables) == len(result.suggested_groups):
        apply_new_syllable_boundaries(word, result.suggested_groups,
            phone_types=phone_types, update_database=update_database)
        applied = True
    return ResyllabifyOutcome(result=result, applied=applied)


# ----------------------------- helpers below ------------------------------

def _assert_single_speaker(word):
    '''Resyllabification works one word at a time, so the word, its syllables
    and their phones must all share one speaker. Raise if they do not.'''
    speaker_ids = {word.speaker_id}
    for syllable in word.syllables:
        speaker_ids.add(syllable.speaker_id)
        for phone in syllable.phones:
            speaker_ids.add(phone.speaker_id)
    if len(speaker_ids) > 1:
        raise ValueError('word, syllables and phones must share one speaker; '
            f'found {len(speaker_ids)} speaker ids')


def _rebuild_syllable(old_syllable, phones, phone_types):
    '''Copy old_syllable onto `phones`: a fresh-id clone carrying every field
    unchanged except label/start/end, with those phones repointed at it and
    onset/nucleus/coda reassigned. The stale loaded `_key` is dropped so the new
    key recomputes from the new id and start.'''
    syllable = copy.copy(old_syllable)
    syllable.identifier = key_helper.make_identifier()
    syllable.label = ' '.join(p.label for p in phones)
    syllable.start = min(p.start for p in phones)
    syllable.end = max(p.end for p in phones)
    if hasattr(syllable, '_key'):
        del syllable._key
    for phone in phones:
        phone.parent_id = syllable.identifier
        phone.parent_start = syllable.start
        phone._parent = syllable
    assign_syllable_positions_to_phones(phones, phone_types=phone_types)
    syllable._children, syllable._related = phones, []
    return syllable


def _save_new_syllables(new_syllables):
    '''Persist the new syllables (fresh keys, so plain saves write their
    label-index links) and their repointed phones.'''
    store = new_syllables[0].store
    for syllable in new_syllables:
        syllable.save()
        store.save_many(syllable.phones, overwrite=True)


def _delete_old_syllables(old_syllables):
    '''Drop the old syllable rows and their label-index entries. The old objects
    were never mutated, so their cached key and computed label-index key still
    address the original rows.'''
    store = old_syllables[0].store
    keys = [s.key for s in old_syllables]
    store.delete_many(keys)
    links = [s.label_index_key for s in old_syllables]
    store.DB.delete_many_label_index_links(links)
