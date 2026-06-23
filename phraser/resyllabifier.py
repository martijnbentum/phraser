from . import model_helper
from .syllable_structure import assign_syllable_positions_to_phones


def apply_syllable_groups(syllables, groups, phone_types=None,
        update_database=False):
    '''Rewrite syllable boundaries from a new phone grouping.
    syllables        existing Syllable objects, in order (e.g. word.syllables)
    groups           list of phone lists: the SAME phone objects regrouped
                     (e.g. dutch_syllabifier.resyllabify_phones(word.phones))
    update_database  if True, persist the change in one batched write; default
                     False, mutating the objects in memory only (nothing is
                     written).

    Repartitions phones among the existing syllables: retimes each syllable to
    span its new phones, relinks every phone's stored parent pointer, and
    reassigns onset/nucleus/coda. Returns the mutated syllables.
    Raises ValueError on a length mismatch or an empty group.
    '''
    if len(syllables) != len(groups):
        raise ValueError('syllable count does not match group count')
    changed = []
    for syllable, group in zip(syllables, groups):
        if not group:
            raise ValueError('cannot assign an empty phone group')
        old_key = syllable.key
        syllable.start = min(p.start for p in group)
        syllable.end = max(p.end for p in group)
        for phone in group:                         # keep the stored pointer
            phone.parent_id = syllable.identifier   # consistent with the new
            phone.parent_start = syllable.start      # time window
        assign_syllable_positions_to_phones(group, phone_types=phone_types)
        changed.append((syllable, old_key, group))
    for syllable in syllables:                      # drop stale child/parent
        for attr in ('_children', '_related', '_parent'):  # caches
            syllable.__dict__.pop(attr, None)
    if update_database and changed:
        segments = []
        for syllable, old_key, group in changed:
            _mark_syllable_write(syllable, old_key)
            for phone in group:
                phone._save_status = 'save'         # own key is stable
            segments.append(syllable)
            segments.extend(group)
        model_helper.write_changes_to_db(segments, syllables[0].store)
    return syllables


def _mark_syllable_write(syllable, old_key):
    '''Flag a retimed syllable for the right write path: 'update' when its key
    changed (start moved), otherwise a plain overwrite 'save'.'''
    if syllable.key != old_key:
        syllable._save_status = 'update'
        syllable._old_key = old_key
    else:
        syllable._save_status = 'save'


def resyllabify_word(word, phone_types=None, update_database=False):
    '''Re-segment a word's syllables by the Maximal Onset Principle, if needed.
    update_database  if True, persist the rewrite; default False (in memory).

    Returns True if boundaries were rewritten, False if already correct or the
    word could not be analysed (unknown phone / no nucleus).
    '''
    from dutch_syllabifier import analyse_word
    result = analyse_word(word)
    if result.ok or result.suggested_groups is None:
        return False
    apply_syllable_groups(word.syllables, result.suggested_groups,
        phone_types=phone_types, update_database=update_database)
    return True
