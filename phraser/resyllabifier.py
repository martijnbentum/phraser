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
    span its new phones, relabels it from those phones (' '.join of their
    labels) so .label stays in sync with the new grouping, relinks every phone's
    stored parent pointer, and reassigns onset/nucleus/coda. Returns the mutated
    syllables. Raises ValueError on a length mismatch or an empty group.

    When update_database is True, the stale label-index entry for each syllable's
    old label/key is removed and the new one written, so a label lookup never
    resolves to a moved-or-relabelled syllable's old form.
    '''
    if len(syllables) != len(groups):
        raise ValueError('syllable count does not match group count')
    changed = []
    for syllable, group in zip(syllables, groups):
        if not group:
            raise ValueError('cannot assign an empty phone group')
        old_key = syllable.key
        old_label_index_key = syllable.label_index_key  # OLD label + OLD key
        syllable.start = min(p.start for p in group)
        syllable.end = max(p.end for p in group)
        syllable.label = ' '.join(p.label for p in group)  # keep label in sync
        for phone in group:                         # keep the stored pointer
            phone.parent_id = syllable.identifier   # consistent with the new
            phone.parent_start = syllable.start      # time window
        assign_syllable_positions_to_phones(group, phone_types=phone_types)
        # Refill the navigation caches to the new grouping rather than dropping
        # them. Both views then agree in memory without a write: the time-scan
        # (syllable.phones via _children) and the stored parent pointer
        # (phone.parent via _parent). Dropping instead would force phone.parent
        # to re-resolve through store.load(parent_key) at the moved syllable's
        # new key -- absent from the cache and disk until update_database=True.
        # These caches hold object references, so they survive a later re-key.
        sid = syllable.speaker_id
        syllable._children = [p for p in group if p.speaker_id == sid]
        syllable._related = [p for p in group if p.speaker_id != sid]
        for phone in group:
            phone._parent = syllable
        changed.append((syllable, old_key, old_label_index_key, group))
    if update_database and changed:
        store = syllables[0].store
        segments = []
        for syllable, old_key, old_label_index_key, group in changed:
            _mark_syllable_write(syllable, old_key)
            for phone in group:
                phone._save_status = 'save'         # own key is stable
            segments.append(syllable)
            segments.extend(group)
        model_helper.write_changes_to_db(segments, store)
        # save() wrote each syllable's NEW label-index entry; drop the stale OLD
        # ones (old label and/or old key), but never one we just rewrote
        # identically (a syllable whose label and key both stayed put).
        new_links = {syllable.label_index_key for syllable, *_ in changed}
        stale = [old for _, _, old, _ in changed if old not in new_links]
        store.DB.delete_many_label_index_links(stale)
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
