from dataclasses import dataclass

from . import model_helper
from .syllable_structure import assign_syllable_positions_to_phones


def apply_syllable_groups(syllables, groups, phone_types=None,
        update_database=False):
    '''Rewrite syllable boundaries from a new phone grouping.
    syllables        existing Syllable objects, in order (e.g. word.syllables)
    groups           list of phone lists: the SAME phone objects regrouped
                     (e.g. dutch_syllabifier.resyllabify_phones(word.phones))
    update_database  if True, persist the change in one batched write; default
                     False, mutating the objects in memory only.

    Each syllable is retimed and relabelled to span its new phones; with
    update_database the rows and the moved label-index entries are persisted.
    Returns the mutated syllables. Raises ValueError on a length mismatch or an
    empty group.
    '''
    if len(syllables) != len(groups):
        raise ValueError('syllable count does not match group count')
    retimed = [_retime_syllable(syllable, phones, phone_types)
        for syllable, phones in zip(syllables, groups)]
    if update_database and retimed:
        _save_changed_objects_to_store(retimed, syllables[0].store)
    return syllables


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


# ----------------------------- helpers below ------------------------------

def _retime_syllable(syllable, phones, phone_types):
    '''Rewrite one syllable to span `phones`: retime, relabel, repoint each
    phone's stored parent, assign onset/nucleus/coda, and refresh the in-memory
    caches. Returns a _Retimed holding the pre-mutation keys for persistence.'''
    if not phones:
        raise ValueError('cannot assign an empty phone group')
    record = _Retimed(syllable, syllable.key, syllable.label_index_key, phones)
    syllable.start = min(p.start for p in phones)
    syllable.end = max(p.end for p in phones)
    syllable.label = ' '.join(p.label for p in phones)
    for phone in phones:
        phone.parent_id = syllable.identifier
        phone.parent_start = syllable.start
    assign_syllable_positions_to_phones(phones, phone_types=phone_types)
    _refresh_syllable_phone_caches(syllable, phones)
    return record


def _refresh_syllable_phone_caches(syllable, phones):
    '''Point the syllable<->phone navigation caches at the new grouping in
    memory, so the time-scan (syllable.phones) and the stored parent pointer
    (phone.parent) agree before the moved syllable's new key is ever written.'''
    sid = syllable.speaker_id
    syllable._children = [p for p in phones if p.speaker_id == sid]
    syllable._related = [p for p in phones if p.speaker_id != sid]
    for phone in phones:
        phone._parent = syllable


def _save_changed_objects_to_store(retimed, store):
    '''Write each retimed syllable and its phones, then drop the label-index
    entries no syllable occupies any more.
    retimed   list of _Retimed records from _retime_syllable.

    Both the syllable (new start/end/label, hence possibly a new key) and its
    phones (repointed parent_id/parent_start) changed, so both are written.
    '''
    segments = []
    for r in retimed:
        _flag_syllable_write_path(r.syllable, r.old_key)
        for phone in r.phones:
            phone._save_status = 'save'         # own key is stable
        segments.append(r.syllable)
        segments.extend(r.phones)
    model_helper.write_changes_to_db(segments, store)
    store.DB.delete_many_label_index_links(_collect_stale_label_links(retimed))


def _collect_stale_label_links(retimed):
    '''Old label-index keys no syllable uses after the rewrite (save() wrote the
    new ones). A syllable whose label and key were unchanged keeps its entry.
    retimed   list of _Retimed records from _retime_syllable.'''
    new_links = {r.syllable.label_index_key for r in retimed}
    stale = []
    for r in retimed:
        if r.old_label_index_key not in new_links:
            stale.append(r.old_label_index_key)
    return stale


def _flag_syllable_write_path(syllable, old_key):
    '''Pick how the retimed syllable is written: 'update' (delete old key, save
    new) when its start moved and changed the key, else a plain overwrite
    'save'.'''
    if syllable.key != old_key:
        syllable._save_status = 'update'
        syllable._old_key = old_key
    else:
        syllable._save_status = 'save'


@dataclass
class _Retimed:
    '''A syllable whose boundaries were rewritten, with what persisting it needs:
    its OLD main key and OLD label-index key (captured before mutation) and the
    phones now in it.'''
    syllable: object
    old_key: bytes
    old_label_index_key: bytes
    phones: list
