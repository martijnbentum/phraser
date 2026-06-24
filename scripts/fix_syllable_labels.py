'''Repair syllables whose stored .label no longer matches their phone children.

A phraser Syllable resolves its .phones by a time-range scan over its
[start, end] window, independent of the stored .label string. After phones are
regrouped or re-parented without the syllable's .label being rewritten, a
syllable can keep a stale label -- label "r u" while .phones is [r, u, t].

Each syllable is judged on its own; no word/phrase hierarchy is walked.

Persistence note: a syllable's .label is NOT part of its main storage key
(key = audio, rank, start, identifier), so a label fix is a plain overwrite of
the record. The label IS part of the secondary label-index key
(rank + hash(label) + main_key), so the stale entry for the old label must be
removed explicitly; save() only adds the entry for the new label. Because the
full main key (with the unique identifier) is appended, removing one syllable's
entry never touches another syllable that happens to share the same label.
'''
from phraser import model_helper


def syllable_label_from_phones(syllable):
    '''Ground-truth label for a syllable: its phone children joined in order.

    syllable.phones is time-ordered, so this is the label the syllable should
    carry for its current phone grouping.
    '''
    return ' '.join(phone.label for phone in syllable.phones)


def has_label_phones_mismatch(syllable):
    '''True if the stored .label disagrees with the phone-derived label.'''
    return syllable.label != syllable_label_from_phones(syllable)


def syllables_with_label_phones_mismatch(syllables):
    '''Return the syllables whose .label disagrees with their .phones.

    syllables   an iterable of Syllable objects (NOT an ancestor segment); each
                syllable is judged on its own, no hierarchy is walked.
    '''
    return [s for s in syllables if has_label_phones_mismatch(s)]


def fix_syllable_labels(syllables, update_database=False):
    '''Rewrite each faulty syllable's label from its phone children.

    syllables        an iterable of Syllable objects to check/repair
    update_database  if True, persist the corrected labels in one batched write
                     and drop the stale label-index entry for each old label;
                     default False (in memory only -- nothing is written).

    Returns the list of syllables whose label was changed. Each stale label-index
    key is captured from syllable.label_index_key BEFORE the label is reassigned,
    so it addresses exactly this syllable's old entry (label hash + this main
    key); syllables sharing the same label string keep their own entries.
    '''
    changed, stale_label_links = [], []
    for syllable in syllables:
        if not has_label_phones_mismatch(syllable):
            continue
        stale_label_links.append(syllable.label_index_key)   # built from OLD label
        syllable.label = syllable_label_from_phones(syllable)
        syllable._save_status = 'save'        # main key stable: label not in it
        changed.append(syllable)
    if update_database and changed:
        store = changed[0].store
        model_helper.write_changes_to_db(changed, store)     # record + NEW label link
        # drop the OLD label-index entries in one batched write (helper TBD)
        store.DB.delete_many_label_index_links(stale_label_links)
    return changed
