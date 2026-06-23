from . import model_helper
from . import phone_features

def assign_phone_positions(target, phone_types=None, update_database=True):
    '''Assign onset/nucleus/coda to every phone under `target`.

    target  a syllable, word, or phrase — or a list mixing any of these.
            Words and phrases are expanded to their syllables; positions are
            always assigned one syllable at a time, since the rule "onset*
            nucleus+ coda*" only holds within a single syllable.
    phone_types  optional {label: 'vowel'|'consonant'|'other'} mapping used to
            locate the nucleus; defaults to the module-level PHONE_TYPES.
    update_database  if True (default), persist every touched phone in one
            batched write; if False, mutate phones in memory only.

    Returns `target`. Raises ValueError if a phone label is unknown or a
    syllable's vowels are not consecutive.
    '''
    phones_to_save = []
    for syllable in _object_to_syllables(target):
        phones = syllable.phones
        if not phones: continue
        assign_syllable_positions_to_phones(phones, phone_types=phone_types)
        phones_to_save.extend(phones)
    if update_database and phones_to_save:
        phones_to_save[0].store.save_many(phones_to_save, overwrite=True)
    return target

def _object_to_syllables(target):
    '''Flatten a syllable / word / phrase, or a list of them, to syllables.'''
    items = target if isinstance(target, (list, tuple)) else [target]
    syllables = []
    for item in items:
        if item.object_type == 'Syllable':
            syllables.append(item)
        else:                          # Word or Phrase
            syllables.extend(item.syllables)
    return syllables

def assign_syllable_positions_to_phones(phones, phone_types=None):
    '''Assign onset/nucleus/coda to an ordered list of phones from ONE syllable,
    in-memory (no database write). Raises ValueError if a phone label is unknown
    or vowels are not consecutive.'''
    if not phones: return
    vowel_indices = phones_to_vowel_indices(phones, phone_types=phone_types)
    for i, phone in enumerate(phones):
        if not vowel_indices: phone.position = 'onset'
        elif i in vowel_indices: phone.position = 'nucleus'
        elif i < vowel_indices[0]: phone.position = 'onset'
        elif i > vowel_indices[-1]: phone.position = 'coda'

def phones_to_vowel_indices(phones, phone_types=None):
    '''Return the indices of vowel phones in the list.
    Raises ValueError if a label is missing from phone_types or vowels are not 
    consecutive.'''
    pt = phone_types or PHONE_TYPES
    vowel_indices = [] 
    for i, p in enumerate(phones):
        if p.label not in pt:
            raise ValueError(f'Phone label {p.label} not found in phone types')
        if pt[p.label] == 'vowel':
            vowel_indices.append(i)
    if not check_consecutive_numbers(vowel_indices):
        raise ValueError(f'Vowel indices {vowel_indices} are not consecutive')
    return vowel_indices

def check_consecutive_numbers(numbers):
    '''Return True if all numbers form a consecutive sequence with no gaps.'''
    for i in range(len(numbers) - 1):
        if numbers[i] + 1 != numbers[i + 1]:
            return False
    return True


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


OTHER_PHONE_LABELS = ('', '(..)')


def load_phone_types():
    '''Return {label: 'vowel'|'consonant'} from ipa_features.json, plus the
    non-speech placeholder labels mapped to 'other'.'''
    phone_types = {}
    for label, info in phone_features.load_ipa_features().items():
        phone_types[label] = info['type']
    for label in OTHER_PHONE_LABELS:
        phone_types[label] = 'other'
    return phone_types


PHONE_TYPES = load_phone_types()
