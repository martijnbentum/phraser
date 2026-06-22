from . import model_helper
from . import phone_features


def assign_phrases_phone_positions(phrases, phone_types=None, update_database=True):
    '''Assign onset/nucleus/coda positions to all phones across a list of phrases
    with a single database write.'''
    all_phones = []
    for phrase in phrases:
        assign_phrase_phone_positions(phrase, phone_types=phone_types,
            update_database=False)
        all_phones.extend(phrase.phones)
    if update_database and all_phones:
        store = all_phones[0].store
        store.save_many(all_phones, overwrite=True)

def assign_phrase_phone_positions(phrase, phone_types=None, update_database=True):
    '''Assign onset/nucleus/coda positions to all phones in a phrase
    with a single database write.'''
    for syllable in phrase.syllables:
        assign_phone_positions(syllable, phone_types=phone_types,
            update_database=False)
    if update_database:
        phones = phrase.phones
        if phones:
            store = phones[0].store
            store.save_many(phones, overwrite=True)

def assign_positions_to_phones(phones, phone_types=None):
    '''Assign onset/nucleus/coda to an ordered list of phones in-memory
    (no database write). Raises ValueError if a phone label is unknown or
    vowels are not consecutive.'''
    if not phones: return
    vowel_indices = phones_to_vowel_indices(phones, phone_types=phone_types)
    for i, phone in enumerate(phones):
        if not vowel_indices: phone.position = 'onset'
        elif i in vowel_indices: phone.position = 'nucleus'
        elif i < vowel_indices[0]: phone.position = 'onset'
        elif i > vowel_indices[-1]: phone.position = 'coda'

def assign_phone_positions(syllable, phone_types=None, update_database=True):
    '''Assign onset/nucleus/coda position to each phone in a syllable.
    Raises ValueError if a phone label is unknown or vowels are not consecutive.'''
    phones = syllable.phones
    if not phones: return
    assign_positions_to_phones(phones, phone_types=phone_types)
    if update_database:
        store = phones[0].store
        store.save_many(phones, overwrite=True)

def phones_to_vowel_indices(phones, phone_types=None):
    '''Return the indices of vowel phones in the list.
    Raises ValueError if a label is missing from phone_types or vowels are not consecutive.'''
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
        assign_positions_to_phones(group, phone_types=phone_types)
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
