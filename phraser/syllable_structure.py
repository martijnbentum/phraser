from .phone_types import PHONE_TYPES

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
