

def assign_phrases_phone_positions(phrases, phone_types=None, update_database=True):
    '''Assign onset/nucleus/coda positions to all phones across a list of phrases
    with a single database write.'''
    all_phones = []
    for phrase in phrases:
        assign_phrase_phone_positions(phrase, phone_types=phone_types,
            update_database=False)
        all_phones.extend(phrase.phones)
    if update_database and all_phones:
        cache = all_phones[0].__class__.get_default_cache()
        cache.save_many(all_phones, overwrite=True)

def assign_phrase_phone_positions(phrase, phone_types=None, update_database=True):
    '''Assign onset/nucleus/coda positions to all phones in a phrase
    with a single database write.'''
    for syllable in phrase.syllables:
        assign_phone_positions(syllable, phone_types=phone_types,
            update_database=False)
    if update_database:
        phones = phrase.phones
        if phones:
            cache = phones[0].__class__.get_default_cache()
            cache.save_many(phones, overwrite=True)

def assign_phone_positions(syllable, phone_types=None, update_database=True):
    '''Assign onset/nucleus/coda position to each phone in a syllable.
    Raises ValueError if a phone label is unknown or vowels are not consecutive.'''
    phones = syllable.phones
    if not phones:return
    vowel_indices = phones_to_vowel_indices(phones, phone_types=phone_types)
    for i, phone in enumerate(phones):
        if not vowel_indices: phone.position = 'onset'
        elif i in vowel_indices: phone.position = 'nucleus'
        elif i < vowel_indices[0]: phone.position = 'onset'
        elif i > vowel_indices[-1]: phone.position = 'coda'
    if update_database:
        cache = phones[0].__class__.get_default_cache()
        cache.save_many(phones, overwrite=True)

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


PHONE_TYPES = {
    "aː": "vowel",
    "b": "consonant",
    "d": "consonant",
    "eː": "vowel",
    "f": "consonant",
    "h": "consonant",
    "i": "vowel",
    "j": "consonant",
    "k": "consonant",
    "l": "consonant",
    "m": "consonant",
    "n": "consonant",
    "oː": "vowel",
    "p": "consonant",
    "r": "consonant",
    "s": "consonant",
    "t": "consonant",
    "u": "vowel",
    "ui": "vowel",
    "v": "consonant",
    "w": "consonant",
    "x": "consonant",
    "y": "vowel",
    "z": "consonant",
    "øː": "vowel",
    "ŋ": "consonant",
    "œy": "vowel",
    "œː": "vowel",
    "ɑ": "vowel",
    "ɑu": "vowel",
    "ɔ": "vowel",
    "ɔː": "vowel",
    "ə": "vowel",
    "ɛ": "vowel",
    "ɛi": "vowel",
    "ɛː": "vowel",
    "ɡ": "consonant",
    "ɣ": "consonant",
    "ɪ": "vowel",
    "ɲ": "consonant",
    "ʃ": "consonant",
    "ʏ": "vowel",
    "": "other",
}
