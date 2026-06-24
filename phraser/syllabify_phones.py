'''Rebuild syllable structure from a phone sequence by the Maximal Onset
Principle (the syllabify_* family).

Unlike resyllabifier.py -- which keeps the existing syllables and only fixes
their boundaries -- these functions IGNORE the current structure and build fresh
Syllable (and, higher up, Word/Phrase) objects from the phones, abandoning the
old ones. They operate in memory only; persisting the result (saving the new
objects and deleting the abandoned ones) is a separate concern.

  syllabify_word    rebuild one word's syllables (keeps the word)
  syllabify_phrase  rebuild a phrase's word+syllable layers, connected-speech
                    cross-word MOP, keeps the phrase
  syllabify_phones  rebuild a whole phrase/word/syllable hierarchy from a raw
                    phone sequence, RE-DERIVING phrases from pauses
'''
from . import models
from .syllable_structure import assign_syllable_positions_to_phones


def syllabify_word(word, phone_types=None):
    '''Rebuild a word's syllable layer from its phone sequence by the Maximal
    Onset Principle, in memory only (nothing is written).

    Ignores the existing syllables: syllabifies word.phones from scratch, builds
    fresh Syllable objects labelled by their phones, and rewires the hierarchy
    (word, phrase, audio, speaker) around them through the model's own helpers.
    The old syllables are abandoned; deleting them from the store is a separate,
    persistent-mode concern.

    The new syllables have no LMDB presence, so the live graph is held together
    by the navigation caches filled here -- a close/reopen without persisting
    reverts to the old syllabification. Returns the new syllables, or None when
    the phones cannot be syllabified (unknown phone / no nucleus).
    '''
    from dutch_syllabifier import resyllabify_phones
    phones = word.phones                              # read before rewiring caches
    try: groups = resyllabify_phones(phones)
    except ValueError: return None

    phrase, new_syllables = word.phrase, []
    for group in groups:
        syllable = _build_syllable(word.store, group, phone_types)
        syllable.add_parent(word, update_database=False)     # word + audio + speaker
        syllable._add_phrase(phrase, update_database=False)
        new_syllables.append(syllable)

    word._children, word._related = new_syllables, []   # load-bearing: not on disk
    return new_syllables


def syllabify_phrase(phrase, phone_types=None):
    '''Connected-speech rebuild of a phrase's word+syllable layers (in memory).

    Single-speaker. Syllabifies phrase.phones from scratch by the Maximal Onset
    Principle, so a consonant may resyllabify across a word boundary (het ei ->
    hE.tEi). Each syllable is owned by the word of its nucleus; fresh Words and
    Syllables are built sized to their content, so every time window matches its
    membership. A word that loses all its phones collapses to a zero-width span.
    The old words/syllables are abandoned (deletion is a persistent-mode concern).

    The new objects have no LMDB presence, so the live graph is held together by
    the navigation caches filled here. Returns the new words, or None when the
    phones cannot be syllabified (unknown phone / no nucleus).
    '''
    from dutch_syllabifier import resyllabify_phones
    try: groups = resyllabify_phones(phrase.phones)
    except ValueError: return None

    phone_word = _phone_to_word(phrase)                 # capture before rewiring
    syllables = [_build_syllable(phrase.store, g, phone_types) for g in groups]
    syls_by_word = _bucket_by_nucleus(syllables, phone_word)

    new_words, cursor = [], phrase.start
    for old in phrase.words:
        word = _build_word(phrase, old, syls_by_word.get(old.identifier, []), cursor)
        cursor = word.end                               # next empty word sits here
        new_words.append(word)
    phrase._children, phrase._related = new_words, []   # load-bearing: not on disk
    return new_words


def syllabify_phones(phones, max_pause=500, phone_types=None):
    '''Rebuild a whole phrase/word/syllable hierarchy from a phone sequence.

    WARNING -- this RE-DERIVES the phrase layer from silence and DISCARDS the
    original phrasing. The phones are split into runs wherever a gap >= max_pause
    ms occurs, and EACH RUN BECOMES A NEW PHRASE. So two original phrases with a
    short gap merge into one, and one phrase with a long internal pause splits in
    two. Original Phrase/Word/Syllable objects are abandoned and replaced.

    Within a run, connected-speech MOP applies (a consonant may resyllabify
    across a word boundary, never across a pause). Each syllable is owned by the
    word of its nucleus; words and phrases are sized to their content; a word
    that loses all its phones collapses to a zero-width span. In memory only.

    Accepts a phone list or any segment exposing .phones. Raises ValueError if
    the phones span more than one speaker or more than one audio file. Returns
    the new phrases, or None if any run cannot be syllabified.
    '''
    from dutch_syllabifier import resyllabify_phones
    if hasattr(phones, 'phones'): phones = phones.phones    # segment -> its phones
    phones = sorted(phones, key=lambda p: p.start)
    if not phones: return None
    if len({p.speaker_id for p in phones}) > 1:
        raise ValueError('phones span multiple speakers')
    if len({p.audio_id for p in phones}) > 1:
        raise ValueError('phones span multiple audio files')

    phone_word = {p.identifier: next(p.iter_ancestors_of_type(models.Word))
        for p in phones}                                   # capture before rewiring
    store = phones[0].store

    new_phrases = []
    for run in _split_runs(phones, max_pause):              # one run -> one phrase
        try: groups = resyllabify_phones(run)
        except ValueError: return None                     # atomic fail (no orphans)
        syllables = [_build_syllable(store, g, phone_types) for g in groups]
        syls_by_word = _bucket_by_nucleus(syllables, phone_word)

        filename = phone_word[run[0].identifier].phrase.filename   # provenance
        phrase = _build_phrase(run, store, filename)        # shell: span/audio/speaker
        cursor, words = phrase.start, []
        for old in _words_in_order(run, phone_word):       # incl. emptied words
            word = _build_word(phrase, old, syls_by_word.get(old.identifier, []), cursor)
            cursor = word.end
            words.append(word)
        phrase._children, phrase._related = words, []
        phrase.label = ' '.join(w.label for w in words)
        new_phrases.append(phrase)
    return new_phrases


def _build_syllable(store, group, phone_types):
    '''Fresh Syllable wrapping a phone group; phones re-parented and positions
    assigned. Not yet wired to a word/phrase -- the caller owns that.'''
    syllable = models.Syllable(store=store, save=False,
        label=' '.join(p.label for p in group),
        start=min(p.start for p in group), end=max(p.end for p in group))
    syllable._children, syllable._related = group, []   # fill before wiring
    for phone in group: phone.add_parent(syllable, update_database=False)
    assign_syllable_positions_to_phones(group, phone_types=phone_types)
    return syllable


def _build_word(phrase, old, syllables, cursor):
    '''Fresh Word for lexical word `old`, sized to its syllables. A word that
    lost all its phones collapses to a zero-width span at `cursor` (the previous
    word's new end -- the point its phones vacated, where its neighbours meet).'''
    start, end = ((min(s.start for s in syllables), max(s.end for s in syllables))
        if syllables else (cursor, cursor))
    word = models.Word(store=phrase.store, save=False, label=old.label,
        start=start, end=end)
    for field, value in old.__dict__.items():           # carry ipa/pos/freq/...
        if field in models.Word.METADATA_FIELDS:        # set ones only (skips the
            setattr(word, field, value)                 # derived 'overlap' property)
    word._children, word._related = syllables, []
    word.add_parent(phrase, update_database=False)       # phrase + audio + speaker
    for syllable in syllables:
        syllable.add_parent(word, update_database=False)
        syllable._add_phrase(phrase, update_database=False)
    return word


def _build_phrase(run, store, filename):
    '''Fresh Phrase sized to the run, carrying audio/speaker from it and the
    given filename for provenance. Has no parent (top-level), so audio/speaker
    are set explicitly; the words built under it inherit them via add_parent.'''
    phrase = models.Phrase(store=store, save=False, label='',
        start=run[0].start, end=run[-1].end, filename=filename)
    phrase.add_audio(audio_id=run[0].audio_id, update_database=False, propagate=False)
    phrase.add_speaker(speaker_id=run[0].speaker_id, update_database=False,
        propagate=False)
    return phrase


def _phone_to_word(phrase):
    '''Map each phone to its current (lexical) word, before any rewiring.'''
    return {p.identifier: w for w in phrase.words for p in w.phones}


def _bucket_by_nucleus(syllables, phone_word):
    '''Group syllables under the word that owns each syllable's nucleus phone.'''
    buckets = {}
    for syllable in syllables:
        nucleus = next(p for p in syllable.phones if p.position == 'nucleus')
        owner = phone_word[nucleus.identifier]
        buckets.setdefault(owner.identifier, []).append(syllable)
    return buckets


def _split_runs(phones, max_pause):
    '''Split time-ordered phones at silences >= max_pause (phrases by pause).'''
    runs, run = [], []
    for p in phones:
        if run and p.start - run[-1].end >= max_pause:
            runs.append(run); run = []
        run.append(p)
    if run: runs.append(run)
    return runs


def _words_in_order(run, phone_word):
    '''Original words contributing phones to the run, in time order (an emptied
    word still appears, so it gets a zero-width placeholder).'''
    seen, words = set(), []
    for p in run:
        w = phone_word[p.identifier]
        if w.identifier not in seen:
            seen.add(w.identifier); words.append(w)
    return words
