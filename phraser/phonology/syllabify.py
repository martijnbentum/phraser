'''Maximal-onset (re)syllabification of stored segments.

Syllabification here only regroups phones: phones keep their labels and
times, and each one is reassigned to a syllable. All spans follow from that
regrouping, bottom-up: a syllable spans its phones, and a rebuilt word or
phrase spans its syllables or phones. A syllable boundary is therefore a
split point in the phone sequence (ɑ p | r ɪ l -> ɑ | p r ɪ l moves p to the
second syllable); its time is wherever the phones put it.

repair_*   keep the existing syllables and reassign phones between them
           (same count, same syllable objects and ids, metadata kept);
           skipped if the regrouping would change the syllable count
rebuild_*  discard the existing layer(s) and build fresh objects around the
           regrouped phones (counts and spans may change)

repair_word_syllables(word)         syllables of one word
rebuild_word_syllables(word)        syllables of one word
rebuild_phrase_syllables(phrase)    words + syllables; a phone may move to a
                                    syllable of the neighbouring word
rebuild_phrases_from_phones(phones, max_pause)
                                    phrases + words + syllables; phrases are
                                    re-derived from pauses. Meant for
                                    temporary, in-memory restructuring only.

All functions work in memory only. Persist a repaired or rebuilt phrase with
    store.save_phrase_trees([phrase], overwrite=True)
which replaces the phrase's stored tree (old syllables and words included)
in one transaction; batch the phrases of a corpus run into one call.
rebuild_phrases_from_phones cannot be persisted this way: its phrases are
new objects, so the old phrases would stay on disk.
'''
from dataclasses import dataclass

from .. import models
from .syllable_structure import assign_syllable_positions_to_phones


# ------------------------------- repair ---------------------------------

@dataclass
class RepairOutcome:
    '''What repair_word_syllables saw and did.
    result   the analyse_word Result -- the PRE-repair analysis (its `current`
             is the old boundaries, the diagnosis of what was wrong).
    applied  whether the new boundaries were actually applied.
    '''
    result: object
    applied: bool

    @property
    def ok(self):
        '''True if the word's boundaries are correct after this call: either
        they were already correct, or the maximal-onset boundaries were
        applied.'''
        return self.result.ok or self.applied

    @property
    def count_mismatch(self):
        '''Stored syllable count differs from the suggested count, so the 1:1
        repair could not be applied. None-safe.'''
        c, s = self.result.current, self.result.suggested
        return c is not None and s is not None and len(c) != len(s)


def repair_word_syllables(word, phone_types=None):
    '''Reassign a word's phones to its syllables by the Maximal Onset
    Principle, if needed. Returns a RepairOutcome with the analysis and
    whether the new boundaries were applied.

    The new boundaries are applied only when the word is analysable, its
    current split is wrong, and the syllable count is preserved (the 1:1
    invariant). A count change is reported via outcome.count_mismatch and left
    unapplied rather than raising.
    '''
    from dutch_syllabifier import analyse_word
    result = analyse_word(word)
    applied = False
    if not result.ok and result.suggested_groups is not None \
            and len(word.syllables) == len(result.suggested_groups):
        apply_new_syllable_boundaries(word, result.suggested_groups,
            phone_types=phone_types)
        applied = True
    return RepairOutcome(result=result, applied=applied)


def apply_new_syllable_boundaries(word, phone_groups, phone_types=None):
    '''Apply new syllable boundaries to a word, given as phone groups.
    word          the Word whose syllables get new boundaries
    phone_groups  list of phone lists: the word's SAME phone objects,
                  regrouped; group i becomes syllable i

    Each phone is reassigned to the syllable of its group, and each syllable
    is re-spanned to its phones. Moving phones between onset and coda never
    adds or drops a nucleus, so the count is invariant and syllable i keeps
    its nucleus with new boundaries. The syllables are updated in place: same
    objects and ids, metadata and speaker links unchanged. A syllable whose
    start moves gets a new key; save_phrase_trees(overwrite=True) removes
    the old row. In memory only; see the module docstring for persisting.
    Returns the word's syllables. Raises ValueError on a multi-speaker word or
    a count mismatch.
    '''
    _assert_single_speaker(word)
    syllables = word.syllables
    if len(syllables) != len(phone_groups):
        message = 'new boundaries must preserve the syllable count: '
        message += f'{len(syllables)} syllables vs '
        message += f'{len(phone_groups)} groups'
        raise ValueError(message)
    for syllable, phones in zip(syllables, phone_groups):
        _assign_phones_to_syllable(syllable, phones, phone_types)
    word.replace_children(syllables)
    return syllables


# ------------------------------- rebuild --------------------------------

def rebuild_word_syllables(word, phone_types=None):
    '''Rebuild a word's syllable layer from its phone sequence by the Maximal
    Onset Principle, in memory only (nothing is written).

    Ignores the existing syllables: regroups word.phones from scratch, builds
    fresh Syllable objects around the groups (labelled and spanned by their
    phones), and rewires the hierarchy (word, phrase) around them.
    The old syllables are abandoned.

    In memory only: until the word's phrase is saved with
    save_phrase_trees(overwrite=True), a close/reopen reverts to the old
    syllabification. Returns the new syllables, or None when the phones
    cannot be syllabified (unknown phone / no nucleus).
    '''
    from dutch_syllabifier import resyllabify_phones
    phones = word.phones                              # read before rewiring caches
    try: groups = resyllabify_phones(phones)
    except ValueError: return None

    new_syllables = []
    for group in groups:
        new_syllables.append(_build_syllable(word.store, group, phone_types))
    word.replace_children(new_syllables)
    return new_syllables


def rebuild_phrase_syllables(phrase, phone_types=None):
    '''Connected-speech rebuild of a phrase's word+syllable layers (in memory).

    Single-speaker. Regroups phrase.phones from scratch by the Maximal Onset
    Principle, so a consonant may move to a syllable of the next word (het ei
    -> hE.tEi). Each syllable is owned by the word of its nucleus; fresh
    Words and Syllables are built sized to their content, so every time window
    matches its membership. A word that loses all its phones collapses to a
    zero-width span. The old words/syllables are abandoned.

    In memory only: persist with save_phrase_trees([phrase], overwrite=True).
    Returns the new words, or None when the phones cannot be syllabified
    (unknown phone / no nucleus).
    '''
    from dutch_syllabifier import resyllabify_phones
    try: groups = resyllabify_phones(phrase.phones)
    except ValueError: return None

    phone_word = _phone_to_word(phrase)                 # capture before rewiring
    syllables = [_build_syllable(phrase.store, g, phone_types) for g in groups]
    syls_by_word = _bucket_by_nucleus(syllables, phone_word)

    new_words, cursor = [], phrase.start
    for old_word in phrase.words:
        word = _rebuild_word(phrase, old_word,
            syls_by_word.get(old_word.identifier, []), cursor)
        cursor = word.end                               # next empty word sits here
        new_words.append(word)
    # drop the old word layer in one step; the persisted words stay on
    # disk until the old tree is deleted
    phrase.replace_children(new_words)
    return new_words


def rebuild_phrases_from_phones(phones, max_pause=500, phone_types=None):
    '''Rebuild a whole phrase/word/syllable hierarchy from a phone sequence.

    WARNING -- this RE-DERIVES the phrase layer from silence and DISCARDS the
    original phrasing. The phones are split into runs wherever a gap >= max_pause
    ms occurs, and EACH RUN BECOMES A NEW PHRASE. So two original phrases with a
    short gap merge into one, and one phrase with a long internal pause splits in
    two. Original Phrase/Word/Syllable objects are abandoned and replaced.
    Meant for temporary, in-memory restructuring: the new phrases cannot be
    persisted over the old ones.

    Within a run, connected-speech MOP applies (a consonant may move to a
    syllable of the next word, never across a pause). Each syllable is owned by the
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

    # syllabify every run before rewiring any, so a failing run leaves the
    # in-memory graph untouched
    runs = _split_runs(phones, max_pause)
    groups_per_run = []
    for run in runs:
        try: groups_per_run.append(resyllabify_phones(run))
        except ValueError: return None

    new_phrases = []
    for run, groups in zip(runs, groups_per_run):          # one run -> one phrase
        syllables = [_build_syllable(store, g, phone_types) for g in groups]
        syls_by_word = _bucket_by_nucleus(syllables, phone_word)

        filename = phone_word[run[0].identifier].phrase.filename   # provenance
        phrase = _build_phrase(run, store, filename)        # shell: span/audio/speaker
        cursor, words = phrase.start, []
        for old_word in _words_in_order(run, phone_word):  # incl. emptied words
            word = _rebuild_word(phrase, old_word,
                syls_by_word.get(old_word.identifier, []), cursor)
            cursor = word.end
            words.append(word)
        phrase.replace_children(words)
        phrase.label = ' '.join(w.label for w in words)
        new_phrases.append(phrase)
    return new_phrases



# ------------------------------- helpers --------------------------------

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


def _assign_phones_to_syllable(syllable, phones, phone_types):
    '''Make `phones` the syllable's children, in place: label/start/end are
    re-derived from them, every other field is kept, and onset/nucleus/coda
    are reassigned. The stale loaded `_key` is dropped so the key recomputes
    from the new start.'''
    syllable.label = ' '.join(p.label for p in phones)
    syllable.start = min(p.start for p in phones)
    syllable.end = max(p.end for p in phones)
    if hasattr(syllable, '_key'):
        del syllable._key
    syllable.replace_children(phones)
    assign_syllable_positions_to_phones(phones, phone_types=phone_types)
    return syllable


def _build_syllable(store, group, phone_types):
    '''Fresh Syllable wrapping a phone group; phones re-parented and positions
    assigned. Not yet wired to a word/phrase -- the caller owns that.'''
    syllable = models.Syllable(store=store, save=False,
        label=' '.join(p.label for p in group),
        start=min(p.start for p in group), end=max(p.end for p in group),
        audio_id=group[0].audio_id, speaker_id=group[0].speaker_id)
    # replace_children, not add_children: it starts from empty caches, so the
    # new syllable never scans the store for the old phones in its range
    syllable.replace_children(group)
    assign_syllable_positions_to_phones(group, phone_types=phone_types)
    return syllable


def _rebuild_word(phrase, old_word, syllables, cursor):
    '''Fresh UNLINKED Word replacing `old_word`, sized to its syllables; the
    caller wires it to the phrase (add_children). A word that lost all its
    phones collapses to a zero-width span at `cursor` (the previous word's new
    end -- the point its phones vacated, where its neighbours meet).'''
    start, end = ((min(s.start for s in syllables), max(s.end for s in syllables))
        if syllables else (cursor, cursor))
    word = models.Word(store=phrase.store, label=old_word.label,
        start=start, end=end, audio_id=old_word.audio_id,
        speaker_id=old_word.speaker_id)
    # carry set metadata; overlap_code is left unset because the span changed
    for field, value in old_word.__dict__.items():
        if field == 'overlap_code': continue
        if field in models.Word.METADATA_FIELDS: setattr(word, field, value)
    word.replace_children(syllables)
    return word


def _build_phrase(run, store, filename):
    '''Fresh Phrase sized to the run, carrying audio/speaker from it and the
    given filename for provenance.'''
    phrase = models.Phrase(store=store, save=False, label='',
        start=run[0].start, end=run[-1].end, filename=filename,
        audio_id=run[0].audio_id, speaker_id=run[0].speaker_id)
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
