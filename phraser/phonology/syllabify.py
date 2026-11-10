'''Maximal-onset (re)syllabification of stored segments.

Syllabification here only regroups phones: phones keep their labels and
times, and each one is reassigned to a syllable. All spans follow from that
regrouping, bottom-up: a syllable spans its phones, and a rebuilt word or
phrase spans its syllables or phones. A syllable boundary is therefore a
split point in the phone sequence (ɑ p | r ɪ l -> ɑ | p r ɪ l moves p to the
second syllable); its time is wherever the phones put it.

The grouping comes from a language syllabifier. Only Dutch
(dutch_syllabifier) is implemented; other languages raise
NotImplementedError. apply_new_syllable_boundaries takes a grouping from any
source. Onset/nucleus/coda positions use phone_types.PHONE_TYPES, which
agrees with the syllabifier on every phone it accepts.

repair_*   change the donor store's syllables in place: phones move between
           the existing syllables (same count, same objects and ids, metadata
           kept); skipped if the syllable count would change. Persist with
           store.save_phrase_trees([phrase], overwrite=True).
derive_*   leave the donor untouched and build an alternative segmentation
           on copies (counts and spans may change). With store=None the
           copies live in memory only and cannot be saved; with store=target
           they are saved to that store, which must not be the donor store.

repair_word_syllables(word)                        in place: syllables
apply_new_syllable_boundaries(word, phone_groups)  in place, any grouping
derive_syllables_for_word(word)                    new: syllables
derive_words_and_syllables_for_phrase(phrase)      new: words + syllables; a
                                                   phone may move to a
                                                   syllable of the next word
derive_phrases_for_phones(phones, max_pause)       new: phrases + words +
                                                   syllables; phrases are
                                                   re-derived from pauses

Derived objects keep the donor id when they correspond one-to-one to a donor
object (same id means "corresponds to", not "identical to"): phones always;
words unless a pause splits them; syllables that take their nucleus from a
donor syllable no other new syllable draws from; phrases with exactly one
donor phrase's phones. Other derived objects get an id hashed from their
phones, so a re-run gives the same ids and overwrites its earlier result in
the target store. Different settings (e.g. max_pause) give overlapping
phrases, so save each setting to its own target store.
'''
import hashlib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import dutch_syllabifier

from .. import models
from . import syllable_structure


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


def repair_word_syllables(word, language='dutch'):
    '''Reassign a word's phones to its syllables by the Maximal Onset
    Principle, if needed. Returns a RepairOutcome with the analysis and
    whether the new boundaries were applied.
    word      the Word to repair, in place
    language  syllabifier language; only 'dutch' is implemented

    The new boundaries are applied only when the word is analysable, its
    current split is wrong, and the syllable count is preserved (the 1:1
    invariant). A count change is reported via outcome.count_mismatch and left
    unapplied rather than raising.
    '''
    result = _syllabifier(language).analyse_word(word)
    applied = False
    if not result.ok and result.suggested_groups is not None \
            and len(word.syllables) == len(result.suggested_groups):
        apply_new_syllable_boundaries(word, result.suggested_groups)
        applied = True
    return RepairOutcome(result=result, applied=applied)


def apply_new_syllable_boundaries(word, phone_groups):
    '''Apply new syllable boundaries to a word, given as phone groups.
    word          the Word whose syllables get new boundaries
    phone_groups  list of phone lists: the word's SAME phone objects,
                  regrouped; group i becomes syllable i

    The entry point for repairing with a grouping of your own (a manual fix
    or another syllabifier). Each phone is reassigned to the syllable of its
    group, and each syllable is re-spanned to its phones. Moving phones
    between onset and coda never adds or drops a nucleus, so the count is
    invariant and syllable i keeps its nucleus with new boundaries. The
    syllables are updated in place: same objects and ids, metadata and
    speaker links unchanged. A syllable whose start moves gets a new key;
    save_phrase_trees(overwrite=True) removes the old row. In memory only;
    see the module docstring for persisting.
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
        _set_syllable_phones(syllable, phones)
    word.replace_children(syllables)
    return syllables


# ------------------------------- derive ---------------------------------

def derive_syllables_for_word(word, language='dutch', store=None):
    '''Derive a new syllable layer for a word, on a copy of its phrase.
    word      donor Word; it and its store are left untouched
    language  syllabifier language; only 'dutch' is implemented
    store     target Store to save the derived phrase to; None keeps the
              copies in memory only

    The whole phrase is copied; only the word's syllables are new, built from
    the syllabifier's grouping of its phones. Returns the derived word.
    Raises ValueError, with the syllabifier's reason, when its phones cannot
    be syllabified (unknown phone / no nucleus).
    '''
    syllabifier = _syllabifier(language)
    _check_target_store(word.store, store)
    if word.phrase is None: raise ValueError('word must belong to a phrase')
    phrase = _copy_phrase_tree(word.phrase)
    derived = _find_by_id(phrase.words, word.identifier)
    groups = _group_phones(syllabifier, derived.phones, _describe(word))
    derived.replace_children(_build_syllables(groups))
    _finish([phrase], word, store)
    return derived


def derive_words_and_syllables_for_phrase(phrase, language='dutch',
    store=None):
    '''Derive new word and syllable layers for a phrase, on a copy of it.
    phrase    donor Phrase; it and its store are left untouched
    language  syllabifier language; only 'dutch' is implemented
    store     target Store to save the derived phrase to; None keeps the
              copies in memory only

    Connected speech: the phrase's phones are grouped as one sequence, so a
    consonant may move to a syllable of the next word (het ei -> hE.tEi).
    Each syllable is owned by the word of its nucleus, and words are sized to
    their syllables; a word that loses all its phones becomes zero-width.
    Returns the derived phrase. Raises ValueError, with the syllabifier's
    reason, when its phones cannot be syllabified (unknown phone / no
    nucleus).
    '''
    syllabifier = _syllabifier(language)
    _check_target_store(phrase.store, store)
    derived = _copy_phrase_tree(phrase)
    groups = _group_phones(syllabifier, derived.phones, _describe(phrase))
    phone_word = _phone_to_word(derived)
    syllables = _build_syllables(groups)
    syls_by_word = _bucket_by_nucleus(syllables, phone_word)
    old_words = derived.words
    word_ids = [word.identifier for word in old_words]
    words = _rebuild_words(old_words, word_ids, syls_by_word, derived.start)
    derived.replace_children(words)
    _finish([derived], phrase, store)
    return derived


def derive_phrases_for_phones(phones, max_pause=500, language='dutch',
    store=None):
    '''Derive new phrase, word and syllable layers for a phone sequence.
    phones     donor phones, or a segment exposing .phones; left untouched
    max_pause  a gap of at least this many ms starts a new phrase
    language   syllabifier language; only 'dutch' is implemented
    store      target Store to save the derived phrases to; None keeps the
               copies in memory only

    The phrase layer is re-derived from pauses: each run of phones between
    gaps >= max_pause becomes a phrase, so two donor phrases with a short gap
    merge and a phrase with a long internal pause splits. Within a run,
    connected-speech grouping applies as in
    derive_words_and_syllables_for_phrase. Returns the derived phrases ([]
    for no phones). Raises ValueError if any run cannot be syllabified (with
    the run's time range and the syllabifier's reason), or if the phones
    span more than one speaker or audio file.
    '''
    syllabifier = _syllabifier(language)
    if hasattr(phones, 'phones'): phones = phones.phones
    phones = sorted(phones, key=lambda p: p.start)
    if not phones: return []
    if len({p.speaker_id for p in phones}) > 1:
        raise ValueError('phones span multiple speakers')
    if len({p.audio_id for p in phones}) > 1:
        raise ValueError('phones span multiple audio files')
    _check_target_store(phones[0].store, store)
    copies, donor_phrases = _copy_phones_with_phrases(phones)
    runs = _split_runs(copies, max_pause)
    groups_per_run = []
    for run in runs:
        what = f'phones {run[0].start}-{run[-1].end} ms'
        groups_per_run.append(_group_phones(syllabifier, run, what))
    phrases = _build_phrases(runs, groups_per_run, donor_phrases)
    _finish(phrases, phones[0], store)
    return phrases


# ------------------------------- helpers --------------------------------

def _syllabifier(language):
    '''Return the syllabifier module for a language.'''
    if language == 'dutch': return dutch_syllabifier
    raise NotImplementedError(f'no syllabifier for language {language!r}')


def _group_phones(syllabifier, phones, what):
    '''The syllabifier's phone groups. Its ValueError is re-raised with
    `what` (e.g. "word 'st' (0-200 ms)") added as context.'''
    try: return syllabifier.resyllabify_phones(phones)
    except ValueError as error:
        raise ValueError(f'cannot syllabify {what}: {error}') from error


def _describe(segment):
    '''Short description of a segment for error messages.'''
    name = segment.object_type.lower()
    return f'{name} {segment.label!r} ({segment.start}-{segment.end} ms)'


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


def _set_syllable_phones(syllable, phones):
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
    syllable_structure.assign_syllable_positions_to_phones(phones)
    return syllable


def _check_target_store(donor_store, store):
    '''Refuse to save derived objects into the store they came from.'''
    if store is None: return
    same_path = Path(store.path).resolve() == Path(donor_store.path).resolve()
    if store is donor_store or same_path:
        message = 'derived objects cannot be saved to the donor store; '
        message += 'pass a different store'
        raise ValueError(message)


def _copy_segment(segment):
    '''Unbound copy of a segment: same id and stored fields, no links.'''
    cls = type(segment)
    duplicate = cls(label=segment.label, start=segment.start, end=segment.end,
        audio_id=segment.audio_id, speaker_id=segment.speaker_id)
    duplicate.identifier = segment.identifier
    duplicate.flags = getattr(segment, 'flags', 0)
    for field in cls.METADATA_FIELDS:
        setattr(duplicate, field, getattr(segment, field))
    return duplicate


def _copy_word_tree(word):
    '''Unbound copy of a donor word with its syllables and phones.'''
    duplicate = _copy_segment(word)
    syllables = []
    for syllable in word.children:
        syllable_copy = _copy_segment(syllable)
        phones = [_copy_segment(phone) for phone in syllable.children]
        syllable_copy.replace_children(phones)
        syllables.append(syllable_copy)
    duplicate.replace_children(syllables)
    return duplicate


def _copy_phrase_tree(phrase):
    '''Unbound copy of a donor phrase and all its descendants.'''
    duplicate = _copy_segment(phrase)
    duplicate.replace_children([_copy_word_tree(w) for w in phrase.children])
    return duplicate


def _copy_phones_with_phrases(phones):
    '''Copy the phrase trees holding `phones`. Returns the copies of `phones`
    (same order) and the copied phrases by id.'''
    donor_phrases = {}
    for phone in phones:
        donor_phrases.setdefault(phone.phrase_id, phone.phrase)
    copied = {}
    for phrase_id, phrase in donor_phrases.items():
        copied[phrase_id] = _copy_phrase_tree(phrase)
    phone_copies = {}
    for phrase in copied.values():
        for phone in phrase.phones: phone_copies[phone.identifier] = phone
    return [phone_copies[p.identifier] for p in phones], copied


def _find_by_id(segments, identifier):
    '''The segment with this identifier.'''
    return next(s for s in segments if s.identifier == identifier)


def _derived_id(kind, phones):
    '''Deterministic id for a derived object without a one-to-one donor
    counterpart, hashed from its kind and its phones' ids.'''
    digest = hashlib.blake2b(kind.encode(), digest_size=8)
    for phone in phones: digest.update(phone.identifier)
    return digest.digest()


def _carry_metadata(segment, source):
    '''Copy metadata from source; overlap_code is left unset because the
    span may have changed.'''
    for field in type(segment).METADATA_FIELDS:
        if field == 'overlap_code': continue
        setattr(segment, field, getattr(source, field))


def _syllable_sources(groups):
    '''Per phone group, the old syllable holding its nucleus when that match
    is one-to-one, else None. Read before any phone is re-parented.'''
    owners = []
    for group in groups:
        indices = syllable_structure.phones_to_vowel_indices(group)
        owners.append({group[i].parent_id: group[i].parent for i in indices})
    counts = Counter(next(iter(o)) for o in owners if len(o) == 1)
    sources = []
    for owner in owners:
        one_to_one = len(owner) == 1 and counts[next(iter(owner))] == 1
        sources.append(next(iter(owner.values())) if one_to_one else None)
    return sources


def _build_syllables(groups):
    '''New syllables around the phone groups (see _syllable_sources).'''
    sources = _syllable_sources(groups)
    syllables = []
    for group, source in zip(groups, sources):
        syllables.append(_build_syllable(group, source))
    return syllables


def _build_syllable(group, source):
    '''New Syllable around a phone group, positions assigned. With a source
    (its one-to-one old syllable) it takes that id and metadata; otherwise
    its id is hashed from the group. Not yet wired to a word.'''
    syllable = models.Syllable(label=' '.join(p.label for p in group),
        start=min(p.start for p in group), end=max(p.end for p in group),
        audio_id=group[0].audio_id, speaker_id=group[0].speaker_id)
    if source is None: syllable.identifier = _derived_id('Syllable', group)
    else:
        syllable.identifier = source.identifier
        _carry_metadata(syllable, source)
    # replace_children, not add_children: it starts from empty caches
    syllable.replace_children(group)
    syllable_structure.assign_syllable_positions_to_phones(group)
    return syllable


def _rebuild_words(old_words, word_ids, syls_by_word, start):
    '''New words replacing old_words in order, each sized to its syllables;
    a word without syllables becomes zero-width where the previous word
    ends (the point its phones vacated).'''
    words, cursor = [], start
    for old_word, word_id in zip(old_words, word_ids):
        syllables = syls_by_word.get(old_word.identifier, [])
        word = _rebuild_word(old_word, word_id, syllables, cursor)
        cursor = word.end
        words.append(word)
    return words


def _rebuild_word(old_word, word_id, syllables, cursor):
    '''New Word replacing old_word under word_id, sized to its syllables and
    carrying old_word's metadata. Not yet wired to a phrase.'''
    if syllables:
        start = min(s.start for s in syllables)
        end = max(s.end for s in syllables)
    else: start, end = cursor, cursor
    word = models.Word(label=old_word.label, start=start, end=end,
        audio_id=old_word.audio_id, speaker_id=old_word.speaker_id)
    word.identifier = word_id
    _carry_metadata(word, old_word)
    word.replace_children(syllables)
    return word


def _build_phrases(runs, groups_per_run, donor_phrases):
    '''One new phrase per run, its words and syllables rebuilt from the run's
    phone groups.'''
    phone_word, phrase_phone_ids = {}, {}
    for phrase_id, phrase in donor_phrases.items():
        phone_word.update(_phone_to_word(phrase))
        phrase_phone_ids[phrase_id] = {p.identifier for p in phrase.phones}
    all_groups = [group for groups in groups_per_run for group in groups]
    sources = iter(_syllable_sources(all_groups))
    words_per_run = [_words_in_order(run, phone_word) for run in runs]
    split_words = _words_in_several_runs(words_per_run)
    phrases = []
    for run, groups, old_words in zip(runs, groups_per_run, words_per_run):
        syllables = []
        for group in groups:
            syllables.append(_build_syllable(group, next(sources)))
        syls_by_word = _bucket_by_nucleus(syllables, phone_word)
        phrase = _build_phrase(run, donor_phrases, phrase_phone_ids)
        word_ids = _run_word_ids(run, old_words, split_words, phone_word)
        words = _rebuild_words(old_words, word_ids, syls_by_word, phrase.start)
        phrase.replace_children(words)
        phrase.label = ' '.join(w.label for w in words)
        phrases.append(phrase)
    return phrases


def _build_phrase(run, donor_phrases, phrase_phone_ids):
    '''New Phrase sized to the run. It keeps the donor phrase id when the run
    holds exactly that phrase's phones (phrase_phone_ids, read before any
    rewiring), else its id is hashed from the run; metadata (filename) comes
    from the first phone's donor phrase.'''
    phrase = models.Phrase(label='', start=run[0].start, end=run[-1].end,
        audio_id=run[0].audio_id, speaker_id=run[0].speaker_id)
    donor = donor_phrases[run[0].phrase_id]
    run_ids = {p.identifier for p in run}
    same_phones = run_ids == phrase_phone_ids[donor.identifier]
    if same_phones: phrase.identifier = donor.identifier
    else: phrase.identifier = _derived_id('Phrase', run)
    _carry_metadata(phrase, donor)
    return phrase


def _words_in_several_runs(words_per_run):
    '''Ids of old words whose phones fall in more than one run.'''
    counts = Counter(w.identifier for words in words_per_run for w in words)
    return {identifier for identifier, n in counts.items() if n > 1}


def _run_word_ids(run, old_words, split_words, phone_word):
    '''Ids for a run's rebuilt words: the old word's id, or for a word split
    across runs an id hashed from its phones in this run.'''
    word_ids = []
    for word in old_words:
        if word.identifier not in split_words:
            word_ids.append(word.identifier)
            continue
        own = [p for p in run if phone_word[p.identifier] is word]
        word_ids.append(_derived_id('Word', own))
    return word_ids


def _finish(phrases, donor, store):
    '''Cache the donor's Audio and Speaker on the derived segments; with a
    store, also save the phrases there.'''
    audio, speaker = donor.audio, donor.speaker
    if store is not None:
        audio = _copy_record(audio, store)
        speaker = _copy_record(speaker, store)
        if audio is not None and speaker is not None:
            store.DB.write_speaker_audio_link(speaker, audio)
    for phrase in phrases:
        _attach(phrase, audio, speaker, store)
    if store is not None: store.save_phrase_trees(phrases, overwrite=True)


def _attach(phrase, audio, speaker, store):
    '''Cache audio and speaker on every segment of the phrase tree, and bind
    them to store when one is given.'''
    for segment in phrase.items:
        segment._audio, segment._speaker = audio, speaker
        if store is not None: store.attach(segment)


def _copy_record(record, store):
    '''Same-id copy of an Audio or Speaker record bound to store, saved when
    the store does not hold it yet. None stays None.'''
    if record is None: return None
    cls = type(record)
    fields = {}
    for field in cls.IDENTITY_FIELDS | cls.METADATA_FIELDS:
        if hasattr(record, field): fields[field] = getattr(record, field)
    duplicate = cls(store=store, **fields)
    duplicate.identifier = record.identifier
    duplicate.flags = getattr(record, 'flags', 0)
    if not store.DB.key_exists(duplicate.key): duplicate.save()
    return duplicate


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
