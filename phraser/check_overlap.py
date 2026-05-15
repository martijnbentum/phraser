from . import utils

NO_OVERLAP = utils.overlap_dict[False]
OVERLAP = utils.overlap_dict[True]


def check_overlap(audio):
    """Set overlap_code on all phrases/words/syllables/phones for the given audio.

    Collects all items upfront, sets overlap_code in memory, then writes
    everything to the database in a single save_many call.
    """
    all_items = []
    for phrase in audio.phrases:
        all_items.extend(phrase.all_objects)

    if not all_items: return

    if len(audio.speakers) <= 1:
        for item in all_items:
            item.overlap_code = NO_OVERLAP
    else: _set_overlap_codes(audio.phrases)

    cache = all_items[0].__class__.get_default_cache()
    cache.save_many(all_items, overwrite=True)


def _set_overlap_codes(all_phrases):
    """Set overlap codes for all phrases and their descendants."""
    for phrase in all_phrases:
        _set_phrase_overlap_code(phrase, all_phrases)


def _set_phrase_overlap_code(phrase, all_phrases):
    """Set overlap_code on phrase and descend into its words.

    Finds other-speaker phrases that overlap in time. Passes that list down
    to each word so word-level checking is scoped to relevant phrases only.
    """
    others = [p for p in all_phrases
              if p is not phrase
              and p.speaker_id != phrase.speaker_id
              and utils.overlap(phrase, p)]
    phrase.overlap_code = OVERLAP if others else NO_OVERLAP
    for word in phrase.words:
        _set_word_overlap_code(word, others)


def _set_word_overlap_code(word, other_phrases):
    """Set overlap_code on word and descend into its syllables.

    Checks whether the word overlaps with any of the other-speaker phrases.
    Passes the same other_phrases list down so syllables are checked against
    the same phrase boundaries.
    """
    word.overlap_code = OVERLAP if any(
        utils.overlap(word, p) for p in other_phrases
    ) else NO_OVERLAP
    for syllable in word.syllables:
        _set_syllable_overlap_code(syllable, other_phrases)


def _set_syllable_overlap_code(syllable, other_phrases):
    """Set overlap_code on syllable and descend into its phones.

    Checks whether the syllable overlaps with any of the other-speaker phrases.
    Passes the same other_phrases list down so phones are checked against
    the same phrase boundaries.
    """
    syllable.overlap_code = OVERLAP if any(
        utils.overlap(syllable, p) for p in other_phrases
    ) else NO_OVERLAP
    for phone in syllable.phones:
        _set_phone_overlap_code(phone, other_phrases)


def _set_phone_overlap_code(phone, other_phrases):
    """Set overlap_code on phone based on overlap with other-speaker phrases."""
    phone.overlap_code = OVERLAP if any(
        utils.overlap(phone, p) for p in other_phrases
    ) else NO_OVERLAP
