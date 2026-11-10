'''Turn a TextGrid into linked segment objects in memory.

Builds a Phrase with its Words, Syllables and Phones from the WebMAUS tiers
(ORT-MAU, KAN-MAU, MAS, MAU), bound to a store but not saved.
'''

from textgrid import TextGrid

from phraser import models
from phraser import utils
from phraser.phonology import syllable_structure
from phraser.storage.key_helper import EMPTY_ID


def require_textgrid_store(store=None):
    '''TextGrid conversion builds store-bound staging objects.

    Staging suppresses individual writes so callers can batch-save later; it
    does not mean unbound object construction.
    '''
    if store is not None: return store
    m = 'store is required for TextGrid conversion; staging builds '
    m += 'store-bound objects without individual writes, then callers can '
    m += 'batch-save them later'
    raise ValueError(m)

def get_textgrid_tier(tg, names, tier_name, object_type):
    if tier_name in names:
        return tg.tiers[names.index(tier_name)]
    m = f"TextGrid must contain '{tier_name}' tier for {object_type}."
    raise ValueError(m)

def validate_tier_lengths_match(left, right, left_name, right_name):
    if len(left) == len(right): return
    m = f"TextGrid tiers '{left_name}' and '{right_name}' must have "
    m += f"matching interval counts; got {len(left)} and {len(right)}."
    raise ValueError(m)

def validate_interval_times_match(left, right, left_name, right_name, index):
    if left.minTime == right.minTime and left.maxTime == right.maxTime:
        return
    m = f"TextGrid tiers '{left_name}' and '{right_name}' interval {index} "
    m += 'must have matching start/end times.'
    raise ValueError(m)

def validate_textgrid_overwrite(overwrite):
    if not overwrite: return
    m = 'overwrite=True is not supported for TextGrid imports; imports '
    m += 'create fresh object identifiers and cannot overwrite previous '
    m += 'imports by key'
    raise ValueError(m)

def load_textgrid(filename):
    '''Load a TextGrid file and return the TextGrid object.'''
    return TextGrid.fromFile(filename)

def textgrid_filename_to_database_objects(textgrid_filename, offset=0,
    audio=None, speaker=None, overwrite=False, multiple_speakers=None,
    store=None):
    '''Build store-bound objects from a TextGrid.

    This is staging mode: objects are bound to `store`, individual
    constructor/link writes are suppressed, and `save_textgrid_items()` persists
    them later.
    '''
    validate_textgrid_overwrite(overwrite)
    if audio is None or speaker is None:
        m = 'audio and speaker are required; segments carry their '
        m += 'identity from construction.'
        raise ValueError(m)
    if store is None:
        if audio is not None: store = getattr(audio, '_store', None)
        elif speaker is not None: store = getattr(speaker, '_store', None)
    store = require_textgrid_store(store)
    no_overlap_code = utils.overlap_dict[False]
    tg = load_textgrid(textgrid_filename)
    identity = {'audio_id': audio.identifier, 'speaker_id': speaker.identifier}
    words = list(textgrid_to_words(tg, offset, store=store,
        kwargs=identity))
    syllables = list(textgrid_to_syllables(tg, offset, store=store,
        kwargs=identity))
    phones = list(textgrid_to_phones(tg, offset, store=store,
        kwargs=identity))
    phrase = words_to_phrase(words, textgrid_filename=textgrid_filename,
        store=store, kwargs=identity)
    if phrase is None:
        m = 'TextGrid must contain at least one non-empty word interval; '
        m += f'got {textgrid_filename!r}'
        raise ValueError(m)
    # link top-down: words got the phrase in words_to_phrase, so
    # syllables inherit phrase refs from their word and phones from
    # their syllable at add_parent time
    for word in words:
        find_and_add_syllables_to_word(word, syllables, save_to_db=False)
    for syllable in syllables:
        find_and_add_phones_to_syllable(syllable, phones, save_to_db=False)
    for item in syllables + phones:
        # orphans (e.g. pause phones outside every syllable) still belong
        # to the phrase timeline; give them the refs linking would inherit
        if item.parent_id != EMPTY_ID: continue
        item._set_phrase_refs(phrase.identifier, phrase.start)
    items = words + syllables + phones + [phrase]
    if multiple_speakers is False:
        for item in items:
            item.overlap_code = no_overlap_code
    return items

def words_to_phrase(words, textgrid_filename, store=None, kwargs=None):
    words = list(words)
    if not words: return None
    kwargs = copy_kwargs(kwargs)
    start = words[0].start
    end = words[-1].end
    label = ' '.join([word.label for word in words])
    phrase = models.Phrase(start=start, end=end, label=label,
        filename=textgrid_filename, save=False, store=store, **kwargs)
    for word in words:
        word.add_parent(phrase)
    return phrase

def textgrid_to_words(tg, offset=0, *, store=None, kwargs=None):
    store = require_textgrid_store(store)
    names = tg.getNames()
    ort_mau = get_textgrid_tier(tg, names, 'ORT-MAU', 'words')
    kan_mau = get_textgrid_tier(tg, names, 'KAN-MAU', 'words')
    validate_tier_lengths_match(ort_mau, kan_mau, 'ORT-MAU', 'KAN-MAU')

    for index, (ort, ipa) in enumerate(zip(ort_mau, kan_mau)):
        if ort.mark == '': continue
        validate_interval_times_match(ort, ipa, 'ORT-MAU', 'KAN-MAU', index)
        yield interval_to_word(ort, ipa, offset=offset, kwargs=kwargs,
            store=store)

def textgrid_to_syllables(tg, offset=0, *, store=None, kwargs=None):
    store = require_textgrid_store(store)
    names = tg.getNames()
    syllables = get_textgrid_tier(tg, names, 'MAS', 'syllables')

    for syl in syllables:
        if syl.mark == '<p:>': continue
        yield interval_to_syllable(syl, offset=offset, kwargs=kwargs,
            store=store)

def textgrid_to_phones(tg, offset=0, *, store=None, kwargs=None):
    store = require_textgrid_store(store)
    names = tg.getNames()
    phones = get_textgrid_tier(tg, names, 'MAU', 'phones')

    for phone in phones:
        if phone.mark == '(...)': continue
        yield interval_to_phone(phone, offset=offset, kwargs=kwargs,
            store=store)

def copy_kwargs(kwargs=None):
    if kwargs is None: return {}
    return dict(kwargs)

def interval_to_word(ort_interval, ipa_interval=None, offset=0,
    kwargs=None, store=None):
    kwargs = copy_kwargs(kwargs)
    if ipa_interval: kwargs['ipa'] = ipa_interval.mark
    return interval_to_database_object(ort_interval, models.Word, offset,
        kwargs, store=store)

def interval_to_syllable(syl_interval, offset=0, kwargs=None, store=None):
    return interval_to_database_object(syl_interval, models.Syllable,
        offset, kwargs, store=store)

def interval_to_phone(phone_interval, offset=0, kwargs=None, store=None):
    return interval_to_database_object(phone_interval, models.Phone,
        offset, kwargs, store=store)

def interval_to_database_object(interval, model_class, offset=0, kwargs=None,
    store=None):
    kwargs = copy_kwargs(kwargs)
    store = require_textgrid_store(store)
    start = utils.seconds_to_miliseconds(interval.minTime + offset)
    end = utils.seconds_to_miliseconds(interval.maxTime + offset)
    o = model_class(start=start, end=end, label=interval.mark, save=False,
        store=store, **kwargs)
    return o

def select_objects_in_range(objects, start, end):
    selected = []
    for obj in objects:
        if obj.start >= start and obj.end <= end:
            selected.append(obj)
    return selected

def find_and_add_syllables_to_word(word, syllables, save_to_db=False):
    syllables = select_objects_in_range(syllables, word.start, word.end)
    for syl in syllables:
        syl.add_parent(word)

def find_and_add_phones_to_syllable(syllable, phones, save_to_db=False,
    assign_positions=True):
    phones = select_objects_in_range(phones, syllable.start, syllable.end)
    for phone in phones:
        phone.add_parent(syllable)
    if assign_positions:
        try:
            # in-memory; position_code is persisted by the later db save
            syllable_structure.assign_syllable_positions_to_phones(phones)
        except ValueError:
            pass  # odd transcription -> leave phones at default ('unknown')
