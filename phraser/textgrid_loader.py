from progressbar import progressbar
from textgrid import TextGrid

from phraser import audio
from phraser import force_align
from phraser import locations
from phraser import models
from phraser import syllable_structure
from phraser import utils
from phraser.model_helper import EMPTY_ID


TEXTGRID_EXISTING_POLICIES = {'append', 'add_missing', 'replace', 'upsert'}

def require_store(store=None, items=None):
    if store is not None: return store
    if items: return items[0].store
    raise ValueError('store is required')

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

def validate_sequence_lengths_match(left_name, left, right_name, right):
    if len(left) == len(right): return
    m = f'{left_name} and {right_name} must have matching lengths; '
    m += f'got {len(left)} and {len(right)}.'
    raise ValueError(m)

def load_textgrid(filename):
    """Load a TextGrid file and return the TextGrid object."""
    tg = TextGrid.fromFile(filename)
    return tg

def save_textgrid_items(items, store=None, existing='append'):
    '''Persist staged TextGrid items according to an existence policy.

    `items` should be the staged objects returned by
    `textgrid_filename_to_database_objects(...)`.

    Supported `existing` values:
    - `append`: no existence check; save staged items directly. Use when the
      caller knows the phrase does not already exist.
    - `add_missing`: run an existence check; save when no matching phrase
      exists, skip when one matching phrase exists.
    - `replace`: run an existence check; require exactly one matching phrase,
      delete that phrase tree, then save the staged items.
    - `upsert`: run an existence check; replace when one matching phrase exists,
      otherwise save the staged items as new.

    Existence checks are scoped to phrases linked to the same audio, then matched
    by Phrase equality: `(audio_id, speaker_id, start)`. Multiple matches raise
    ValueError. The function returns `added`, `skipped`, or `replaced`.
    '''
    validate_textgrid_existing_mode(existing)
    items = list(items)
    store = require_store(store, items)
    if existing == 'append':
        store.save_many(items)
        return 'added'
    phrase = require_single_textgrid_phrase(items)
    matches = find_matching_textgrid_phrases(phrase, store=store)
    validate_textgrid_match_count(matches, phrase)
    if existing == 'add_missing':
        if matches: return 'skipped'
        store.save_many(items)
        return 'added'
    if existing == 'replace':
        if not matches:
            raise ValueError('replace requires one matching existing phrase')
        replace_textgrid_phrase_tree(matches[0], items, store=store)
        return 'replaced'
    if matches:
        replace_textgrid_phrase_tree(matches[0], items, store=store)
        return 'replaced'
    store.save_many(items)
    return 'added'

def validate_textgrid_existing_mode(existing):
    if existing in TEXTGRID_EXISTING_POLICIES: return
    values = ', '.join(sorted(TEXTGRID_EXISTING_POLICIES))
    raise ValueError(f'existing must be one of: {values}')

def validate_textgrid_audio(audio, existing, store=None, save_to_db=True):
    validate_textgrid_existing_mode(existing)
    if not save_to_db: return
    if existing == 'append':
        if audio is None: return
        if store is None: return
        if getattr(audio, '_store', None) is store: return
        raise ValueError('audio must be bound to the same Store')
    m = 'audio is required when existing requires an existence check; '
    if audio is None:
        m += f'got existing={existing!r}'
        raise ValueError(m)
    if store is None:
        raise ValueError('store is required to validate textgrid audio')
    if getattr(audio, '_store', None) is not store:
        raise ValueError('audio must be bound to the same Store')
    if store.DB.key_exists(audio.key): return
    raise ValueError('audio must already exist in the Store')

def validate_textgrid_audios(audios, existing, store=None, save_to_db=True):
    if audios is None:
        validate_textgrid_audio(None, existing, store=store,
            save_to_db=save_to_db)
        return
    for i, audio in enumerate(audios):
        try:
            validate_textgrid_audio(audio, existing, store=store,
                save_to_db=save_to_db)
        except ValueError as e:
            raise ValueError(f'audios[{i}]: {e}') from e

def require_single_textgrid_phrase(items):
    phrases = get_phrases_from_items(items)
    if len(phrases) == 1: return phrases[0]
    raise ValueError(f'TextGrid items must contain exactly one Phrase; '
        f'got {len(phrases)}')

def find_matching_textgrid_phrases(phrase, store=None):
    store = require_store(store, [phrase])
    if phrase.audio_id == EMPTY_ID:
        raise ValueError('TextGrid existence checks require phrase.audio_id')
    keys = list(store.DB.audio_id_to_child_keys(phrase.audio_id, 'Phrase'))
    phrases = store.load_many(keys)
    return [existing for existing in phrases if existing == phrase]

def validate_textgrid_match_count(matches, phrase):
    if len(matches) <= 1: return
    audio_id = phrase.audio_id.hex()
    speaker_id = phrase.speaker_id.hex()
    m = 'multiple matching phrases found for TextGrid identity '
    m += f'audio_id={audio_id}, speaker_id={speaker_id}, start={phrase.start}'
    raise ValueError(m)

def replace_textgrid_phrase_tree(existing_phrase, items, store=None):
    store = require_store(store, items)
    old_items = list(existing_phrase.all_objects)
    delete_textgrid_items(old_items, store=store)
    store.save_many(items)

def delete_textgrid_items(items, store=None):
    store = require_store(store, items)
    label_index_keys = items_to_label_index_keys(items)
    if label_index_keys:
        store.DB.delete_many_label_index_links(label_index_keys)
    keys = [item.key for item in items]
    if keys: store.delete_many(keys)

def items_to_label_index_keys(items):
    label_index_keys = []
    for item in items:
        try: label_index_keys.append(item.label_index_key)
        except AttributeError: pass
    return label_index_keys

def textgrid_filename_to_database_objects(textgrid_filename, offset = 0, 
    audio = None, speaker = None, overwrite = False, multiple_speakers = None,
    store=None):
    '''Build store-bound objects from a TextGrid.

    This is staging mode: objects are bound to `store`, individual
    constructor/link writes are suppressed, and `save_textgrid_items()` persists
    them later.
    '''
    validate_textgrid_overwrite(overwrite)
    if store is None:
        if audio is not None: store = getattr(audio, '_store', None)
        elif speaker is not None: store = getattr(speaker, '_store', None)
    store = require_textgrid_store(store)
    no_overlap_code = utils.overlap_dict[False]
    tg = load_textgrid(textgrid_filename)
    # segments are born with their identity; a later pass no longer
    # attaches audio/speaker (step toward mandatory constructor params)
    identity = {}
    if audio is not None: identity['audio_id'] = audio.identifier
    if speaker is not None: identity['speaker_id'] = speaker.identifier
    words = list(textgrid_to_words(tg, offset, store=store,
        kwargs=identity))
    syllables = list(textgrid_to_syllables(tg, offset, store=store,
        kwargs=identity))
    phones = list(textgrid_to_phones(tg, offset, store=store,
        kwargs=identity))
    phrase = words_to_phrase(words, textgrid_filename = textgrid_filename,
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
    if not words:
        return None
    kwargs = copy_kwargs(kwargs)
    start = words[0].start
    end = words[-1].end
    label = ' '.join([word.label for word in words])
    phrase = models.Phrase(start=start, end=end, label=label,
        filename =textgrid_filename, save = False, store=store, **kwargs)
    for word in words:
        word.add_parent(phrase)
    return phrase

def textgrid_to_words(tg, offset = 0, *, store=None, kwargs=None):
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

def textgrid_to_syllables(tg, offset = 0, *, store=None, kwargs=None):
    store = require_textgrid_store(store)
    names = tg.getNames()
    syllables = get_textgrid_tier(tg, names, 'MAS', 'syllables')

    for syl in syllables:
        if syl.mark == '<p:>': continue
        yield interval_to_syllable(syl, offset=offset, kwargs=kwargs,
            store=store)

def textgrid_to_phones(tg, offset = 0, *, store=None, kwargs=None):

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

def interval_to_word(ort_interval, ipa_interval = None, offset = 0, 
    kwargs = None, store=None):
    kwargs = copy_kwargs(kwargs)
    if ipa_interval: 
        kwargs['ipa'] = ipa_interval.mark
    word = interval_to_database_object(ort_interval, models.Word, 
        offset, kwargs, store=store)
    return word
        
def interval_to_syllable(syl_interval, offset = 0, kwargs = None, store=None):
    syllable = interval_to_database_object(syl_interval, models.Syllable, 
    offset, kwargs, store=store)
    return syllable

def interval_to_phone(phone_interval, offset = 0, kwargs = None, store=None):
    phone = interval_to_database_object(phone_interval, models.Phone, 
    offset, kwargs, store=store)
    return phone

def interval_to_database_object(interval, model_class, offset = 0, kwargs=None,
    store=None):
    kwargs = copy_kwargs(kwargs)
    store = require_textgrid_store(store)
    start = utils.seconds_to_miliseconds(interval.minTime + offset)
    end = utils.seconds_to_miliseconds(interval.maxTime + offset)
    o = model_class(start=start, end=end, label=interval.mark, save=False,
        store=store, **kwargs)
    return o

def select_objecs_in_range(objects, start, end):
    selected = []
    for obj in objects:
        if obj.start >= start and obj.end <= end:
            selected.append(obj)
    return selected

def find_and_add_syllables_to_word(word, syllables, save_to_db=False):
    syllables = select_objecs_in_range(syllables, word.start, word.end)
    for syl in syllables:
        syl.add_parent(word)

def find_and_add_phones_to_syllable(syllable, phones, save_to_db=False,
    assign_positions=True):
    phones = select_objecs_in_range(phones, syllable.start, syllable.end)
    for phone in phones:
        phone.add_parent(syllable)
    if assign_positions:
        try:
            # in-memory; position_code is persisted by the later db save
            syllable_structure.assign_syllable_positions_to_phones(phones)
        except ValueError:
            pass  # odd transcription -> leave phones at default ('unknown')


def load_single_audio_and_transcription_to_db(audio_filename, text = None, 
    speaker = None,textgrid_filename = None, do_force_align = False, 
    save_to_db = True, textgrid_output_dir = None, store=None,
    existing='append', audio=None):
    '''Load a transcription, optionally force-aligning first.

    Returns the same object list as `load_single_audio_textgrid_to_db()`.
    '''
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    validate_textgrid_audio(audio, existing, store=store,
        save_to_db=save_to_db)
    if textgrid_filename is None and not do_force_align:
        m = f'Either a TextGrid filename must be provided '
        m += f'or force alignment must be requested.'
        raise ValueError(m)
    if textgrid_output_dir is None:
        textgrid_output_dir = locations.textgrids
    if do_force_align:
        o = force_align.force_align_single(text, audio_filename, 
            output_dir = textgrid_output_dir) 
        textgrid_filename = o['output_file']
    db_objects = load_single_audio_textgrid_to_db(audio_filename, 
        textgrid_filename, speaker = speaker, save_to_db = save_to_db,
        store=store, existing=existing, audio=audio)
    return db_objects

def load_single_audio_textgrid_to_db(audio_filename, textgrid_filename,
    speaker = None, save_to_db = True, store=None, existing='append',
    audio=None):
    '''Load one audio/TextGrid pair.

    When `save_to_db=False`, returns staged objects: audio plus TextGrid items.
    When `save_to_db=True`, returns only objects written by this call. A
    pre-existing `audio` is not returned because it was not written. If
    `existing='add_missing'` skips an existing phrase, returns an empty list.
    '''
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    validate_textgrid_audio(audio, existing, store=store,
        save_to_db=save_to_db)
    if audio is None:
        audio = audio_filename_to_db_object(
            audio_filename, save_to_db=False, store=store)
    items = textgrid_filename_to_database_objects(textgrid_filename,
        audio=audio, speaker=speaker, store=store)
    db_objects = [audio] + items
    if save_to_db:
        stored_objects = []
        if existing == 'append' and not store.DB.key_exists(audio.key):
            store.save(audio)
            stored_objects.append(audio)
        action = save_textgrid_items(items, store=store, existing=existing)
        if action != 'skipped': stored_objects.extend(items)
        return stored_objects
    return db_objects

def audio_filename_to_db_object(audio_filename, save_to_db = False, kwargs=None,
    store=None):
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    kwargs = copy_kwargs(kwargs)
    audio_info = audio.audio_info(audio_filename)
    if audio_info.keys() & kwargs.keys():
        m = 'WARNING: Conflicting keys in audio info and kwargs: '
        m += f'{audio_info.keys() & kwargs.keys()}'
        print(m)
    audio_info.update(kwargs)
    audio_object = models.Audio(**audio_info, save = save_to_db, store=store)
    return audio_object

def load_audios_textgrids_to_db(audio_filenames, textgrid_filenames, speakers, 
    save_to_db = True, store=None, existing='append', audios=None):
    '''Load audio/TextGrid pairs.

    Returns the concatenated object lists from `load_single_audio_textgrid_to_db()`.
    '''
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    if audios is not None:
        validate_sequence_lengths_match('textgrid_filenames',
            textgrid_filenames, 'audios', audios)
    validate_textgrid_audios(audios, existing, store=store,
        save_to_db=save_to_db)
    validate_sequence_lengths_match('audio_filenames', audio_filenames,
        'textgrid_filenames', textgrid_filenames)
    if speakers is not None:
        validate_sequence_lengths_match('textgrid_filenames',
            textgrid_filenames, 'speakers', speakers)
    db_objects = []
    for i in progressbar(range(len(audio_filenames))):
        audio_filename = audio_filenames[i]
        textgrid_filename = textgrid_filenames[i]
        if speakers is not None:
            speaker = speakers[i]
        else: speaker = None
        if audios is not None:
            audio_object = audios[i]
        else: audio_object = None
        objs = load_single_audio_textgrid_to_db(
            audio_filename, textgrid_filename, speaker = speaker,
            save_to_db = save_to_db, store=store, existing=existing,
            audio=audio_object)
        db_objects.extend(objs)
    return db_objects
    
def load_speaker_audios_textgrids_to_db(speaker, audio_filenames, 
    textgrid_filenames, save_to_db = True, store=None, existing='append',
    audios=None):
    '''Load audio/TextGrid pairs for one speaker.

    Returns the same object list as `load_audios_textgrids_to_db()`.
    '''
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    validate_textgrid_audios(audios, existing, store=store,
        save_to_db=save_to_db)
    validate_sequence_lengths_match('audio_filenames', audio_filenames,
        'textgrid_filenames', textgrid_filenames)
    speakers = [speaker] * len(audio_filenames)
    db_objects = load_audios_textgrids_to_db(
        audio_filenames, textgrid_filenames, speakers,
        save_to_db = save_to_db, store=store, existing=existing, audios=audios)
    return db_objects
    
def get_phrases_from_items(items):
    phrases = [item for item in items if isinstance(item, models.Phrase)]
    return phrases

def check_items_excists_in_db(db_items, refresh_db = True, store=None):
    store = require_store(store, db_items)
    if refresh_db: store.refresh_query_roots()
    existing_items = []
    new_items = []
    for item in db_items:
        if item.exists_in_db: 
            existing_items.append(item)
        else:
            new_items.append(item)
    return existing_items, new_items

        
