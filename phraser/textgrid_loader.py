from progressbar import progressbar
from textgrid import TextGrid

from phraser import audio
from phraser import force_align
from phraser import locations
from phraser import models
from phraser import syllable_structure
from phraser import utils

db_save_state = None

def require_store(store=None, items=None):
    if store is not None: return store
    if items: return items[0].store
    raise ValueError('store is required')

def require_textgrid_store(store=None):
    '''TextGrid conversion builds store-bound staging objects.

    save_to_db=False only suppresses individual writes so callers can batch-save
    later; it does not mean unbound object construction.
    '''
    if store is not None: return store
    m = 'store is required for TextGrid conversion; save_to_db=False stages '
    m += 'store-bound objects without individual writes, then callers can '
    m += 'batch-save them later'
    raise ValueError(m)

def load_textgrid(filename):
    """Load a TextGrid file and return the TextGrid object."""
    tg = TextGrid.fromFile(filename)
    return tg

def save_items_to_db(items, store=None):
    store = require_store(store, items)
    update_db_save_state(store)
    handle_db_save_option(store, save_to_db=True)
    store.save_many(items)
    handle_db_save_option(store, revert=True)

def textgrid_filename_to_database_objects(textgrid_filename, offset = 0, 
    audio = None, speaker = None, save_to_db=False, overwrite = False, 
    multiple_speakers = None, store=None):
    '''Build store-bound objects from a TextGrid.

    save_to_db=False is staging mode: objects are bound to `store`, individual
    constructor/link writes are suppressed, and a later `save_items_to_db()` can
    persist them in one batch.
    '''
    if store is None:
        if audio is not None: store = getattr(audio, '_store', None)
        elif speaker is not None: store = getattr(speaker, '_store', None)
    store = require_textgrid_store(store)
    no_overlap_code = utils.overlap_dict[False]
    tg = load_textgrid(textgrid_filename)
    words = list(textgrid_to_words(tg, offset, store=store))
    syllables = list(textgrid_to_syllables(tg, offset, store=store))
    phones = list(textgrid_to_phones(tg, offset, store=store))
    phrase = words_to_phrase(words, textgrid_filename = textgrid_filename,
        store=store)  
    for phone in phones:
        phone._add_phrase(phrase, update_database = False)
    for syllable in syllables:
        find_and_add_phones_to_syllable(syllable, phones, save_to_db=False)
        syllable._add_phrase(phrase, update_database = False)
    for word in words:
        find_and_add_syllables_to_word(word, syllables, save_to_db=False)
    items = words + syllables + phones + [phrase]
    for item in items:
        item.add_audio(audio, update_database = False, propagate = False)
        item.add_speaker(speaker, update_database = False, propagate = False)
        if multiple_speakers is False: item.overlap_code = no_overlap_code
    if save_to_db: save_items_to_db(items, store=store)
    return items
         
def words_to_phrase(words, textgrid_filename, store=None):
    words = list(words)
    if not words:
        return None
    start = words[0].start
    end = words[-1].end
    label = ' '.join([word.label for word in words])
    phrase = models.Phrase(start=start, end=end, label=label, 
        filename =textgrid_filename, save = False, store=store)
    for word in words:
        word.add_parent(phrase, update_database = False)
    return phrase 

def textgrid_to_words(tg, offset = 0, save_to_db=False, store=None):
    store = require_textgrid_store(store)
    update_db_save_state(store)
    handle_db_save_option(store, save_to_db=save_to_db)
    names = tg.getNames()
    assert 'ORT-MAU' in names, "TextGrid must contain 'ORT-MAU' tier for words."
    assert 'KAN-MAU' in names, "TextGrid must contain 'KAN-MAU' tier for words."
    ort_mau = tg.tiers[names.index('ORT-MAU')]
    kan_mau = tg.tiers[names.index('KAN-MAU')]
    
    for index, (ort, ipa) in enumerate(zip(ort_mau, kan_mau)):
        if ort.mark == '': continue
        assert ort.minTime == ipa.minTime and ort.maxTime == ipa.maxTime, \
            "ORT-MAU and KAN-MAU tiers must have matching intervals."
        yield interval_to_word(ort, ipa, offset=offset, store=store)  
            
    handle_db_save_option(store, revert=True)

def textgrid_to_syllables(tg, offset = 0, save_to_db=False, store=None):
    store = require_textgrid_store(store)
    update_db_save_state(store)
    handle_db_save_option(store, save_to_db=save_to_db)
    names = tg.getNames()
    assert 'MAS' in names, "TextGrid must contain 'MAS' tier for syllables."
    syllables = tg.tiers[names.index('MAS')]
    
    for syl in syllables:
        if syl.mark == '<p:>': continue
        yield interval_to_syllable(syl, offset=offset, store=store)
    handle_db_save_option(store, revert=True)

def textgrid_to_phones(tg, offset = 0, save_to_db=False, store=None):
    
    store = require_textgrid_store(store)
    update_db_save_state(store)
    handle_db_save_option(store, save_to_db=save_to_db)
    names = tg.getNames()
    assert 'MAU' in names, "TextGrid must contain 'MAU' tier for phones."
    phones= tg.tiers[names.index('MAU')]
    
    for phone in phones:
        if phone.mark == '(...)': continue
        yield interval_to_phone(phone, offset=offset, store=store)
    handle_db_save_option(store, revert=True)


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
    o = model_class(start=start, end=end, label=interval.mark, store=store,
        **kwargs)
    return o
    
def handle_db_save_option(store, save_to_db = None, revert = None):
    global db_save_state
    if store is None: return
    if save_to_db is None and revert is None:return
    if revert:
        if db_save_state:
            turn_on_db_saving(store)
        else:
            turn_off_db_saving(store)
    elif save_to_db is not None:
        if save_to_db:
            turn_on_db_saving(store)
        else:
            turn_off_db_saving(store)
        
def update_db_save_state(store):
    global db_save_state
    if store is None: return
    db_save_state = store.is_db_saving_allowed()

def turn_off_db_saving(store):
    store.disable_writes()

def turn_on_db_saving(store):
    store.enable_writes()

def select_objecs_in_range(objects, start, end):
    selected = []
    for obj in objects:
        if obj.start >= start and obj.end <= end:
            selected.append(obj)
    return selected

def find_and_add_syllables_to_word(word, syllables, save_to_db=False):
    store = word.store if save_to_db else None
    update_db_save_state(store)
    handle_db_save_option(store, save_to_db=save_to_db)
    syllables = select_objecs_in_range(syllables, word.start, word.end)
    for syl in syllables:
        syl.add_parent(word, update_database = save_to_db)
    handle_db_save_option(store, revert=True)

def find_and_add_phones_to_syllable(syllable, phones, save_to_db=False,
    assign_positions=True):
    store = syllable.store if save_to_db else None
    update_db_save_state(store)
    handle_db_save_option(store, save_to_db=save_to_db)
    phones = select_objecs_in_range(phones, syllable.start, syllable.end)
    for phone in phones:
        phone.add_parent(syllable, update_database = save_to_db)
    if assign_positions:
        try:
            # in-memory; position_code is persisted by the later db save
            syllable_structure.assign_syllable_positions_to_phones(phones)
        except ValueError:
            pass  # odd transcription -> leave phones at default ('unknown')
    handle_db_save_option(store, revert=True)


def load_single_audio_and_transcription_to_db(audio_filename, text = None, 
    speaker = None,textgrid_filename = None, do_force_align = False, 
    save_to_db = True, textgrid_output_dir = None, store=None):
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
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
        store=store)
    return db_objects

def load_single_audio_textgrid_to_db(audio_filename, textgrid_filename,
    speaker = None, save_to_db = True, store=None):
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    audio_object = audio_filename_to_db_object(
        audio_filename, save_to_db=False, store=store)
    items = textgrid_filename_to_database_objects(textgrid_filename,
        audio=audio_object, speaker=speaker, save_to_db=False, store=store)
    db_objects = [audio_object] + items
    if save_to_db: save_items_to_db(db_objects, store=store)
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
    save_to_db = True, store=None):
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    assert len(audio_filenames) == len(textgrid_filenames) 
    if speakers is not None:
        assert len(textgrid_filenames) == len(speakers)
    db_objects = []
    for i in progressbar(range(len(audio_filenames))):
        audio_filename = audio_filenames[i]
        textgrid_filename = textgrid_filenames[i]
        if speakers is not None:
            speaker = speakers[i]
        else: speaker = None
        objs = load_single_audio_textgrid_to_db(
            audio_filename, textgrid_filename, speaker = speaker,
            save_to_db = save_to_db, store=store)
        db_objects.extend(objs)
    return db_objects
    
def load_speaker_audios_textgrids_to_db(speaker, audio_filenames, 
    textgrid_filenames, save_to_db = True, store=None):
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    assert len(audio_filenames) == len(textgrid_filenames)
    speakers = [speaker] * len(audio_filenames)
    db_objects = load_audios_textgrids_to_db(
        audio_filenames, textgrid_filenames, speakers,
        save_to_db = save_to_db, store=store)
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

        
