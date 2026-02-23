import audio
import force_align
import locations
import models
from progressbar import progressbar
from textgrid import TextGrid
import utils

db_save_state = models.cache.is_db_saving_allowed

def load_textgrid(filename):
    """Load a TextGrid file and return the TextGrid object."""
    tg = TextGrid.fromFile(filename)
    return tg

def save_items_to_db(items):
    update_db_save_state()
    handle_db_save_option(save_to_db=True)
    models.cache.save_many(items)
    handle_db_save_option(revert=True)

def textgrid_filename_to_database_objects(textgrid_filename, offset = 0, 
    audio = None, speaker = None, save_to_db=False, overwrite = False):
    
    tg = load_textgrid(textgrid_filename)
    words = list(textgrid_to_words(tg, offset))
    syllables = list(textgrid_to_syllables(tg, offset))
    phones = list(textgrid_to_phones(tg, offset))
    phrase = words_to_phrase(words, textgrid_filename = textgrid_filename)  
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
    if save_to_db: save_items_to_db(items)
    return items
         
def words_to_phrase(words, textgrid_filename):
    words = list(words)
    if not words:
        return None
    start = words[0].start
    end = words[-1].end
    label = ' '.join([word.label for word in words])
    phrase = models.Phrase(start=start, end=end, label=label, 
        filename =textgrid_filename, save = False)
    for word in words:
        word.add_parent(phrase, update_database = False)
    return phrase 

def textgrid_to_words(tg, offset = 0, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    names = tg.getNames()
    assert 'ORT-MAU' in names, "TextGrid must contain 'ORT-MAU' tier for words."
    assert 'KAN-MAU' in names, "TextGrid must contain 'KAN-MAU' tier for words."
    ort_mau = tg.tiers[names.index('ORT-MAU')]
    kan_mau = tg.tiers[names.index('KAN-MAU')]
    
    for index, (ort, ipa) in enumerate(zip(ort_mau, kan_mau)):
        if ort.mark == '': continue
        assert ort.minTime == ipa.minTime and ort.maxTime == ipa.maxTime, \
            "ORT-MAU and KAN-MAU tiers must have matching intervals."
        yield interval_to_word(ort, ipa, offset=offset)  
            
    handle_db_save_option(revert=True)

def textgrid_to_syllables(tg, offset = 0, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    names = tg.getNames()
    assert 'MAS' in names, "TextGrid must contain 'MAS' tier for syllables."
    syllables = tg.tiers[names.index('MAS')]
    
    for syl in syllables:
        if syl.mark == '<p:>': continue
        yield interval_to_syllable(syl, offset=offset)
    handle_db_save_option(revert=True)

def textgrid_to_phones(tg, offset = 0, save_to_db=False):
    
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    names = tg.getNames()
    assert 'MAU' in names, "TextGrid must contain 'MAU' tier for phones."
    phones= tg.tiers[names.index('MAU')]
    
    for phone in phones:
        if phone.mark == '(...)': continue
        yield interval_to_phone(phone, offset=offset)
    handle_db_save_option(revert=True)


def interval_to_word(ort_interval, ipa_interval = None, offset = 0, 
    kwargs = {}):
    if ipa_interval: 
        kwargs['ipa'] = ipa_interval.mark
    word = interval_to_database_object(ort_interval, models.Word, 
        offset, kwargs)
    return word
        
def interval_to_syllable(syl_interval, offset = 0, kwargs = {}):
    syllable = interval_to_database_object(syl_interval, models.Syllable, 
    offset, kwargs)
    return syllable

def interval_to_phone(phone_interval, offset = 0, kwargs = {}):
    phone = interval_to_database_object(phone_interval, models.Phone, 
    offset, kwargs)
    return phone

def interval_to_database_object(interval, model_class, offset = 0, kwargs={}):
    start = utils.seconds_to_miliseconds(interval.minTime + offset)
    end = utils.seconds_to_miliseconds(interval.maxTime + offset)
    o = model_class(start = start, end= end, label = interval.mark, **kwargs)
    return o
    
def handle_db_save_option(save_to_db = None, revert = None):
    global db_save_state
    if save_to_db is None and revert is None:return
    if revert:
        if db_save_state:
            turn_on_db_saving()
        else:
            turn_off_db_saving()
    elif save_to_db is not None:
        if save_to_db:
            turn_on_db_saving()
        else:
            turn_off_db_saving()
        
def update_db_save_state():
    global db_save_state
    db_save_state = models.cache.is_db_saving_allowed

def turn_off_db_saving():
    models.turn_off_db_saving()

def turn_on_db_saving():
    models.turn_on_db_saving()

def select_objecs_in_range(objects, start, end):
    selected = []
    for obj in objects:
        if obj.start >= start and obj.end <= end:
            selected.append(obj)
    return selected

def find_and_add_syllables_to_word(word, syllables, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    syllables = select_objecs_in_range(syllables, word.start, word.end)
    for syl in syllables:
        syl.add_parent(word, update_database = save_to_db)
    handle_db_save_option(revert=True)

def find_and_add_phones_to_syllable(syllable, phones, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    phones = select_objecs_in_range(phones, syllable.start, syllable.end)
    for phone in phones:
        phone.add_parent(syllable, update_database = save_to_db)
    handle_db_save_option(revert=True)


def load_single_audio_and_transcription_to_db(audio_filename, text = None, 
    speaker = None,textgrid_filename = None, do_force_align = False, 
    save_to_db = True, textgrid_output_dir = None):
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
        textgrid_filename, speaker = speaker, save_to_db = save_to_db)
    return db_objects

def load_single_audio_textgrid_to_db(audio_filename, textgrid_filename,
    speaker = None, save_to_db = True):
    tg = load_textgrid(textgrid_filename)
    db_objects = textgrid_to_database_objects(
        tg, audio = audio_object, speaker = speaker, save_to_db=save_to_db)
    return db_objects

def audio_filename_to_db_object(audio_filename, save_to_db = False, kwargs={}):
    audio_info = audio.audio_info(audio_filename)
    if audio_info.keys() & kwargs.keys():
        m = 'WARNING: Conflicting keys in audio info and kwargs: '
        m += f'{audio_info.keys() & kwargs.keys()}'
        print(m)
    audio_info.update(kwargs)
    audio_object = models.Audio(**audio_info, save = save_to_db)
    return audio_object

def load_audios_textgrids_to_db(audio_filenames, textgrid_filenames, speakers, 
    save_to_db = True):
    assert len(audio_filenames) == len(texts) 
    if speaker is not None: assert len(texts) == len(speakers)
    db_objects = []
    for i in progressbar(range(len(audio_filenames))):
        audio_filename = audio_filenames[i]
        textgrid_filename = textgrid_filenames[i]
        if speakers is not None:
            speaker = speakers[i]
        else: speaker = None
        objs = load_single_audio_textgrid_to_db(
            audio_filename, textgrid_filename, speaker = speaker,
            save_to_db = save_to_db)
        db_objects.extend(objs['items'])
    return db_objects
    
def load_speaker_audios_textgrids_to_db(speaker, audio_filenames, 
    textgrid_filenames, save_to_db = True):
    assert len(audio_filenames) == len(textgrid_filenames)
    speakers = [speaker] * len(audio_filenames)
    db_objects = load_audios_textgrids_to_db(
        audio_filenames, textgrid_filenames, speakers,
        save_to_db = save_to_db)
    
def get_phrases_from_items(items):
    phrases = [item for item in items if isinstance(item, models.Phrase)]
    return phrases

def check_items_excists_in_db(db_items, reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    existing_items = []
    new_items = []
    for item in db_items:
        if item.exists_in_db: 
            existing_items.append(item)
        else:
            new_items.append(item)
    return existing_items, new_items

        
