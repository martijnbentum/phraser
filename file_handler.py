from . import force_align
from textgrid import TextGrid
import models

db_save_state = models.cache.is_db_saving_allowed

def load_audio_and_transcription_to_db(audio_filename, text, speaker = None):
    pass
    
    

def load_textgrid(filename):
    """Load a TextGrid file and return the TextGrid object."""
    tg = TextGrid.fromFile(filename)
    return tg

def textgrid_to_database_objects(tg, audio = None, speaker = None,
    save_to_db=False):
    words = list(textgrid_to_words(tg))
    syllables = list(textgrid_to_syllables(tg))
    phones = list(textgrid_to_phones(tg))
    phrase = words_to_phrase(words, False)
    if audio is not None: 
        phrase.add_audio(audio, update_database=False)
    if speaker is not None: 
        phrase.add_speaker(speaker, update_database=False)
    for word in words:
        find_and_add_syllables_to_word(word, syllables, save_to_db=False)
    for syllable in syllables:
        find_and_add_phones_to_syllable(syllable, phones, save_to_db=False)
    items = words + syllables + phones + [phrase]
    if audio is not None:items.append(audio)
    if speaker is not None:items.append(speaker)
    if save_to_db:
        update_db_save_state()
        handle_db_save_option(save_to_db=save_to_db)
        models.cache.save_many(items)
        handle_db_save_option(revert=True)
    return {'phrase': phrase, 'items': items} 
         
         

def words_to_phrase(words, save_to_db = False):
    words = list(words)
    if not words:
        return None
    start = words[0].start
    end = words[-1].end
    label = ' '.join([word.label for word in words])
    phrase = models.Phrase(start=start, end=end, label=label, save = save_to_db)
    for word in words:
        phrase.add_child(word, update_database = save_to_db)
    return phrase

def textgrid_to_words(tg, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    names = tg.getNames()
    assert 'ORT-MAU' in names, "TextGrid must contain 'ORT-MAU' tier for words."
    assert 'KAN-MAU' in names, "TextGrid must contain 'KAN-MAU' tier for words."
    ort_mau = tg.tiers[names.index('ORT-MAU')]
    kan_mau = tg.tiers[names.index('KAN-MAU')]
    
    for index, (ort, ipa) in enumerate(zip(ort_mau, kan_mau)):
        assert ort.minTime == ipa.minTime and ort.maxTime == ipa.maxTime, \
            "ORT-MAU and KAN-MAU tiers must have matching intervals."
        yield interval_to_word(ort, ipa)
    handle_db_save_option(revert=True)

def textgrid_to_syllables(tg, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    names = tg.getNames()
    assert 'MAS' in names, "TextGrid must contain 'MAS' tier for syllables."
    syllables = tg.tiers[names.index('MAS')]
    
    for syl in syllables:
        yield interval_to_syllable(syl)
    handle_db_save_option(revert=True)

def textgrid_to_phones(tg, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    names = tg.getNames()
    assert 'MAU' in names, "TextGrid must contain 'MAU' tier for phones."
    phones= tg.tiers[names.index('MAU')]
    
    for phone in phones:
        yield interval_to_phone(phone)
    handle_db_save_option(revert=True)


def interval_to_word(ort_interval, ipa_interval = None, kwargs = {}):
    if ipa_interval: 
        kwargs['ipa'] = ipa_interval.mark
    word = interval_to_database_object(ort_interval, models.Word, kwargs)
    return word
        
def interval_to_syllable(syl_interval, kwargs = {}):
    syllable = interval_to_database_object(syl_interval, models.Syllable, kwargs)
    return syllable

def interval_to_phone(phone_interval, kwargs = {}):
    phone = interval_to_database_object(phone_interval, models.Phone, kwargs)
    return phone

def interval_to_database_object(interval, model_class, kwargs={}):
    o = model_class(start=interval.minTime, end=interval.maxTime,
        label=interval.mark, **kwargs)
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
        word.add_child(syl, update_database = save_to_db)
    handle_db_save_option(revert=True)

def find_and_add_phones_to_syllable(syllable, phones, save_to_db=False):
    update_db_save_state()
    handle_db_save_option(save_to_db=save_to_db)
    phones = select_objecs_in_range(phones, syllable.start, syllable.end)
    for phone in phones:
        syllable.add_child(phone, update_database = save_to_db)
    handle_db_save_option(revert=True)
        
