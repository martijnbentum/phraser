from collections import Counter
import process_cgn 
import load_to_db
import models
from pathlib import Path
from progressbar import progressbar
from utils import seconds_to_miliseconds

def get_filenames_of_audios_in_db(reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    audios = list(models.Audio.objects.all())
    fn = [Path(x.filename).name for x in audios]
    duplicates = find_duplicates(fn)
    if duplicates:
        print(f'found {len(duplicates)} duplicate filenames in database:')
        for d in duplicates:
            print(f'  {d}')
    return fn

def get_cgn_speaker_names_in_db(reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    speakers = list(models.Speaker.objects.filter(dataset='cgn'))
    names = [s.name for s in speakers]
    duplicates = find_duplicates(names)
    if duplicates:
        print(f'found {len(duplicates)} duplicate speaker names in database:')
        for d in duplicates:
            print(f'  {d}')
    return names

def get_cgn_textgrid_filenames_in_db(reconnect_db = True):
    phrases = list(models.Phrase.objects.filter(audio__dataset='cgn'))
    textgrid_fn = [x.filename for x in phrases]
    return textgrid_fn


def save_audio_to_db(audio_infos = None, reconnect_db = True):
    if audio_infos is None:
        audio_infos = process_cgn.make_or_load_audio_info()
    skipped, added, audios = [], [], []
    fn = get_filenames_of_audios_in_db(reconnect_db = reconnect_db)
    print(f'making db {len(audio_infos)} audio objects')
    for audio_info in progressbar(audio_infos):
        p = Path(audio_info['filename'])
        audio_info['dataset'] = 'cgn'
        audio_info['duration'] = seconds_to_miliseconds(audio_info['duration'])
        name = p.name
        if name in fn:
            skipped.append(name)
            continue
        audio = models.Audio(**audio_info, save=False)
        audios.append(audio)
        added.append(name)
    print(f'saving {len(audios)} audio objects to database')
    load_to_db.save_items_to_db(audios)
    print(f'added {len(added)} new audio objects')
    print(f'skipped {len(skipped)} existing audio objects')
    models.reconnect_db()
    return added, skipped

def save_cgn_speakers_to_db(speaker_infos = None, reconnect_db = True):
    if speaker_infos is None:
        speaker_infos = process_cgn.make_or_load_speaker_info()
    skipped, added, speakers = [], [], []
    speaker_names = get_cgn_speaker_names_in_db(reconnect_db = reconnect_db)
    print(f'making db {len(speaker_infos)} speaker objects')
    for speaker_info in progressbar(speaker_infos):
        name = speaker_info['name']
        if name in speaker_names:
            skipped.append(name)
            continue
        if speaker_info['age'] is None:
            speaker_info['age'] = 0
        speaker = models.Speaker(**speaker_info, save=False)
        speakers.append(speaker)
        added.append(name)
    print(f'saving {len(speakers)} speaker objects to database')
    load_to_db.save_items_to_db(speakers)
    print(f'added {len(added)} new speaker objects')
    print(f'skipped {len(skipped)} existing speaker objects')
    models.reconnect_db()
    return added, skipped

def get_db_cgn_speaker(speaker_name, reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    return models.Speaker.objects.get(name=speaker_name, dataset='cgn')

def get_db_audio(filename, reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    return models.Audio.objects.get(filename=filename) 

def ort_info_to_speaker_and_audio(ort_info, reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    sid = ort_info['tier_name']
    filename = ort_info['audio_filename']
    speaker = get_db_cgn_speaker(sid, reconnect_db = False)
    audio = get_db_audio(filename, reconnect_db = False)
    models.cache.DB.write_speaker_audio_link(speaker, audio)
    return speaker, audio

def ort_infos_to_db_items(ort_infos, reconnect_db = True, save = True):
    if reconnect_db: models.reconnect_db()
    name_to_speaker_dict = make_cgn_speaker_name_to_db_speaker_dict()
    fn_to_audio_dict = make_cgn_audio_filename_to_db_audio_dict()
    textgrid_fn = get_cgn_textgrid_filenames_in_db()

    add_items, errors, skipped, no_textgrid = [], [], [], []
    for ort_info in progressbar(ort_infos):
        p = Path(ort_info['output_filename'])
        if not p.exists(): 
            no_textgrid.append(ort_info)
            continue
        textgrid_filename = str(p)
        speaker = name_to_speaker_dict.get(ort_info['tier_name'], None)
        audio = fn_to_audio_dict.get(ort_info['audio_filename'], None)
        if speaker is None or audio is None:
            errors.append((ort_info, 'missing speaker or audio in db'))
            continue
        if textgrid_filename in textgrid_fn:
            skipped.append(ort_info)
            continue
        db_items = ort_info_to_db_items(ort_info, speaker = speaker, 
            audio = audio)
        add_items.extend(db_items)
    if save:
        print(f'saving {len(add_items)} db items to database')
        load_to_db.save_items_to_db(add_items)
    print(f'{len(add_items)} new db items')
    print(f'{len(skipped)} existing db items skipped')
    print(f'{len(errors)} ort infos with missing speaker or audio')
    print(f'{len(no_textgrid)} ort infos with missing textgrid file')
    return add_items, errors, skipped, no_textgrid
    

def ort_info_to_db_items(ort_info, speaker = None, audio = None):
    if speaker is None or audio is None:
        speaker, audio = ort_info_to_speaker_and_audio(
            ort_info, reconnect_db = False)
    else: models.cache.DB.write_speaker_audio_link(speaker, audio)
    textgrid_filename = ort_info['output_filename']
    offset = ort_info['start_time']
    db_items=load_to_db.textgrid_filename_to_database_objects(textgrid_filename,
         offset = offset, audio = audio, speaker = speaker, save_to_db = False)
    return db_items

    
def make_cgn_speaker_name_to_db_speaker_dict():
    speakers = models.Speaker.objects.filter(dataset='cgn')
    name_to_speaker_dict = {s.name: s for s in speakers}
    return name_to_speaker_dict
def make_cgn_audio_filename_to_db_audio_dict():
    audios = models.Audio.objects.filter(dataset='cgn')
    fn_to_audio_dict = {a.filename: a for a in audios}
    return fn_to_audio_dict

    
def find_duplicates(strings):
    '''return items that occur more than once'''
    return [s for s, c in Counter(strings).items() if c > 1]
