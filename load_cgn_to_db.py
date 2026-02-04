from collections import Counter
import process_cgn 
import load_to_db
import models
from pathlib import Path
from progressbar import progressbar

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


def save_audio_to_db(audio_infos = None, reconnect_db = True):
    if audio_infos is None:
        audio_infos = process_cgn.make_or_load_audio_info()
    skipped, added, audios = [], [], []
    fn = get_filenames_of_audios_in_db(reconnect_db = reconnect_db)
    print(f'making db {len(audio_infos)} audio objects')
    for audio_info in progressbar(audio_infos):
        p = Path(audio_info['filename'])
        audio_info['dataset'] = 'cgn'
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
    return speaker, audio

def ort_infos_to_db_items(ort_infos, reconnect_db = True):
    name_to_speaker_dict = make_cgn_speaker_name_to_db_speaker_dict()
    fn_to_audio_dict = make_cgn_audio_filename_to_db_audio_dict()
    add_items, errors, skipped, no_textgrid = [], [], [], []
    if reconnect_db: models.reconnect_db()
    for ort_info in progressbar(ort_infos):
        p = Path(ort_info['output_filename'])
        if not p.exists(): 
            no_textgrid.append(ort_info)
            continue
        speaker = name_to_speaker_dict.get(ort_info['tier_name'], None)
        audio = fn_to_audio_dict.get(ort_info['audio_filename'], None)
        if speaker is None or audio is None:
            errors.append((ort_info, 'missing speaker or audio in db'))
            continue
        db_items = ort_info_to_db_items(ort_info, speaker = speaker, 
            audio = audio)
        status = _check_db_status_items_ort_info(db_items, reconnect_db = False)
        if status == 'not in db':
            add_items.extend(db_items)
        elif status == 'all in db':
            skipped.append((ort_info['output_filename'], status))
        elif status == 'partly in db':
            errors.append((ort_info, db_items))
        else:
            print(f'WARNING: unknown status {status} for ort_info: {ort_info}')
    print(f'found {len(add_items)} items to add to database')
    print(f'found {len(skipped)} ort_infos fully in database')
    print(f'found {len(errors)} errors, ort_infos partly in database')
    print(f'found {len(no_textgrid)} ort_infos without textgrid file')
    return add_items, errors, skipped, no_textgrid
    

def ort_info_to_db_items(ort_info, speaker = None, audio = None):
    if speaker is None or audio is None:
        speaker, audio = ort_info_to_speaker_and_audio(
            ort_info, reconnect_db = False)
    textgrid = load_to_db.load_textgrid(ort_info['output_filename'])
    offset = ort_info['start_time']
    db_items = load_to_db.textgrid_to_database_objects(
        textgrid, offset = offset, audio = audio, speaker = speaker, 
        save_to_db = False)
    return db_items

def _check_db_status_items_ort_info(db_items, reconnect_db = True):
    existing_items, new_items = load_to_db.check_items_excists_in_db(db_items,
        reconnect_db = reconnect_db)
    if len(new_items) == len(db_items):
        status = 'not in db'
    elif len(existing_items) == len(db_items):
        status = 'all in db'
    else:
        status = 'partly in db'
    return status
    
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
