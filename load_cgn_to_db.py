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

def get_speaker_names_in_db(reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    speakers = list(models.Speaker.objects.all())
    names = [s.name for s in speakers]
    duplicates = find_duplicates(names)
    if duplicates:
        print(f'found {len(duplicates)} duplicate speaker names in database:')
        for d in duplicates:
            print(f'  {d}')
    return names

def load_audio(audio_infos = None, reconnect_db = True):
    if audio_infos is None:
        audio_infos = process_cgn.make_or_load_audio_info()
    skipped, added, audios = [], [], []
    fn = get_filenames_of_audios_in_db(reconnect_db = reconnect_db)
    print(f'making db {len(audio_infos)} audio objects')
    for audio_info in progressbar(audio_infos):
        p = Path(audio_info['filename'])
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

def load_speakers(speaker_infos = None, reconnect_db = True):
    if speaker_infos is None:
        speaker_infos = process_cgn.make_or_load_speaker_info()
    skipped, added, speakers = [], [], []
    speaker_names = get_speaker_names_in_db(reconnect_db = reconnect_db)
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

def get_db_speaker(speaker_name, reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    speaker = list(models.Speaker.objects.filter(name=speaker_name))
    if len(speaker) == 0:
        print(f'WARNING speaker {speaker_name} not found in database')
    if len(speaker) > 1:
        print(f'WARNING multiple speakers found for {speaker_name} in database')
        return speaker[0]
    return speaker[0]

def get_db_audio(filename, reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    audio = list(models.Audio.objects.filter(filename=filename))
    if len(audio) == 0:
        print(f'WARNING audio {filename} not found in database')
    if len(audio) > 1:
        print(f'WARNING multiple audios found for {filename} in database')
        return audio[0]
    return audio[0]

def ort_info_to_speaker_and_audio(ort_info, reconnect_db = True):
    if reconnect_db: models.reconnect_db()
    sid = ort_info['tier_name']
    filename = ort_info['audio_filename']
    speaker = get_db_speaker(sid, reconnect_db = False)
    audio = get_db_audio(filename, reconnect_db = False)
    return speaker, audio

def handle_ort_info(ort_info, reconnect_db = True, save_to_db = True):
    if reconnect_db: models.reconnect_db()
    speaker, audio = ort_info_to_speaker_and_audio(
        ort_info, reconnect_db = False)
    textgrid = load_to_db.load_textgrid(ort_info['output_filename'])
    db_objects = load_to_db.textgrid_to_database_objects(
        textgrid, audio = audio, speaker = speaker, 
        save_to_db = save_to_db)
    return db_objects

    
def find_duplicates(strings):
    '''return items that occur more than once'''
    return [s for s, c in Counter(strings).items() if c > 1]
