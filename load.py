import audio
import file_handler
import force_align
import locations
import models
from progressbar import progressbar

def load_single_audio_and_transcription_to_db(audio_filename, text, 
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
    audio_info = audio.audio_info(audio_filename)
    audio_object = models.Audio(**audio_info, save = False)
    tg = file_handler.load_textgrid(textgrid_filename)
    db_objects = file_handler.textgrid_to_database_objects(
        tg, audio = audio_object, speaker = speaker, save_to_db=save_to_db)
    return db_objects

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
    
    

