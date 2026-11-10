'''Import recordings: audio files plus TextGrids or text into a store.

Creates the Audio objects, force-aligns text when no TextGrid is given,
converts each TextGrid to segments and saves them.
'''

from progressbar import progressbar

from phraser import locations
from phraser import models
from phraser.audio import audio_info
from phraser.textgrids import force_align, save_to_store, textgrid_to_segments


def validate_textgrid_audio(audio, existing, store=None, save_to_db=True):
    save_to_store.validate_textgrid_existing_mode(existing)
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

def validate_sequence_lengths_match(left_name, left, right_name, right):
    if len(left) == len(right): return
    m = f'{left_name} and {right_name} must have matching lengths; '
    m += f'got {len(left)} and {len(right)}.'
    raise ValueError(m)

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
    items = textgrid_to_segments.textgrid_filename_to_database_objects(
        textgrid_filename, audio=audio, speaker=speaker, store=store)
    db_objects = [audio] + items
    if save_to_db:
        stored_objects = []
        if existing == 'append' and not store.DB.key_exists(audio.key):
            store.save(audio)
            stored_objects.append(audio)
        action = save_to_store.save_textgrid_items(items, store=store,
            existing=existing)
        if action != 'skipped': stored_objects.extend(items)
        return stored_objects
    return db_objects

def audio_filename_to_db_object(audio_filename, save_to_db = False, kwargs=None,
    store=None):
    if save_to_db and store is None:
        raise ValueError('store is required when save_to_db=True')
    kwargs = textgrid_to_segments.copy_kwargs(kwargs)
    info = audio_info(audio_filename)
    if info.keys() & kwargs.keys():
        m = 'WARNING: Conflicting keys in audio info and kwargs: '
        m += f'{info.keys() & kwargs.keys()}'
        print(m)
    info.update(kwargs)
    audio_object = models.Audio(**info, save = save_to_db, store=store)
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
