'''Turn TextGrids into stored recordings.

Modules:
    import_recordings      entry point: audio files + TextGrids/text -> store
    textgrid_to_segments   TextGrid -> linked segment objects (in memory)
    save_to_store          save segments; skip/replace phrases already stored
    force_align            text + audio -> TextGrid via webmaus

Typical use:
    load_speaker_audios_textgrids_to_db(speaker, audio_filenames,
        textgrid_filenames, store=store, existing='add_missing')
'''

from .import_recordings import audio_filename_to_db_object
from .import_recordings import load_audios_textgrids_to_db
from .import_recordings import load_single_audio_and_transcription_to_db
from .import_recordings import load_single_audio_textgrid_to_db
from .import_recordings import load_speaker_audios_textgrids_to_db
from .save_to_store import TEXTGRID_EXISTING_POLICIES, save_textgrid_items
from .textgrid_to_segments import textgrid_filename_to_database_objects

__all__ = ['TEXTGRID_EXISTING_POLICIES', 'audio_filename_to_db_object',
    'load_audios_textgrids_to_db', 'load_single_audio_and_transcription_to_db',
    'load_single_audio_textgrid_to_db', 'load_speaker_audios_textgrids_to_db',
    'save_textgrid_items', 'textgrid_filename_to_database_objects']
