from pathlib import Path

data = Path('../data')
data.mkdir(parents=True, exist_ok=True)
default_lmdb = data / 'default_lmdb'
cgn_lmdb = data / 'cgn_lmdb'

audio_filenames = data / 'audio_filenames.txt'

textgrids = data / 'textgrids'
cgn_ort_directory = data / 'ort'

cgn_base = Path('/vol/bigdata/corpora2/CGN2/')
cgn_audio = cgn_base / 'data/audio/wav/'
cgn_speaker_file = cgn_base / 'data/meta/text/speakers.txt'
