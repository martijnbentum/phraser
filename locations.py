from pathlib import Path

cgn_base = Path('/vol/bigdata/corpora2/CGN2/')
cgn_audio = cgn_base / 'data/audio/wav/'
cgn_annotations = cgn_base / 'data/annot/text/'
cgn_ort = cgn_annotations / 'ort/'
cgn_awd = cgn_annotations / 'awd/'
cgn_fon = cgn_annotations / 'fon/'
cgn_speaker_file = cgn_base / 'metadata/speakers.csv'

data = Path('../data')

ort = data / 'ort/'
awd = data / 'awd/'
fon = data / 'fon/'
