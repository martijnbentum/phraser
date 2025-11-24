from pathlib import Path

cgn_base = Path('/vol/bigdata/corpora2/CGN2/')
cgn_audio = cgn_base / 'data/audio/wav/'
cgn_annotations = cgn_base / 'data/annot/text/'
cgn_ort = cgn_annotations / 'ort/'
cgn_awd = cgn_annotations / 'awd/'
cgn_fon = cgn_annotations / 'fon/'
cgn_speaker_file = cgn_base / 'data/meta/text/speakers.txt'

data = Path('../data')

ort = data / 'ort/'
awd = data / 'awd/'
fon = data / 'fon/'

audio_filenames = data / 'audio_filenames.txt'
cgn_id_to_audio_filenames_dict = data / 'cgn_id_to_audio_filenames.json'
cgn_id_to_component_dict = data / 'cgn_id_to_component.json'

number_to_cgn_id_file = data / 'number_to_cgn_id.json'
speaker_ids_file = data / 'speaker_ids.txt'

ort_filemap = data / 'ort_filemap.json'
awd_filemap = data / 'awd_filemap.json'
fon_filemap = data / 'fon_filemap.json'

ort_annotation_pickle = data / 'ort_annotations.pickle'
awd_annotation_pickle = data / 'awd_annotations.pickle'
fon_annotation_pickle = data / 'fon_annotations.pickle'

cgn_lmdb = data / 'cgn_lmdb'
