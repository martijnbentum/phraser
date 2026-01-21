from pathlib import Path

data = Path('../data')
data.mkdir(parents=True, exist_ok=True)
default_lmdb = data / 'default_lmdb'
cgn_lmdb = data / 'cgn_lmdb'

textgrids = data / 'textgrids'
cgn_ort_directory = data / 'ort'
