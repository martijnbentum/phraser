import glob
import json
import locations
from pathlib import Path
from progressbar import progressbar
from textgrid import TextGrid

def textgrid_filenames(cgn_ort_directory = None):
    if cgn_ort_directory is None:
        cgn_ort_directory = locations.cgn_ort_directory
    fn = list(cgn_ort_directory.glob('*.ort'))
    return fn

def load_textgrid(filename):
    """Load a TextGrid file and return the TextGrid object."""
    tg = TextGrid.fromFile(filename)
    return tg

def make_or_load_ort_info_dict(cgn_ort_directory = None, overwrite=False):
    p = Path('../data/cgn_ort_info_dict.json')
    if not overwrite and p.exists():
        with open(p, 'r', encoding='utf-8') as f:
            d = json.load(f)
        return d
    fn = textgrid_filenames(cgn_ort_directory)
    d = {}
    for f in progressbar(fn):
        tg = load_textgrid(f)
        d[f.stem] = {'filename':str(f), 'tier_names': tg.getNames()}
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(d, f)
    return d
    
