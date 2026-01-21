import glob
import locations
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
