import audio
from dutch_text_clean import clean
import glob
import json
import locations
from pathlib import Path
from progressbar import progressbar
from textgrid import TextGrid
import utils

def ort_textgrid_filenames(cgn_ort_directory = None):
    if cgn_ort_directory is None:
        cgn_ort_directory = locations.cgn_ort_directory
    fn = list(cgn_ort_directory.glob('*.ort'))
    return fn

def load_textgrid(filename):
    """Load a TextGrid file and return the TextGrid object."""
    tg = TextGrid.fromFile(filename)
    return tg

def load_speaker_file():
    with locations.cgn_speaker_file.open('r', encoding='utf-8') as f:
        lines = [x.split('\t') for x in f.read().split('\n') if x]
    header = lines[0]
    data = lines[1:]
    return header, data

def load_speaker_info(speaker_id, speaker_file = None, return_db_dict = False):
    if speaker_file: header, data = speaker_file
    else: header, data = load_speaker_file()
    for row in data:
        if row[4] == speaker_id:
            info = dict(zip(header, row))
            if return_db_dict:
                return speaker_info_to_database_dict(info)
            return info
    return None

def speaker_info_to_database_dict(speaker_info):
    gender = {'sex1':'male', 'sex2':'female'}
    try:age = 2000 - int(speaker_info['birthYear'])
    except ValueError: age = None
    identifier = speaker_info['ID']
    if identifier.startswith('N'):dialect = 'nl-NL' 
    elif identifier.startswith('V'): dialect = 'nl-BE'
    else: dialect = 'unknown'
    d = {}
    d['name'] = identifier
    d['gender'] = gender.get(speaker_info['sex'], 'unknown')
    d['age'] = age
    d['language'] = 'nld'
    d['dialect'] = dialect
    d['region'] = speaker_info['resRegion']
    return d

def audio_filename_to_database_dict(audio_filename):
    component = audio_filename_to_component(audio_filename)
    language = audio_filename_to_language(audio_filename)
    if language == 'nl': dialect = 'nl-NL'
    elif language == 'vl': dialect = 'nl-BE'
    else: dialect = 'unknown'
    info = audio.audio_info(audio_filename)
    info['component'] = component
    info['language'] = 'nld'
    info['dialect'] = dialect
    info['dataset'] = 'cgn'
    return info
    

def load_cgn_audio_filenames():
    with open(locations.audio_filenames, 'r', encoding='utf-8') as f:
        fn = f.read().split('\n')
    paths = [Path(x) for x in fn if x]
    return paths

def audio_filename_to_component(audio_filename):
    return audio_filename.parent.parent.name.split('-')[-1]

def audio_filename_to_language(audio_filename):
    return audio_filename.parent.name

def cgn_id_to_audio(cgn_id, cgn_audio_filenames = None):
    if cgn_audio_filenames is None:
        cgn_audio_filenames = load_cgn_audio_filenames()
    for p in cgn_audio_filenames:
        if p.stem == cgn_id:
            return p

def cgn_id_to_ort(cgn_id, cgn_ort_directory = None):
    if cgn_ort_directory is None:
        cgn_ort_directory = locations.cgn_ort_directory
    p = cgn_ort_directory / f'{cgn_id}.ort'
    if p.exists():
        return p

def textgrid_to_speaker_tiers(tg, exclude = ['BACKGROUND', 'COMMENT']):
    names = [name for name in tg.getNames() if name not in exclude]
    return names

def handle_tier(tg, tier_name, audio_filename, component, language):
    tier_index = tg.getNames().index(tier_name)
    tier = tg.tiers[tier_index]
    # speaker_info = load_speaker_info(tier_name, speakers_file)
    # output = {'speaker': speaker_info, 'intervals': []}
    output = []
    for interval in tier.intervals:
        start_time = interval.minTime
        end_time = interval.maxTime
        raw_text = interval.mark
        text = clean.clean_dutch_cgn(raw_text)
        output_dir = locations.textgrids / tier_name
        output_filname = make_output_filename(
            audio_filename, start_time, end_time, output_dir)
        if not text: continue
        d = {'start_time': start_time, 'end_time': end_time, 'text': text, 
            'raw_text': raw_text,
            'tier_name': tier_name, 'component': component, 
            'language': language,
            'audio_filename': str(audio_filename), 
            'output_directory': str(output_dir),
            'output_filename': str(output_filname)}
        output.append(d)
    return output

def cgn_id_to_info(cgn_id, cgn_audio_filenames = None, cgn_ort_directory = None):
    if cgn_audio_filenames is None: 
        cgn_audio_filenames = load_cgn_audio_filenames()
    speakers_info = load_speaker_file()
    audio_path = cgn_id_to_audio(cgn_id, cgn_audio_filenames)
    ort_path = cgn_id_to_ort(cgn_id, cgn_ort_directory)
    tg = load_textgrid(ort_path)
    comp = audio_filename_to_component(audio_path)
    language = audio_filename_to_language(audio_path)
    output = []
    tier_names = textgrid_to_speaker_tiers(tg)
    for tier_name in tier_names:
        tier_data = handle_tier(tg, tier_name, audio_path, comp, language)
        output.extend(tier_data)
    return output

def make_or_load_ort_info(fn = None, cgn_ort_directory = None, 
    overwrite=False):
    p = Path('../data/cgn_ort_info_dict.json')
    if not overwrite and p.exists():
        with open(p, 'r', encoding='utf-8') as f:
            ort_info = json.load(f)
        return ort_info
    if fn is None:
        fn = ort_textgrid_filenames(cgn_ort_directory)
    output = []
    for f in progressbar(fn):
        cgn_id = f.stem
        o = cgn_id_to_info(cgn_id)
        output.extend(o)
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(output, f)
    return output

def make_or_load_audio_info(fn = None, overwrite=False):
    p = Path('../data/cgn_audio_info_dict.json')
    if not overwrite and p.exists():
        with open(p, 'r', encoding='utf-8') as f:
            audio_info = json.load(f)
        return audio_info
    if fn is None:
        fn = load_cgn_audio_filenames()
    output = []
    for f in progressbar(fn):
        o = audio_filename_to_database_dict(f)
        output.append(o)
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(output, f)
    return output

def make_or_load_speaker_info(speaker_file = None, overwrite=False):
    p = Path('../data/cgn_speaker_info_dict.json')
    if not overwrite and p.exists():
        with open(p, 'r', encoding='utf-8') as f:
            speaker_info = json.load(f)
        return speaker_info
    header, data = load_speaker_file()
    output = []
    for row in data:
        if not row: continue
        info = dict(zip(header, row))
        db_info = speaker_info_to_database_dict(info)
        db_info['dataset'] = 'cgn'
        output.append(db_info)
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(output, f)
    return output

def make_output_filename(audio_filename, start_time, end_time, output_directory):
    audio_filename = Path(audio_filename)
    output_directory = Path(output_directory)
    name = audio_filename.stem
    start = f'_s-{int(start_time*1000)}' 
    end = f'-e-{int(end_time*1000)}' 
    output_filname = output_directory / f'{name}{start}{end}-ms.TextGrid'
    return output_filname
    
