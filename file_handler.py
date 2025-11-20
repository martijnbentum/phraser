import gzip
import json
import locations
import os
import pickle
import re
from textgrid import TextGrid

def load_textgrid(textgrid_type = 'ort', 
    number = None, filename = None, cgn_id = None):
    if filename is None and number is None and cgn_id is None:
        m = f'Must provide at least one of filename, number, or cgn_id'
        raise ValueError(m)
    print(f'Loading {textgrid_type} TextGrid')
    directory = getattr(locations, textgrid_type)
    if cgn_id: 
        print(f'Using cgn_id: {cgn_id}')
        filename = directory / f'{cgn_id}.{textgrid_type}'
    if number: 
        print(f'Using number: {number}')
        filename = find_file_by_number(directory, number)
    if filename is None: 
        m = f'Could not find file for {textgrid_type} with number {number}'
        m += f' or cgn_id {cgn_id}'
        print(m)
        return None
    print(f'loading from file: {filename}')
    return TextGrid.fromFile(str(filename))
    

def load_fon_textgrid(number = None, filename = None, cgn_id = None):
    return load_textgrid('fon', number, filename, cgn_id)

def load_ort_textgrid(number = None, filename = None, cgn_id = None):
    return load_textgrid('ort', number, filename, cgn_id)

def load_awd_textgrid(number = None, filename = None, cgn_id = None):
    return load_textgrid('awd', number, filename, cgn_id)


def load_gz_iso_file(filename):
    with gzip.open(filename, 'rt', encoding='iso-8859-1') as f:
        text = f.read()
    return text

def save_text_file(filename, text):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(text)

def load_text_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        text = f.read()
    return text

def convert_iso_to_utf8(input_filename, output_filename):
    print(f'Converting {input_filename} to {output_filename}')
    text = load_gz_iso_file(input_filename)
    save_text_file(output_filename, text)

def convert_ort_files():
    for gz_file in locations.cgn_ort.glob('**/*.ort.gz'):
        output_file = locations.ort / gz_file.stem  # Remove .gz extension
        if output_file.exists(): continue
        convert_iso_to_utf8(gz_file, output_file)

def convert_awd_files():
    for gz_file in locations.cgn_awd.glob('**/*.awd.gz'):
        output_file = locations.awd / gz_file.stem  # Remove .gz extension
        if output_file.exists(): continue
        convert_iso_to_utf8(gz_file, output_file)

def convert_fon_files():
    for gz_file in locations.cgn_fon.glob('**/*.fon.gz'):
        output_file = locations.fon / gz_file.stem  # Remove .gz extension
        if output_file.exists(): continue
        convert_iso_to_utf8(gz_file, output_file)

def find_file_by_number(directory, number):
    number = int(number)
    pattern = re.compile(r'\d+')   # match any digit block

    for fname in os.listdir(directory):
        match = pattern.search(fname)
        if match:
            if int(match.group()) == number:
                return os.path.join(directory, fname)
    return None

def find_cgn_id_by_number(number, annotation_type='ort'):
    directory = getattr(locations, annotation_type)
    filepath = find_file_by_number(directory, number)
    if filepath is None:
        return None
    filename = os.path.basename(filepath)
    cgn_id = filename.split('.')[0]
    return cgn_id

def load_speaker_file():
    with locations.cgn_speaker_file.open('r', encoding='utf-8') as f:
        lines = [x.split('\t') for x in f.read().split('\n') if x]
    header = lines[0]
    data = lines[1:]
    return header, data

def load_speaker_info(speaker_id, speaker_file = None):
    if speaker_file: header, data = speaker_file
    else: header, data = load_speaker_file()
    for row in data:
        if row[4] == speaker_id:
            info = dict(zip(header, row))
            return info
    return None

def load_number_to_cgn_id_dict():
    if locations.number_to_cgn_id_file.exists():
        with locations.number_to_cgn_id_file.open('r', encoding='utf-8') as f:
            d = json.load(f)
        return {int(k):v for k,v in d.items()}
    d = {}
    for ort in locations.ort.glob('**/*.ort'):
        cgn_id = ort.stem
        number = int(re.search(r'\d+', ort.name).group())
        d[number] = cgn_id
    with locations.number_to_cgn_id_file.open('w', encoding='utf-8') as f:
        json.dump(d, f, indent=4)
    return d

def load_audio_files():
    if locations.audio_filenames.exists():
        with locations.audio_filenames.open('r', encoding='utf-8') as f:
            audio_files = [x for x in f.read().split('\n') if x]
        return audio_files
    audio_files = list(locations.cgn_audio.glob('**/*.wav'))
    with locations.audio_filenames.open('w', encoding='utf-8') as f:
        f.write('\n'.join([str(x) for x in audio_files]))
    return audio_files

def load_speaker_ids():
    if locations.speaker_ids_file.exists():
        with locations.speaker_ids_file.open('r', encoding='utf-8') as f:
            speaker_ids = [x for x in f.read().split('\n')]
        return speaker_ids
    import speaker
    speakers = speaker.Speakers()
    speaker_ids = [s.speaker_id for s in speakers.speakers]
    with locations.speaker_ids_file.open('w', encoding='utf-8') as f:
        f.write('\n'.join(speaker_ids))
    return speaker_ids

def filename_to_cgn_id(filename):
    cgn_id = str(filename).split('/')[-1].split('.')[0]
    return cgn_id

def load_cgn_id_to_audio_filename_dict():
    if locations.cgn_id_to_audio_filenames_dict.exists():
        with locations.cgn_id_to_audio_filenames_dict.open('r') as f:
            d = json.load(f)
        return d
    audio_filenames = load_audio_files()
    d = {}
    for filename in audio_filenames:
        cgn_id = filename_to_cgn_id(filename)
        d[cgn_id] = str(filename)
    with locations.cgn_id_to_audio_filenames_dict.open('w') as f:
        json.dump(d, f, indent=4)
    return d


def load_cgn_id_to_component_dict():
    if locations.cgn_id_to_component_dict.exists():
        with locations.cgn_id_to_component_dict.open('r') as f:
            d = json.load(f)
        return d
    d = load_cgn_id_to_audio_filename_dict()
    output = {}
    for cgn_id, filename in d.items():
        component = filename.split('/')[-3].split('-')[-1]
        output[cgn_id] = component
    with locations.cgn_id_to_component_dict.open('w') as f:
        json.dump(output, f, indent=4)
    return output
    
    


class FileMapper:
    def __init__(self):
        self._create_file_map('ort')
        self._create_file_map('awd')
        self._create_file_map('fon')
        self._load_number_to_cgn_id()
        self.audio_filenames = load_audio_files()
        self.cgn_id_to_audio_filenames_dict = load_cgn_id_to_audio_filename_dict()
        self.cgn_id_to_component_dict = load_cgn_id_to_component_dict()

    def __repr__(self):
        m = 'FileMapper' 
        m += f' ort files: {self.n_ort},'
        m += f' awd files: {self.n_awd},'
        m += f' fon files: {self.n_fon},'
        return m

    def _load_number_to_cgn_id(self):
        self.number_to_cgn_id = load_number_to_cgn_id_dict()

    def _filenames(self, annotation_type):
        directory = getattr(locations, annotation_type)
        setattr(self, f'{annotation_type}_filenames', 
            list(directory.glob('**/*.' + annotation_type)))

    def _create_file_map(self, annotation_type):
        map_filename = getattr(locations, f'{annotation_type}_filemap')
        if map_filename.exists():
            with map_filename.open('r', encoding='utf-8') as f:
                d = json.load(f)
            setattr(self, f'{annotation_type}', d)
            return

        if not hasattr(self, f'{annotation_type}_filenames'):
            self._filenames(annotation_type)

        filenames = getattr(self, f'{annotation_type}_filenames')
        d = {}
        for filename in filenames:
            cgn_id = filename.stem
            d[cgn_id] = str(filename)
        setattr(self,f'{annotation_type}', d)
        setattr(self,f'n_{annotation_type}', len(d))

        print(f'Saving {annotation_type} file map to {map_filename}')
        with map_filename.open('w', encoding='utf-8') as f:
            json.dump(d, f, indent=4)

    def cgn_id_to_audio_filename(self, cgn_id):
        return self.cgn_id_to_audio_filenames_dict[cgn_id]

    def cgn_id_to_component(self, cgn_id):
        return self.cgn_id_to_component_dict[cgn_id]
        
    def cgn_id_to_textgrid(self, cgn_id, annotation_type='ort'):
        d = getattr(self, annotation_type)
        if cgn_id in d:
            filename = d[cgn_id]
        return TextGrid.fromFile(filename)

    def number_to_textgrid(self, number, annotation_type = 'ort'):
        cgn_id = self.number_to_cgn_id[number]
        return self.cgn_id_to_textgrid(cgn_id, annotation_type)

def load_pickled_annotations(parent, annotation_type):
    filename = getattr(locations, f'{annotation_type}_annotation_pickle')
    with filename.open('rb') as f:
        annotations = pickle.load(f)
    return annotations



file_mapper = FileMapper()
