import gzip
import locations

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
