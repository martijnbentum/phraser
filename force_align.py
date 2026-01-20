from webmaus import pipeline 

def force_align_single(text, audio_filename, start_time = None, end_time = None,
    output_dir = '', language = 'nld-NL', overwrite = False):
    '''Performs forced alignment of the given text with the provided audio file.
    text: The text to be aligned.
    audio_filename: The path to the audio file.
    start_time: Optional start time for alignment.
    end_time: Optional end time for alignment.
    output_dir: Directory to save output files.
    
    returns the alignment result.
    '''
    files = make_files(text, audio_filename, start_time, end_time, files = [])
    p = pipeline.Pipeline(files, output_dir, language = language, 
        overwrite = overwrite)
    p.run()
    if len(p.errors) > 0:
        print(f"WARNING: Errors during alignment: {p.errors}")
    info = p.infos[0]
    return info


def make_files(text, audio_filename, start_time = None, end_time = None, 
    files = []):
    '''create and entry for the files list used in force alignment
    text: The text to be aligned.
    audio_filename: The path to the audio file.
    start_time: Optional start time for alignment.
    end_time: Optional end time for alignment.
    files: The list to which the entry will be added.
    Returns the updated files list.
    '''
    d = {'text': text, 'audio_filename': audio_filename, 
        'start_time': start_time, 'end_time': end_time}
    files.append(d)
    return files
    
    
    
