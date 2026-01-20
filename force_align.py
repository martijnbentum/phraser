from webmaus import pipeline 

def force_align_multiple(texts, audiofilenames, start_times= None, 
    end_times = None, lanuguage = 'nld-NL', output_dir = '', overwrite = False):
    '''Performs forced alignment of multiple texts with their corresponding 
    audio files.
    texts: A list of texts to be aligned.
    audiofilenames: A list of paths to the audio files.
    start_times: Optional list of start times for alignment.
    end_times: Optional list of end times for alignment.
    lanuguage: Language code for alignment. see webmaus.utils.languages
    output_dir: Directory to save output files.
    overwrite: Whether to overwrite existing output files (ie textgrids).
    '''
    files = make_files(texts, audiofilenames, start_times, end_times)
    p = pipeline.Pipeline(files, output_dir, language = lanuguage,
        overwrite = overwrite)
    p.run()
    return p.infos

def force_align_single(text, audio_filename, start_time = None, end_time = None,
    output_dir = '', language = 'nld-NL', overwrite = False):
    '''Performs forced alignment of the given text with the provided audio file.
    text: The text to be aligned.
    audio_filename: The path to the audio file.
    start_time: Optional start time for alignment.
    end_time: Optional end time for alignment.
    lanuguage: Language code for alignment. see webmaus.utils.languages
    output_dir: Directory to save output files.
    overwrite: Whether to overwrite existing output files (ie textgrid).
    
    returns the alignment result.
    '''
    files = make_file(text, audio_filename, start_time, end_time, files = [])
    p = pipeline.Pipeline(files, output_dir, language = language, 
        overwrite = overwrite)
    p.run()
    if len(p.errors) > 0:
        print(f"WARNING: Errors during alignment: {p.errors}")
    info = p.infos[0]
    return info


def make_file(text, audio_filename, start_time = None, end_time = None, 
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
    
def make_files(texts, audiofilenames, start_times= None, end_times = None):
    '''create a list of entries for the files list used in force alignment
    '''
    files = []
    assert len(texts) == len(audiofilenames)
    if start_times is None:start_times = [None] * len(texts)
    else: assert len(start_times) == len(texts)
    if end_times is None:end_times = [None] * len(texts)
    else: assert len(end_times) == len(texts)
    
    for i in range(len(texts)):
        files = make_file(texts[i], audiofilenames[i], start_times[i], 
            end_times[i], files)

    return files
        
    
    
