import uuid

TYPE_TO_RANK_MAP = {
    "Audio":    0,
    "Phrase":   1,
    "Word":     2,
    "Syllable": 3,
    "Phone":    4,   
    "Speaker":  5,
}

def find_hex_length_based_on_n_items(n_items):
    '''Find the appropriate hex length for a given number of items.
    '''
    hex_length = {2**8:4, 2**12:6, 2**16:8, 2**20:10, 2**24:12,
        2**28:14, 2**32:16, 2**36:18, 2**40:20, 2**44:22, 2**48:24}
    for limit, length in hex_length.items():
        if n_items <= limit:
            return length
    raise ValueError(f'n_items {n_items} too large to determine hex length.')

def find_hex_length_based_on_object_type(item):
    hex_length = {'Audio':16, 'Speaker': 18, 'Phrase':12, 'Word':12,
        'Syllable':14, 'Phone':16}
    cls_name = item.__class__.__name__
    try:
        return hex_length[cls_name]
    except KeyError:
        raise ValueError(f"Unsupported segment type: {cls_name}")
    

def make_item_identifier(item, n_items = None):
    if n_items: hex_length = find_hex_length_based_on_n_items(n_items)
    else: hex_length = find_hex_length_based_on_object_type(item)
    identifier = uuid.uuid4().hex[:hex_length]
    object_type = item.__class__.__name__ 
    if object_type== 'Speaker': object_type= f'~{object_type}'
    identifier = f'{object_type}-{identifier}'
    return identifier

def item_to_key(item):
    '''Build an LMDB key for any segment-like object.
    
    Expected fields on item:
      - identifier      : unique ID string (for all non-audio)
      - start           : start time in seconds or milliseconds
      - audio_id        : ID of the audio file this belongs to
      - class type      : one of Audio, Phrase, Word, Syllable, Phone
    '''
    rank = object_to_rank(item)

    # audio and speaker only have identifier
    if rank in [0,5]: 
        return f"{item.identifier}:{rank}"

    # --- Everything else: Phrase, Word, Syllable, Phone ---
    # Normalize start time to milliseconds
    start_ms = int(round(item.start * 1000))

    # Zero-pad to 8 digits for lexicographic ordering
    start_str = f"{start_ms:08d}"
    audio_key = item.audio_key
    audio_id = key_to_identifier(audio_key)

    return f"{audio_id}:{rank}:{start_str}:{item.identifier}"

def object_to_rank(item):
    '''Map an object to its single-letter type code.
    Supports: Audio, Phrase, Word, Syllable, Phone.
    '''

    object_type = item.__class__.__name__
    return object_type_to_rank(object_type)

def object_type_to_rank(object_type):
    '''Map an object type string to its rank.
    '''
    try:
        return TYPE_TO_RANK_MAP[object_type]
    except KeyError:
        raise ValueError(f'Unsupported object type: {object_type}')


def rank_to_object_type(rank):
    '''Map a rank to its object type string.
    '''
    for k, v in TYPE_TO_RANK_MAP.items():
        if v == rank:
            return k
    raise ValueError(f'Unsupported rank: {rank}')

def object_to_scan_prefix(item, all_in_same_audio=False):
    '''Get LMDB scan prefix for a given segment type.
    does not work YET
    '''
    rank = object_to_rank(item)

    if rank == 0 or all_in_same_audio:
        # Audio files only have identifier
        return f'{item.identifier}:'

    # Everything else: Phrase, Word, Syllable, Phone
    return f'{rank}:'


def key_to_object_type(key):
    '''Get object type string from LMDB key.
    '''
    if ':' not in key:
        raise ValueError(f'Invalid key format: {key}')
    rank = int(key.split(":")[1]) 
    return rank_to_object_type(rank)

def key_to_identifier(key):
    if key == 'EMPTY': return key
    if ':' not in key:
        raise ValueError(f'Invalid key format: {key}')
    parts = key.split(":")
    if len(parts) == 2 and ('Audio' in key or 'Speaker' in key):
        return parts[0]
    return parts[-1]
    
