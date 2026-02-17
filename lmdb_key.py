import os
import struct_key
import struct_helper
from struct_helper import CLASS_RANK_MAP

def instance_to_child_time_scan_keys(instance):
    return struct_key.instance_to_child_time_scan_keys(instance)

def instance_to_key(instance):
    return struct_key.instance_to_key(instance)
    
def make_identifier(item):
    return os.urandom(8).hex()

def key_to_info(key):
    return struct_key.unpack_key(key)
    
def audio_id_segment_id_class_to_key(audio_id, segment_id, object_type, 
    offset_ms):
    '''Make LMDB key for a segment.
    audio_id: hex string of audio UUID
    segment_id: hex string of segment UUID
    object_type: one of Audio, Phrase, Word, Syllable, Phone
    offset_ms: integer offset in milliseconds
    '''
    rank = object_type_to_rank(object_type)
    return struct_key.pack_segment_key(audio_id, rank, offset_ms, segment_id)

def audio_id_to_key(audio_id):
    '''Make LMDB key for an audio.
    audio_id: hex string of audio UUID
    '''
    return struct_key.pack_audio_key(audio_id)

def speaker_id_to_key(speaker_id):
    '''Make LMDB key for a speaker.
    speaker_id: hex string of speaker UUID
    '''
    return struct_key.pack_speaker_key(speaker_id)


def instance_to_rank(instance):
    '''Map an object to its single-letter type code.
    Supports: Audio, Phrase, Word, Syllable, Phone.
    '''
    object_type = instance.__class__.__name__
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

def key_to_object_type(key):
    '''Get object type string from LMDB key.
    '''
    info = key_to_info(key)
    return info['object_type']

def key_to_identifier(key):
    '''Get identifier (UUID hex string) from LMDB key.
    '''
    info = key_to_info(key)
    return info['identifier']
    
def key_to_audio_identifier(key):
    info = key_to_info(key)
    if info['object_type'] == 'Audio':
        return info['identifier']
    if info['object_type'] == 'Speaker':
        return None
    return info['audio_identifier']

def audio_id_to_scan_prefix(audio_id, child_class = 'Phrase'):
    '''Get LMDB scan prefix for a given audio ID.
    '''
    return struct_key.pack_audio_prefix(audio_id, child_class)

