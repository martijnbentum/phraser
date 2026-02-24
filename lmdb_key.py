import os
import struct_key
import struct_helper
from struct_helper import CLASS_RANK_MAP, RANK_CLASS_MAP

def instance_to_child_time_scan_keys(instance, child_class = None):
    return struct_key.instance_to_child_time_scan_keys(instance, child_class)

def instance_to_key(instance):
    return struct_key.instance_to_key(instance)
    
def make_identifier():
    return os.urandom(8)

def key_to_info(key):
    return struct_key.unpack_key(key)
    
def audio_id_segment_id_class_to_key(audio_id, segment_id, object_type, 
    offset_ms):
    '''Make LMDB key for a segment.
    audio_id:  audio UUID
    segment_id:  segment UUID
    object_type: one of Audio, Phrase, Word, Syllable, Phone
    offset: integer offset in milliseconds
    '''
    rank = CLASS_RANK_MAP[object_type]
    return struct_key.pack_segment_key(audio_id, rank, offset_ms, segment_id)

def audio_id_to_key(audio_id):
    '''Make LMDB key for an audio.
    audio_id:  audio UUID
    '''
    return struct_key.pack_audio_key(audio_id)

def speaker_id_to_key(speaker_id):
    '''Make LMDB key for a speaker.
    speaker_id:  speaker UUID
    '''
    return struct_key.pack_speaker_key(speaker_id)


def instance_to_rank(instance):
    '''Map an object to its single-letter type code.
    Supports: Audio, Phrase, Word, Syllable, Phone.
    '''
    object_type = instance.__class__.__name__
    return CLASS_RANK_MAP[object_type]


def key_to_object_type(key):
    '''Get object type string from LMDB key.
    '''
    if len(key) == struct_key.AUDIO_LEN: return 'Audio'
    if len(key) == struct_key.SPEAKER_LEN: return 'Speaker'
    rank = key[9]
    return RANK_CLASS_MAP[rank]

def key_to_identifier(key):
    '''Get identifier (UUID) from LMDB key.
    '''
    if len(key) == struct_key.AUDIO_LEN: key[1:]
    if len(key) == struct_key.SPEAKER_LEN: return None
    return key[-8:]
    
def key_to_audio_identifier(key):
    if len(key) == struct_key.SPEAKER_LEN: return None
    return key[1:9]

def key_to_rank(key):
    '''Get rank (single-letter code) from LMDB key.
    '''
    if len(key) == struct_key.AUDIO_LEN: return 0
    if len(key) == struct_key.SPEAKER_LEN: return 5
    return key[9]

def audio_id_to_scan_prefix(audio_id, child_class = 'Phrase'):
    '''Get LMDB scan prefix for a given audio ID.
    '''
    return struct_key.pack_audio_prefix(audio_id, child_class)

def speaker_audio_link(speaker, audio):
    '''Make LMDB key for a speaker-audio pair.
    speaker_id:  speaker UUID
    audio_id:  audio UUID
    '''
    return struct_key.pack_speaker_audio_key(speaker.identifier, 
        audio.identifier)

def speaker_id_to_scan_prefix(speaker_id):
    '''Get LMDB scan prefix for a given speaker ID.
    '''
    return struct_key.make_speaker_scan_prefix(speaker_id)

