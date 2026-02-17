import struct
import utils

CLASS_RANK_MAP = {
    "Audio":    0,
    "Phrase":   1,
    "Word":     2,
    "Syllable": 3,
    "Phone":    4,   
    "Speaker":  5,
}

RANK_CLASS_MAP = utils.reverse_dict(CLASS_RANK_MAP)



def key_token_for_field(field_name):
    '''Map key fields to struct fmt tokens for binary LMDB keys.
    field_name: one of rank, audio_uuid, segment_uuid, start_ms, end_ms
    '''
    field_name = field_name.lower()

    if field_name in 'class_id':
        return 'B'     # u8
    if field_name == 'uuid':
        return '8s'    # 8 bytes
    if field_name == 'start_ms':
        return 'I'     # u32

    raise ValueError(f'Unknown key field: {field_name}')

def make_key_fields_for_class(class_name):
    '''Return ordered key field names for a class-specific LMDB key layout.
    class_name: one of audio, speaker, phrase, word, syllable, phone
    '''
    class_name = class_name.lower()

    if class_name == 'audio':
        return ['class_id', 'uuid', 'class_id']

    if class_name == 'speaker':
        return ['class_id', 'uuid']

    if class_name == 'speaker_audio':
        return ['uuid', 'uuid']

    if class_name in ['phrase', 'word', 'syllable', 'phone', 'segment']:
        return ['class_id', 'uuid', 'class_id', 'start_ms', 'uuid']

    raise ValueError(f'Unknown class: {class_name}')

def _make_key_fields_for_time_scan():
    return ['class_id', 'uuid', 'class_id', 'start_ms']

def make_key_fmt_for_time_scan(byte_order='>'):
    fields = _make_key_fields_for_time_scan()
    tokens = [byte_order] + [key_token_for_field(f) for f in fields]
    return ''.join(tokens)

def make_key_fmt_for_class(class_name, byte_order='>'):
    '''Return struct fmt string for a class-specific LMDB key layout.
    class_name: one of audio, speaker, phrase, word, syllable, phone
    byte_order: endianness, '>' for big-endian
    '''
    fields = make_key_fields_for_class(class_name)
    tokens = [byte_order] + [key_token_for_field(f) for f in fields]
    return ''.join(tokens)

def hex_to_8_bytes(hex_str):
    '''Convert 16-hex-char string to 8 raw bytes.'''
    if len(hex_str) != 16:
        raise ValueError(f'Expected 16 hex chars, got {len(hex_str)}')
    return bytes.fromhex(hex_str)
