import struct

AUDIO_RANK = 0
PHRASE_RANK = 1
WORD_RANK = 2
SYLLABLE_RANK = 3
PHONE_RANK = 4
SPEAKER_RANK = 5

class_rank_map = {0: 'Audio', 1: 'Phrase', 2: 'Word', 3: 'Syllable', 4: 'Phone',
    5: 'Speaker'}


def key_token_for_field(field_name):
    '''Map key fields to struct fmt tokens for binary LMDB keys.
    field_name: one of rank, audio_uuid, segment_uuid, offset_ms
    '''
    field_name = field_name.lower()

    if field_name in 'class_id':
        return 'B'     # u8
    if field_name == 'uuid':
        return '8s'    # 8 bytes
    if field_name == 'offset_ms':
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

    if class_name in ['phrase', 'word', 'syllable', 'phone', 'segment']:
        return ['class_id', 'uuid', 'class_id', 'offset_ms', 'uuid']

    raise ValueError(f'Unknown class: {class_name}')

def make_key_fmt_for_class(class_name, byte_order='>'):
    '''Return struct fmt string for a class-specific LMDB key layout.
    class_name: one of audio, speaker, phrase, word, syllable, phone
    byte_order: endianness, '>' for big-endian
    '''
    fields = make_key_fields_for_class(class_name)
    tokens = [byte_order] + [key_token_for_field(f) for f in fields]
    return ''.join(tokens)
