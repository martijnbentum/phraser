import struct
import struct_helper

SPEAKER_FMT = struct_helper.make_key_fmt_for_class('speaker')  # '>B8s'
AUDIO_FMT   = struct_helper.make_key_fmt_for_class('audio')    # '>B8sB'
SEGMENT_FMT = struct_helper.make_key_fmt_for_class('segment')  # '>B8sBI8s'

SPEAKER_LEN = struct.calcsize(SPEAKER_FMT)  # 9
AUDIO_LEN   = struct.calcsize(AUDIO_FMT)    # 10
SEGMENT_LEN = struct.calcsize(SEGMENT_FMT)  # 22


def _hex_to_8_bytes(hex_str):
    '''Convert 16-hex-char string to 8 raw bytes.'''
    if len(hex_str) != 16:
        raise ValueError(f'Expected 16 hex chars, got {len(hex_str)}')
    return bytes.fromhex(hex_str)

# -------- pack --------

def object_to_key(obj):
    uuid = obj.identifier.split('-')[-1]
    if obj.object_type == 'Audio': return pack_audio_key(uuid)
    if obj.object_type == 'Speaker': return pack_speaker_key(uuid)
    if obj.object_type == 'Phrase': rank = PHRASE_RANK
    if obj.object_type == 'Word': rank = WORD_RANK
    if obj.object_type == 'Syllable': rank = SYLLABLE_RANK
    if obj.object_type == 'Phone': rank = PHONE_RANK
    offset = int(round(obj.start * 1000))
    audio_uuid = obj.audio.identifier.split('-')[-1]
    return pack_segment_key(audio_uuid, rank, offset, uuid)


def pack_speaker_key(speaker_uuid_hex):
    speaker_uuid = _hex_to_8_bytes(speaker_uuid_hex)
    return struct.pack(SPEAKER_FMT, SPEAKER_RANK, speaker_uuid)


def pack_audio_key(audio_uuid_hex):
    audio_uuid = _hex_to_8_bytes(audio_uuid_hex)
    return struct.pack(AUDIO_FMT, AUDIO_RANK, audio_uuid, AUDIO_RANK)


def pack_segment_key(audio_uuid_hex, class_rank, offset_ms, segment_uuid_hex):
    if not (0 <= offset_ms <= 0xFFFFFFFF):
        raise ValueError('offset_ms must fit in uint32')

    audio_uuid = _hex_to_8_bytes(audio_uuid_hex)
    segment_uuid = _hex_to_8_bytes(segment_uuid_hex)

    return struct.pack(SEGMENT_FMT, AUDIO_RANK, audio_uuid, class_rank, 
        offset_ms, segment_uuid)


# -------- unpack --------

def unpack_key(key_bytes):
    '''Dispatch by length and (for speaker) first byte != AUDIO_RANK.'''
    if not key_bytes:
        raise ValueError('Empty key')

    n = len(key_bytes)

    if n == AUDIO_LEN:
        audio_rank, audio_uuid, rank = struct.unpack(AUDIO_FMT, key_bytes)
        if audio_rank != AUDIO_RANK or rank != AUDIO_RANK:
            raise ValueError('Invalid audio key')
        return {
            'object_type': 'Audio',
            'audio_uuid': audio_uuid.hex(),
        }

    if n == SEGMENT_LEN:
        o =  struct.unpack(SEGMENT_FMT, key_bytes)
        audio_rank, audio_uuid, class_rank, offset_ms, segment_uuid = o
        if audio_rank != AUDIO_RANK:
            raise ValueError('Invalid segment key (audio_rank must be 0)')
        return {
            'object_type': class_rank_map[class_rank],
            'audio_uuid': audio_uuid.hex(),
            'offset_ms': offset_ms,
            'segment_uuid': segment_uuid.hex(),
        }

    if n == SPEAKER_LEN:
        speaker_rank, speaker_uuid = struct.unpack(SPEAKER_FMT, key_bytes)
        if speaker_rank == AUDIO_RANK:
            m = 'Ambiguous: 9-byte key with rank 0 reserved for audio/segments'
            raise ValueError(m)
        return {
            'object_type': 'Speaker',
            'speaker_uuid': speaker_uuid.hex(),
        }

    raise ValueError(f'Unknown key length: {n}')

