import struct
import struct_helper
from struct_helper import CLASS_RANK_MAP, RANK_CLASS_MAP

SPEAKER_FMT = struct_helper.make_key_fmt_for_class('speaker')  # '>B8s'
AUDIO_FMT   = struct_helper.make_key_fmt_for_class('audio')    # '>B8sB'
SEGMENT_FMT = struct_helper.make_key_fmt_for_class('segment')  # '>B8sBI8s'
TIME_SCAN_FMT = struct_helper.make_key_fmt_for_time_scan()     # '>B8sBI'

SPEAKER_LEN = struct.calcsize(SPEAKER_FMT)  # 9
AUDIO_LEN   = struct.calcsize(AUDIO_FMT)    # 10
SEGMENT_LEN = struct.calcsize(SEGMENT_FMT)  # 22
TIME_SCAN_LEN = struct.calcsize(TIME_SCAN_FMT)  # 17



# -------- pack --------
def instance_to_key(obj):
    uuid = obj.identifier
    if obj.object_type == 'Audio': return pack_audio_key(uuid)
    if obj.object_type == 'Speaker': return pack_speaker_key(uuid)
    ranks = CLASS_RANK_MAP[obj.object_type]
    offset = int(round(obj.start * 1000))
    audio_uuid = obj.audio_id
    return pack_segment_key(audio_uuid, rank, offset, uuid)

def instance_to_child_time_scan_keys(instance, child_class):
    start_ms = int(round(instance.start * 1000))
    start_key = make_time_scan_prefix(instance.audio_id, child_class, start_ms)
    end_ms = int(round(instance.end * 1000))
    end_key = make_time_scan_prefix(instance.audio_id, child_class, end_ms)
    return start_key, end_key

def make_time_scan_prefix(audio_uuid_hex, child_class, time_ms):
    '''Make key prefix for time scan of segments in an audio.
    audio_uuid_hex: hex string of audio UUID
    child_class: Word, Syllable, Phone
    time_ms: integer offset in milliseconds
    '''
    audio_uuid = _hex_to_8_bytes(audio_uuid_hex)
    child_class_rank = CLASS_RANK_MAP[child_class]
    audio_rank = CLASS_RANK_MAP['Audio']
    return struct.pack(TIME_SCAN_FMT, audio_rank, 
         audio_uuid, child_class_rank, time_ms)


def pack_speaker_key(speaker_uuid_hex):
    speaker_uuid = _hex_to_8_bytes(speaker_uuid_hex)
    speaker_rank = CLASS_RANK_MAP['Speaker']
    return struct.pack(SPEAKER_FMT, speaker_rank, speaker_uuid)


def pack_audio_key(audio_uuid_hex):
    audio_uuid = _hex_to_8_bytes(audio_uuid_hex)
    rank = CLASS_RANK_MAP['Audio']
    return struct.pack(AUDIO_FMT, rank, audio_uuid, rank)


def pack_segment_key(audio_uuid_hex, class_rank, offset_ms, segment_uuid_hex):
    if not (0 <= offset_ms <= 0xFFFFFFFF):
        raise ValueError('offset_ms must fit in uint32')

    audio_uuid = _hex_to_8_bytes(audio_uuid_hex)
    segment_uuid = _hex_to_8_bytes(segment_uuid_hex)
    audio_rank = CLASS_RANK_MAP['Audio']

    return struct.pack(SEGMENT_FMT, struct_helper.AUDIO_RANK, audio_uuid, 
        class_rank, offset_ms, segment_uuid)
        


# -------- unpack --------
def unpack_prefix(prefix_bytes):
    return struct.unpack(prefix_bytes)

def unpack_key(key_bytes):
    '''Dispatch by length and (for speaker) first byte != AUDIO_RANK.'''
    AUDIO_RANK= CLASS_RANK_MAP['Audio']
    SPEAKER_RANK= CLASS_RANK_MAP['Speaker']
    if not key_bytes:
        raise ValueError('Empty key')

    n = len(key_bytes)

    if n == AUDIO_LEN:
        audio_rank, audio_uuid, rank = struct.unpack(AUDIO_FMT, key_bytes)
        if not (audio_rank == AUDIO_RANK == rank):
            raise ValueError('Invalid audio key')
        return {
            'object_type': 'Audio',
            'audio_uuid': audio_uuid.hex(),
        }

    if n == SEGMENT_LEN:
        o =  struct.unpack(SEGMENT_FMT, key_bytes)
        audio_rank, audio_uuid, class_rank, start_ms, segment_uuid = o
        if audio_rank != AUDIO_RANK:
            raise ValueError('Invalid segment key (audio_rank must be 0)')
        return {
            'object_type': struct_helper.class_rank_map[class_rank],
            'audio_uuid': audio_uuid.hex(),
            'start_ms': start_ms,
            'segment_uuid': segment_uuid.hex(),
        }

    if n == SPEAKER_LEN:
        speaker_rank, speaker_uuid = struct.unpack(SPEAKER_FMT, key_bytes)
        if speaker_rank != SPEAKER_RANK:
            m = f'Invalid speaker key (rank must be {struct_helper.SPEAKER_RANK})'
            raise ValueError(m)
        return {
            'object_type': 'Speaker',
            'speaker_uuid': speaker_uuid.hex(),
        }

    if n == TIME_SCAN_LEN:
        o = struct.unpack(TIME_SCAN_FMT, key_bytes)
        audio_rank, audio_uuid, child_class_rank, start_ms = o
        object_type = struct_helper.class_rank_map.get(child_class_rank)
        if audio_rank != AUDIO_RANK:
            raise ValueError('Invalid time scan key (audio_rank must be 0)')
        return {
            'child_object_type': object_type, 
            'audio_uuid': audio_uuid.hex(),
            'start_ms': start_ms,
        }

    raise ValueError(f'Unknown key length: {n}')


def _hex_to_8_bytes(hex_str):
    '''Convert 16-hex-char string to 8 raw bytes.'''
    if len(hex_str) != 16:
        raise ValueError(f'Expected 16 hex chars, got {len(hex_str)}')
    return bytes.fromhex(hex_str)

