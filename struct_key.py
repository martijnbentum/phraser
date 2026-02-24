import struct
import struct_helper
from struct_helper import CLASS_RANK_MAP, RANK_CLASS_MAP

SPEAKER_FMT = struct_helper.make_key_fmt_for_class('speaker')  # '>B8s'
AUDIO_FMT   = struct_helper.make_key_fmt_for_class('audio')    # '>B8sB'
SEGMENT_FMT = struct_helper.make_key_fmt_for_class('segment')  # '>B8sBI8s'
TIME_SCAN_FMT = struct_helper.make_key_fmt_for_time_scan()     # '>B8sBI'
SPEAKER_AUDIO_FMT = struct_helper.make_key_fmt_for_class('speaker_audio')#'>8s8s'


SPEAKER_LEN = struct.calcsize(SPEAKER_FMT)  # 9
AUDIO_LEN   = struct.calcsize(AUDIO_FMT)    # 10
SPEAKER_AUDIO_LEN = struct.calcsize(SPEAKER_AUDIO_FMT)  # 16
SEGMENT_LEN = struct.calcsize(SEGMENT_FMT)  # 22
TIME_SCAN_LEN = struct.calcsize(TIME_SCAN_FMT)  # 17



# -------- pack --------
def instance_to_key(obj):
    uuid = obj.identifier
    if obj.object_type == 'Audio': return pack_audio_key(uuid)
    if obj.object_type == 'Speaker': return pack_speaker_key(uuid)
    rank = CLASS_RANK_MAP[obj.object_type]
    offset = obj.start 
    audio_uuid = obj.audio_id
    return pack_segment_key(audio_uuid, rank, offset, uuid)


def instance_to_child_time_scan_keys(instance, child_class = None):
    if child_class is None: child_class = instance.child_class_name
    start = instance.start
    start_key = make_time_scan_prefix(instance.audio_id, child_class, start)
    end = instance.end
    end_key = make_time_scan_prefix(instance.audio_id, child_class, end)
    return start_key, end_key

def make_time_scan_prefix(audio_uuid, child_class, time):
    '''Make key prefix for time scan of segments in an audio.
    audio_uuid: audio UUID
    child_class: Word, Syllable, Phone
    time: integer offset in milliseconds
    '''
    child_class_rank = CLASS_RANK_MAP[child_class]
    audio_rank = CLASS_RANK_MAP['Audio']
    return struct.pack(TIME_SCAN_FMT, audio_rank, 
         audio_uuid, child_class_rank, time)

def make_speaker_scan_prefix(speaker_uuid):
    '''Make key prefix for scan of speaker-audio pairs for a speaker.
    speaker_uuid: speaker UUID
    '''
    return struct.pack('>8s', speaker_uuid)


def pack_speaker_key(speaker_uuid):
    speaker_rank = CLASS_RANK_MAP['Speaker']
    return struct.pack(SPEAKER_FMT, speaker_rank, speaker_uuid)

def pack_speaker_audio_key(speaker_uuid, audio_uuid):
    return struct.pack(SPEAKER_AUDIO_FMT, speaker_uuid, audio_uuid)


def pack_audio_key(audio_uuid):
    rank = CLASS_RANK_MAP['Audio']
    return struct.pack(AUDIO_FMT, rank, audio_uuid, rank)

def pack_audio_prefix(audio_uuid, child_class):
    child_class_rank = CLASS_RANK_MAP[child_class]
    audio_rank = CLASS_RANK_MAP['Audio']
    return struct.pack(AUDIO_FMT, audio_rank, audio_uuid, child_class_rank)


def pack_segment_key(audio_uuid, class_rank, offset, segment_uuid):
    if not (0 <= offset <= 0xFFFFFFFF):
        raise ValueError('offset must fit in uint32')

    audio_rank = CLASS_RANK_MAP['Audio']

    return struct.pack(SEGMENT_FMT, audio_rank, audio_uuid, 
        class_rank, offset, segment_uuid)
        


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
            'identifier': audio_uuid,
        }

    if n == SEGMENT_LEN:
        o =  struct.unpack(SEGMENT_FMT, key_bytes)
        audio_rank, audio_uuid, class_rank, start, segment_uuid = o
        if audio_rank != AUDIO_RANK:
            raise ValueError('Invalid segment key (audio_rank must be 0)')
        return {
            'object_type': RANK_CLASS_MAP[class_rank],
            'audio_id': audio_uuid,
            'start': start,
            'identifier': segment_uuid,
        }

    if n == SPEAKER_LEN:
        speaker_rank, speaker_uuid = struct.unpack(SPEAKER_FMT, key_bytes)
        if speaker_rank != SPEAKER_RANK:
            m = f'Invalid speaker key (rank must be {struct_helper.SPEAKER_RANK})'
            raise ValueError(m)
        return {
            'object_type': 'Speaker',
            'identifier': speaker_uuid,
        }

    if n == SPEAKER_AUDIO_LEN:
        speaker_uuid, audio_uuid = struct.unpack(SPEAKER_AUDIO_FMT, key_bytes)
        return {
            'speaker_id': speaker_uuid,
            'audio_id': audio_uuid,
        }

    if n == TIME_SCAN_LEN:
        o = struct.unpack(TIME_SCAN_FMT, key_bytes)
        audio_rank, audio_uuid, child_class_rank, start = o
        object_type = RANK_CLASS_MAP[child_class_rank]
        if audio_rank != AUDIO_RANK:
            raise ValueError('Invalid time scan key (audio_rank must be 0)')
        return {
            'child_object_type': object_type, 
            'start': start,
            'audio_id': audio_uuid,
        }

    raise ValueError(f'Unknown key length: {n}')



