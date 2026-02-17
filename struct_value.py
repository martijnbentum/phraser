import struct 
from struct_helper import hex_to_8_bytes

VERSION = 1

# ---- class-specific wrappers ----

def pack_instance(instance):
    f = globals().get(f'pack_{instance.object_type.lower()}')
    if f is None:
        raise ValueError(f'Unsupported object type: {instance.object_type}')
    return f(instance)

def unpack_instance(object_type, value_bytes):
    f = globals().get(f'unpack_{object_type.lower()}')
    if f is None:
        raise ValueError(f'Unsupported object type: {object_type}')
    return f(value_bytes)

def pack_audio(instance):
    '''Pack Audio value bytes from dict.
    layout: layout dict for audio
    instance: Audio instance with fields matching audio layout
    '''

    layout = LAYOUTS['audio']
    flags = 1 if instance.has_extra() else 0
    fixed = {
        'version': VERSION,
        'flags': flags,
        'n_channels': instance.n_channels,
        'duration_ms': instance.duration_ms,
        'sample_rate': instance.sample_rate,
    }
    var = {
        'filename': instance.filename,
        'dialect': instance.dialect,
        'language': instance.language,
        'dataset': instance.dataset,
    }
    return _pack_with_layout(layout, fixed, var, 'audio')


def unpack_audio(value_bytes):
    '''Unpack Audio value bytes to dict.
    layout: layout dict for audio
    value_bytes: bytes stored in LMDB
    '''
    layout = LAYOUTS['audio']
    return _unpack_with_layout(layout, value_bytes, 'audio')

def pack_phrase(instance):
    '''Pack Phrase value bytes from dict.
    layout: layout dict for phrase
    instance: Phrase instance with fields matching phrase layout
    '''
    layout = LAYOUTS['phrase']
    flags = 1 if instance.has_extra() else 0
    fixed = {
        'version': VERSION,
        'flags': flags,
        'end_ms': instance.end_ms,
        'speaker_id': hex_to_8_bytes(instance.speaker_id),
    }
    var = {'label': instance.label}
    return _pack_with_layout(layout, fixed, var, 'phrase')


def unpack_phrase(value_bytes):
    '''Unpack Phrase value bytes to dict.
    layout: layout dict for phrase
    value_bytes: bytes stored in LMDB
    '''
    layout = LAYOUTS['phrase']
    return _unpack_with_layout(layout, value_bytes, 'phrase')


def pack_word(instance):
    '''Pack Word value bytes from dict.
    layout: layout dict for word
    instance: Word instance with fields matching word layout
    '''
    layout = LAYOUTS['word']
    flags = 1 if instance.has_extra() else 0
    fixed = {
        'version': VERSION,
        'flags': flags,
        'end_ms': instance.end_ms,
        'speaker_id': hex_to_8_bytes(instance.speaker_id),
        'parent_id': hex_to_8_bytes(instance.parent_id),
        'parent_start_ms': instance.parent_start_ms,
    }
    var = {
        'label': instance.label,
        'ipa': instance.ipa,
    }
    return _pack_with_layout(layout, fixed, var, 'word')


def unpack_word(value_bytes):
    '''Unpack Word value bytes to dict.
    layout: layout dict for word
    value_bytes: bytes stored in LMDB
    '''
    layout = LAYOUTS['word']
    return _unpack_with_layout(layout, value_bytes, 'word')


def pack_syllable(instance):
    '''Pack Syllable value bytes from dict.
    layout: layout dict for syllable
    instance: Syllable instance with fields matching syllable layout
    '''
    layout = LAYOUTS['syllable']
    flags = 1 if instance.has_extra() else 0
    fixed = {
        'version': VERSION,
        'flags': flags,
        'stress_code': instance.stress_code,
        'end_ms': instance.end_ms,
        'speaker_id': hex_to_8_bytes(instance.speaker_id),
        'parent_id': hex_to_8_bytes(instance.parent_id),
        'parent_start_ms': instance.parent_start_ms,
        'phrase_id': hex_to_8_bytes(instance.phrase_id),
        'phrase_start_ms': instance.phrase_start_ms,
    }
    var = {'label': instance.label}
    return _pack_with_layout(layout, fixed, var, 'syllable')


def unpack_syllable(value_bytes):
    '''Unpack Syllable value bytes to dict.
    layout: layout dict for syllable
    value_bytes: bytes stored in LMDB
    '''
    layout = LAYOUTS['syllable']
    return _unpack_with_layout(layout, value_bytes, 'syllable')


def pack_phone(instance):
    '''Pack Phone value bytes from dict.
    layout: layout dict for phone
    instance: Phone instance with fields matching phone layout
    '''
    layout = LAYOUTS['phone']
    flags = 1 if instance.has_extra() else 0
    fixed = {
        'version': VERSION,
        'flags': flags,
        'position_code': instance.position_code,
        'end_ms': instance.end_ms,
        'speaker_id': hex_to_8_bytes(instance.speaker_id),
        'parent_id': hex_to_8_bytes(instance.parent_id),
        'parent_start_ms': instance.parent_start_ms,
        'phrase_id': hex_to_8_bytes(instance.phrase_id),
        'phrase_start_ms': instance.phrase_start_ms,
    }
    var = {'label': instance.label}
    return _pack_with_layout(layout, fixed, var, 'phone')


def unpack_phone(value_bytes):
    '''Unpack Phone value bytes to dict.
    layout: layout dict for phone
    value_bytes: bytes stored in LMDB
    '''
    layout = LAYOUTS['phone']
    return _unpack_with_layout(layout, value_bytes, 'phone')


def pack_speaker(instance):
    '''Pack Speaker value bytes from dict.
    layout: layout dict for speaker
    instance: Speaker instance with fields matching speaker layout
    '''
    layout = LAYOUTS['speaker']
    flags = 1 if instance.has_extra() else 0
    fixed = {
        'version': VERSION,
        'flags': flags,
        'gender_code': instance.gender_code,
        'age': instance.age,
    }
    var = {
        'name': instance.name,
        'dataset': instance.dataset,
        'dialect': instance.dialect,
        'region': instance.region,
        'language': instance.language,
    }
    return _pack_with_layout(layout, fixed, var, 'speaker')


def unpack_speaker(value_bytes):
    '''Unpack Speaker value bytes to dict.
    layout: layout dict for speaker
    value_bytes: bytes stored in LMDB
    '''
    layout = LAYOUTS['speaker']
    return _unpack_with_layout(layout, value_bytes, 'speaker')

# --- internal helper functions for packing/unpacking with layouts ---

def _pack_str(text, bits):
    '''Pack a UTF-8 string with u8/u16 length prefix.
    text: string (None becomes '')
    bits: length prefix size (8 or 16)
    '''
    if text is None:
        text = ''
    b = text.encode('utf-8')

    if bits == 8:
        if len(b) > 0xFF:
            raise ValueError('string too long for u8 length prefix')
        return struct.pack('>B', len(b)) + b

    if bits == 16:
        if len(b) > 0xFFFF:
            raise ValueError('string too long for u16 length prefix')
        return struct.pack('>H', len(b)) + b

    raise ValueError('bits must be 8 or 16')


def _unpack_str(buf, pos, bits):
    '''Unpack a UTF-8 string with u8/u16 length prefix.
    buf: bytes
    pos: current offset in buf
    bits: length prefix size (8 or 16)
    '''
    if bits == 8:
        if pos + 1 > len(buf):
            raise ValueError('truncated u8 length prefix')
        n = buf[pos]
        pos += 1

    elif bits == 16:
        if pos + 2 > len(buf):
            raise ValueError('truncated u16 length prefix')
        (n,) = struct.unpack('>H', buf[pos:pos + 2])
        pos += 2

    else:
        raise ValueError('bits must be 8 or 16')

    end = pos + n
    if end > len(buf):
        raise ValueError('truncated string payload')

    return buf[pos:end].decode('utf-8'), end


def _parse_var_fields(fields, obj):
    '''Convert layout var field tokens to (name, bits) list.
    fields: list of tokens like 'u16str:label'
    obj: class name for errors
    '''
    out = []
    for token in fields:
        if token.startswith('u8str:'):
            out.append((token.split(':', 1)[1], 8))
        elif token.startswith('u16str:'):
            out.append((token.split(':', 1)[1], 16))
        else:
            raise ValueError(f'{obj}: unsupported token {token}')
    return out


def _pack_with_layout(layout, fixed, var, obj_name):
    '''Pack bytes for one record according to layout dict.
    layout: dict with fixed_fmt, fixed_fields, fields
    fixed: dict of fixed field values in fixed_fields order
    var: dict of variable string values
    obj_name: string used in error messages
    '''
    fixed_fmt = layout['fixed_fmt']
    fixed_fields = layout['fixed_fields']
    var_fields = _parse_var_fields(layout['fields'], obj_name)

    values = []
    for name in fixed_fields:
        if name not in fixed:
            raise KeyError(f'{obj_name}: missing fixed field {name}')
        values.append(fixed[name])

    out = struct.pack(fixed_fmt, *values)

    for name, bits in var_fields:
        out += _pack_str(var.get(name, ''), bits)

    return out


def _unpack_with_layout(layout, value_bytes, obj_name):
    '''Unpack bytes for one record according to layout dict.
    layout: dict with fixed_fmt, fixed_fields, fields
    value_bytes: bytes stored in LMDB
    obj_name: string used in error messages
    '''
    fixed_fmt = layout['fixed_fmt']
    fixed_fields = layout['fixed_fields']
    var_fields = _parse_var_fields(layout['fields'], obj_name)

    fixed_len = struct.calcsize(fixed_fmt)
    if len(value_bytes) < fixed_len:
        raise ValueError(f'{obj_name}: value too short for fixed header')

    fixed_vals = struct.unpack(fixed_fmt, value_bytes[:fixed_len])
    out = dict(zip(fixed_fields, fixed_vals))

    pos = fixed_len
    for name, bits in var_fields:
        out[name], pos = _unpack_str(value_bytes, pos, bits)

    if pos != len(value_bytes):
        raise ValueError(f'{obj_name}: trailing bytes not described by layout')

    return out



# --- layout definitions for each class ---

def speaker_layout():
    fixed_specs = []
    for name in 'version flags gender_code age'.split():
        fixed_specs.append({'name': name, 'kind': 'int', 'bits': 8})
    variable_specs = []
    for name in 'name dataset dialect region language'.split():
        variable_specs.append({'name': name, 'kind': 'str', 'bits': 16})
    return build_layout(fixed_specs=fixed_specs, variable_specs=variable_specs)

def audio_layout():
    fixed_specs = []
    for name in 'version flags n_channels'.split():
        fixed_specs.append({'name': name, 'kind': 'int', 'bits': 8})
    for name in 'duration_ms sample_rate'.split():
        fixed_specs.append({'name': name, 'kind': 'int', 'bits': 32})
    variable_specs = []
    for name in 'filename dialect language dataset'.split():
        variable_specs.append({'name': name, 'kind': 'str', 'bits': 16})
    return build_layout(fixed_specs=fixed_specs, variable_specs=variable_specs)

def phrase_layout():
    fixed_specs = []
    for name in 'version flags'.split():
        fixed_specs.append({'name': name, 'kind': 'int', 'bits': 8})
    fixed_specs.append({'name': 'end_ms', 'kind': 'int', 'bits': 32})
    fixed_specs.append({'name': 'speaker_id', 'kind': 'bytes', 'n_bytes': 8})
    variable_specs = []
    variable_specs.append({'name': 'label', 'kind': 'str', 'bits': 16})
    return build_layout(fixed_specs=fixed_specs, variable_specs=variable_specs)

def word_layout():
    fixed_specs = []
    for name in 'version flags'.split():
        fixed_specs.append({'name': name, 'kind': 'int', 'bits': 8})
    fixed_specs.append({'name': 'end_ms', 'kind': 'int', 'bits': 32})
    fixed_specs.append({'name': 'parent_start_ms', 'kind': 'int', 'bits': 32})
    for name in 'speaker_id parent_id'.split():
        fixed_specs.append({'name': name, 'kind': 'bytes', 'n_bytes': 8})
    variable_specs = []
    for name in 'label ipa_label'.split():
        variable_specs.append({'name': name, 'kind': 'str', 'bits': 16})
    return build_layout(fixed_specs=fixed_specs, variable_specs=variable_specs)

def syllable_layout():
    fixed_specs = []
    for name in 'version flags stress_code'.split():
        fixed_specs.append({'name': name, 'kind': 'int', 'bits': 8})
    fixed_specs.append({'name': 'end_ms', 'kind': 'int', 'bits': 32})
    fixed_specs.append({'name': 'parent_start_ms', 'kind': 'int', 'bits': 32})
    fixed_specs.append({'name': 'phrase_start_ms', 'kind': 'int', 'bits': 32})
    for name in 'speaker_id parent_id phrase_id'.split():
        fixed_specs.append({'name': name, 'kind': 'bytes', 'n_bytes': 8})
    variable_specs = []
    variable_specs.append({'name': 'label', 'kind': 'str', 'bits': 16})
    return build_layout(fixed_specs=fixed_specs, variable_specs=variable_specs)

def phone_layout():
    fixed_specs = []
    for name in 'version flags position_code'.split():
        fixed_specs.append({'name': name, 'kind': 'int', 'bits': 8})
    fixed_specs.append({'name': 'end_ms', 'kind': 'int', 'bits': 32})
    fixed_specs.append({'name': 'parent_start_ms', 'kind': 'int', 'bits': 32})
    fixed_specs.append({'name': 'phrase_start_ms', 'kind': 'int', 'bits': 32})
    for name in 'speaker_id parent_id phrase_id'.split():
        fixed_specs.append({'name': name, 'kind': 'bytes', 'n_bytes': 8})
    variable_specs = []
    variable_specs.append({'name': 'label', 'kind': 'str', 'bits': 8})
    return build_layout(fixed_specs=fixed_specs, variable_specs=variable_specs)


def field_token(name, kind, bits=16, variable=False, signed=False, n_bytes=8):
    '''Build a fixed fmt token or a variable field token for a field.
    name: field name (e.g. 'age_years', 'label', 'speaker_uuid')
    kind: 'int', 'str', or 'bytes'
    bits: integer size in bits for int/str length (8, 16, 32, 64)
    variable: if True, return a variable token like 'u16str:label'
    signed: if True, use signed int for fixed ints (ignored for variable tokens)
    n_bytes: size for fixed bytes fields (e.g. 8 for UUID)
    '''
    if variable:
        if kind == 'str':
            if bits not in (8, 16, 32):
                raise ValueError('str bits must be 8, 16, or 32')
            return f'u{bits}str:{name}'
        if kind == 'bytes':
            if bits not in (8, 16, 32):
                raise ValueError('bytes bits must be 8, 16, or 32')
            return f'u{bits}bytes:{name}'
        raise ValueError('variable tokens support kind=str or kind=bytes')

    if kind == 'int':
        if bits == 8:
            return 'b' if signed else 'B'
        if bits == 16:
            return 'h' if signed else 'H'
        if bits == 32:
            return 'i' if signed else 'I'
        if bits == 64:
            return 'q' if signed else 'Q'
        raise ValueError('int bits must be 8, 16, 32, or 64')

    if kind == 'bytes':
        if n_bytes <= 0:
            raise ValueError('n_bytes must be > 0')
        return f'{n_bytes}s'

    raise ValueError('kind must be int, str, or bytes')

def build_layout(byte_order='>', fixed_specs=None, variable_specs=None):
    '''Build a layout dict from fixed and variable field specs.
    byte_order: endianness prefix for struct fmt (e.g. '>')
    fixed_specs: list of field spec dicts for fixed header
    variable_specs: list of field spec dicts for variable fields
    '''
    if fixed_specs is None:
        fixed_specs = []
    if variable_specs is None:
        variable_specs = []

    fixed_tokens = [byte_order]
    fixed_fields = []
    for spec in fixed_specs:
        name = spec['name']
        kind = spec['kind']
        bits = spec.get('bits', 16)
        signed = spec.get('signed', False)
        n_bytes = spec.get('n_bytes', 8)
        token = field_token(name, kind, bits=bits, variable=False,
                            signed=signed, n_bytes=n_bytes)
        fixed_tokens.append(token)
        fixed_fields.append(name)

    fields = []
    for spec in variable_specs:
        name = spec['name']
        kind = spec['kind']
        bits = spec.get('bits', 16)
        token = field_token(name, kind, bits=bits, variable=True)
        fields.append(token)

    return {
        'fixed_fmt': ''.join(fixed_tokens),
        'fixed_fields': fixed_fields,
        'fields': fields,
    }



LAYOUTS = {
    'audio': audio_layout(),
    'speaker': speaker_layout(),
    'phrase': phrase_layout(),
    'word': word_layout(),
    'syllable': syllable_layout(),
    'phone': phone_layout(),
}
    

