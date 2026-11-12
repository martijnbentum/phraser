'''Read and update independent states in the existing segment flags byte.'''

DURATION_OUTLIER_MASK = 0b11
DURATION_OUTLIER_STATES = ('unset', 'short', 'long', 'within_range')
TIMING_UNCERTAIN_MASK = 0b00000100
SHARED_PHONE_MASK = 0b00001000
MISSING_SHARED_PHONE_MASK = 0b00010000
INSERTED_PHONE_MASK = 0b00100000


def _set_boolean_flag(segment, mask, value):
    '''Update one boolean flag without changing other bits or saving.'''
    if type(value) is not bool:
        raise ValueError(f'Flag value must be a bool: {value!r}')
    if value: segment.flags |= mask
    else: segment.flags &= ~mask


def get_timing_uncertain(segment):
    '''Whether a concern about this segment's timing has been recorded.'''
    return bool(segment.flags & TIMING_UNCERTAIN_MASK)


def set_timing_uncertain(segment, value):
    '''Record or clear a timing concern on this segment alone.'''
    _set_boolean_flag(segment, TIMING_UNCERTAIN_MASK, value)


def get_shared_phone(segment):
    '''Whether this phone is recorded as shared across words.'''
    return bool(segment.flags & SHARED_PHONE_MASK)


def set_shared_phone(segment, value):
    '''Record or clear phone sharing without changing parent links.'''
    _set_boolean_flag(segment, SHARED_PHONE_MASK, value)


def get_missing_shared_phone(segment):
    '''Whether a word or syllable is missing a linked shared phone.'''
    return bool(segment.flags & MISSING_SHARED_PHONE_MASK)


def set_missing_shared_phone(segment, value):
    '''Record or clear a missing shared-phone link on this object alone.'''
    _set_boolean_flag(segment, MISSING_SHARED_PHONE_MASK, value)


def get_inserted_phone(segment):
    '''Whether this phone is recorded as an inserted linking sound.'''
    return bool(segment.flags & INSERTED_PHONE_MASK)


def set_inserted_phone(segment, value):
    '''Record or clear insertion without changing other flags or links.'''
    _set_boolean_flag(segment, INSERTED_PHONE_MASK, value)


def get_duration_outlier_code(segment):
    '''Read the duration assessment from the lowest two bits.'''
    return segment.flags & DURATION_OUTLIER_MASK


def set_duration_outlier_code(segment, code):
    '''Set the duration assessment while preserving unrelated flag bits.'''
    valid = type(code) is int and code in range(4)
    if not valid:
        raise ValueError(f'Invalid duration outlier code: {code!r}')
    segment.flags = (segment.flags & ~DURATION_OUTLIER_MASK) | code


def get_duration_outlier(segment):
    '''Return unset, short, long, or within_range for this segment itself.'''
    return DURATION_OUTLIER_STATES[get_duration_outlier_code(segment)]


def set_duration_outlier(segment, state):
    '''Set a named assessment without saving the segment.'''
    if state not in DURATION_OUTLIER_STATES:
        raise ValueError(f'Invalid duration outlier state: {state!r}')
    set_duration_outlier_code(segment, DURATION_OUTLIER_STATES.index(state))


def is_duration_outlier(segment):
    '''Whether this segment was assessed as unusually short or long.'''
    return get_duration_outlier_code(segment) in (1, 2)
