'''Read and update assessment states in the existing segment flags byte.'''

DURATION_OUTLIER_MASK = 0b11
DURATION_OUTLIER_STATES = ('unset', 'short', 'long', 'within_range')


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
