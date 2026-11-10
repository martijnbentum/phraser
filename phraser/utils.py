R = "\033[91m"
G = "\033[92m"
B = "\033[94m"
GR = "\033[90m"
P = "\033[35m"
RE = "\033[0m"
object_type_to_ljust_label={'Phrase':40, 'Word':15, 'Syllable':12, 'Phone':3}

def pretty_print_object_dict(obj_dict):
    '''pretty print dict with red keys and aligned outline.
    returns a single formatted string.
    '''
    lines = []
    width = max(len(str(k)) for k in obj_dict)

    for key, value in obj_dict.items():
        if key.startswith('_'): continue
        key_pad = ' ' * (width - len(str(key)))
        prefix = f'{R}{key}{RE}{key_pad} : '

        if isinstance(value, dict):
            if not value:
                lines.append(prefix + '{}')
                continue
            lines.append(prefix.rstrip())
            sub_width = max(len(str(k)) for k in value)
            for subkey, subval in value.items():
                sub_pad = ' ' * (sub_width - len(str(subkey)))
                lines.append(f'  {R}{subkey}{RE}{sub_pad} : {subval}')
        if isinstance(value, list):
            if not value:
                lines.append(prefix + '[]')
                continue
            lines.append(prefix.rstrip())
            for item in value:
                lines.append(f'  - {item}')
        else:
            lines.append(prefix + value.__repr__())

    return '\n'.join(lines)

def reverse_dict(d):
    return {v: k for k, v in d.items()}

def make_gender_dict(reverse=False):
    d = {name:code for name,code in zip(['unknown','female','male'], [9,1,2])}
    if reverse: return reverse_dict(d)
    return d

def make_overlap_dict(reverse=False):
    d = {n:c for n,c in zip([None,False,True],[9,0,1])}
    if reverse: return reverse_dict(d)
    return d

gender_dict = make_gender_dict()
reverse_gender_dict = make_gender_dict(reverse=True)

def make_stress_dict(reverse=False):
    d = {'unstressed': 0, 'primary': 1, 'secondary': 2, 'unknown': 9}
    if reverse: return reverse_dict(d)
    return d

def make_position_dict(reverse=False):
    d = {'onset': 1, 'nucleus': 2, 'coda': 3, 'unknown': 9}
    if reverse: return reverse_dict(d)
    return d

overlap_dict = make_overlap_dict()
reverse_overlap_dict = make_overlap_dict(reverse=True)

stress_dict = make_stress_dict()
reverse_stress_dict = make_stress_dict(reverse=True)

position_dict = make_position_dict()
reverse_position_dict = make_position_dict(reverse=True)


def seconds_to_miliseconds(seconds):
    return int(seconds * 1000)

def miliseconds_to_seconds(milliseconds):
    return milliseconds / 1000

def middle_point(start, end):
    '''Return the middle point of an interval defined by start and end.'''
    return (start + end) / 2

def overlap(object1, object2):
    '''Return True if intervals overlap (partial or full), else False.
    Assumes integer start and end, and start < end.
    '''
    return object1.start < object2.end and object2.start < object1.end

def check_init_fields(obj, kwargs, allowed):
    '''Raise if kwargs holds a field that is not stored in the database.'''
    unknown = set(kwargs) - set(allowed)
    if not unknown: return
    m = f'{obj.__class__.__name__} got unsupported fields: {sorted(unknown)}. '
    m += f'Allowed: {sorted(allowed)}'
    raise ValueError(m)
