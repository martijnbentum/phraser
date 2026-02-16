R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"

def pretty_print_object_dict(obj_dict):
    '''pretty print dict with red keys and aligned outline.
    returns a single formatted string.
    '''
    lines = []
    width = max(len(str(k)) for k in obj_dict)

    for key, value in obj_dict.items():
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
        elif isinstance(value, list):
            if not value:
                lines.append(prefix + '[]')
                continue
            lines.append(prefix.rstrip())
            for item in value:
                lines.append(f'  - {item}')
        else:
            lines.append(prefix + str(value))

    return '\n'.join(lines)

def reverse_dict(d):
    return {v: k for k, v in d.items()}

def make_gender_dict(reverse=False):
    d = {name:code for name,code in zip(['unknown','female','male'], range(3))}
    if reverse: return reverse_dict(d)
    return d

gender_dict = make_gender_dict()
reverse_gender_dict = make_gender_dict(reverse=True)
