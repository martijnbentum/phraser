import operator
import re

def op_exact(a, b):
    '''checks if strings are exactly equal'''
    return isinstance(a, str) and a == b

def op_startswith(value, prefix):
    '''checks if value starts with prefix'''
    return isinstance(value, str) and value.startswith(prefix)

def op_endswith(value, suffix):
    '''checks if value ends with suffix'''
    return isinstance(value, str) and value.endswith(suffix)

def op_contains(value, substring):
    '''checks if substring is in value'''
    return isinstance(value, str) and substring in value

def op_icontains(value, substring):
    '''checks if substring is in value (case-insensitive)'''
    return isinstance(value, str) and substring.lower() in value.lower()

def op_iexact(a, b):
    '''checks if strings are equal (case-insensitive)'''
    return isinstance(a, str) and a.lower() == b.lower()

def op_istartswith(a, b):
    '''checks if string starts with prefix (case-insensitive)'''
    return isinstance(a, str) and a.lower().startswith(b.lower())

def op_iendswith(a, b):
    return isinstance(a, str) and a.lower().endswith(b.lower())

def op_iregex(a, b):
    return isinstance(a, str) \
        and re.search(b, a, flags=re.IGNORECASE) is not None

def op_regex(a, b):
    return isinstance(a, str) and re.search(b, a) is not None

def op_range(a, rng):
    low, high = rng
    return low <= a <= high

def op_len_eq(a, n):
    return len(a) == n

def op_len_gt(a, n):
    return len(a) > n

def op_len_lt(a, n):
    return len(a) < n

OPS = {
    # numeric
    'eq': operator.eq,
    'gt': operator.gt,
    'gte': operator.ge,
    'lt': operator.lt,
    'lte': operator.le,
    'range': op_range,

    # membership / sequence
    'in': lambda a, b: a in b,
    'contains': op_contains,
    'icontains': op_icontains,

    # string exact / case-insensitive
    'startswith': op_startswith,
    'istartswith': op_istartswith,
    'endswith': op_endswith,
    'iendswith': op_iendswith,
    'iexact': op_iexact,

    # regex
    'regex': op_regex,
    'iregex': op_iregex,

    # length-based operators
    'len': len,               # returns value → use with nested lookup
    'len_eq': op_len_eq,
    'len_gt': op_len_gt,
    'len_lt': op_len_lt,
}
