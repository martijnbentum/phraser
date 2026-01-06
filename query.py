from comparison import OPS
import lmdb_key

R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"

def queryset_from_items(items, cache = None):
    """
    Create a QuerySet for items.

    Example:
        qs = queryset_from_items(cache, some_words)
        qs.filter(label="t").order_by("start")
    """
    # Materialize once (supports generators)
    items = list(items)
    types = set(type(x) for x in items)
    if len(types) == 0: 
        raise ValueError("Cannot create QuerySet from empty items.")
    if len(types) > 1:
        raise TypeError(
            f"All items must be of the same type; got: "
            f"{', '.join(t.__name__ for t in types)}"
        )
    cls = types.pop()
    if cache is None:
        cache = cls.get_default_cache()

    # Empty input -> empty queryset
    data = Data(cls, cache)

    # Convert to LMDB keys and restrict
    keys = objects_to_keys(items)

    # Optional: keep only keys that actually exist for this class in the cache
    data.keys = keys

    return QuerySet(data)


def get_class_object(cls, cache):
    '''sets the objects attribute on each class (e.g. Audio.objects)'''
    data = Data(cls, cache)
    return QuerySet(data)

class Data:
    '''handles loading objects of a given class from cache'''
    def __init__(self, cls, cache):
        self.cls = cls
        self.cache = cache
        self.object_type = cls.__name__
        self._get_keys(update = False)

    def _get_keys(self, update = False):
        d = self.cache.object_type_to_keys_dict(update = update)
        m = f'No keys found for object type: {B}{self.object_type}{RE}'
        if self.object_type not in d:
            print(f'{R}WARNING:{RE} {m}')
            self.keys = []
        else:
            self.keys = d[self.object_type]

    def load(self, keys = None):
        if keys is None: keys = self.keys
        objs = self.cache.load_many(keys)
        return objs


class QuerySet:
    '''handles filtering, excluding, and ordering of objects'''
    def __init__(self, data, filters=None, ordering = None):
        self._data = data
        self._filters = filters or []
        self._ordering = ordering

    def all(self):
        '''returns a copy of the current QuerySet'''
        return QuerySet(self._data, self._filters)

    def get_one(self):
        '''returns the first object in an unfiltered QuerySet'''
        if self._filters or self._ordering:
            m = "get_n() is only supported on unfiltered QuerySets."
            raise NotImplementedError(m)
        return self._data.load([self._data.keys[0]])[0]

    def get_n(self, n):
        '''returns the first n objects in an unfiltered QuerySet'''
        if self._filters or self._ordering:
            m = "get_n() is only supported on unfiltered QuerySets."
            raise NotImplementedError(m)
        keys = self._data.keys[:n]
        return self._data.load(keys)

    def filter(self, **kwargs):
        '''adds filter conditions to the QuerySet'''
        return QuerySet(self._data, self._filters + [("filter", kwargs)])
            
    def exclude(self, **kwargs):
        '''adds exclude conditions to the QuerySet'''
        return QuerySet(self._data, self._filters + [("exclude", kwargs)])

    def order_by(self, *fields):
        '''adds ordering to the QuerySet'''
        return QuerySet(self._data, self._filters, ordering = fields)

    def _apply(self):
        '''applies filters, excludes, and ordering to the QuerySet'''
        if hasattr(self, '_objs'): return self._objs
        objs = self._data.load()
        for op, params in self._filters:
            self.check_relations_loaded(params)
            if op == "filter":
                objs = filter_objects(objs, **params)
            elif op == "exclude":
                excluded = set(filter_objects(objs, **params))
                objs = [x for x in objs if x not in excluded]
        if self._ordering:
            objs = sorted(objs, key=lambda obj: sort_key(obj, self._ordering))
        self._objs = objs
        return self._objs

    def __iter__(self):
        return iter(self._apply())

    def __len__(self):
        return len(self._apply())

    def __repr__(self):
        return queryset_summary(self)

    def check_relations_loaded(self, params):
        '''loads related objects needed for filtering/excluding
        e.g. filter(syllables__phones__label = 't')
        you need all syllables and phones loaded to apply this filter
        otherwhise you will load syllables and phones on a per word
        and syllable basis which is very slow
        '''
        print('params:', params)
        for key in params.keys():
            attr_names = key.split("__")
            for attr_name in attr_names:
                ensure_relations_loaded(self, attr_name)

    @property
    def cache(self):
        '''returns the cache associated with the QuerySet'''
        return self._data.cache



def get_attr(obj, path):
    '''Resolve nested attributes (speaker__gender → obj.speaker.gender).
    '''
    parts = path.split("__")
    current = obj
    for index, part in enumerate(parts):
        current = getattr(current, part)
        if isinstance(current, list):
            remaining = "__".join(parts[index+1:]) or None
            return current, remaining
    return current, None

def matches(obj, key, value):
    """
    Fully recursive descendant lookup resolver. 
    Any attribute that returns a list is interpreted as a relation.
        this assumption holds for now but is a weak spot
    """

    parts = key.split("__")

    # 1. If last part is a lookup operator (gt, lt, in, etc.)
    if parts[-1] in OPS:
        lookup = parts[-1]
        op = OPS[lookup]
        attr_path = "__".join(parts[:-1])
        attr_val, _ = get_attr(obj, attr_path)
        return op(attr_val, value)

    # 2. Resolve first hop of the chain
    attr = parts[0]
    rest = "__".join(parts[1:]) if len(parts) > 1 else None

    # Try to read the attribute
    attr_val = getattr(obj, attr)

    # ---------------------------------
    # CASE A: relation (list-valued)
    # ---------------------------------
    if isinstance(attr_val, list):

        # No deeper path → compare list directly
        if rest is None:
            return attr_val == value

        # Recurse into each related object
        for child in attr_val:
            if matches(child, rest, value):
                return True
        return False

    # ---------------------------------
    # CASE B: scalar attribute
    # ---------------------------------
    if rest is None:
        # Last hop: direct value compare
        return attr_val == value

    # Deeper hops → recurse
    return matches(attr_val, rest, value)


def filter_objects(objs, **filters):
    '''filters a list of objects based on key-value pairs'''
    result = []
    for obj in objs:
        ok = True
        for key, value in filters.items():
            if not matches(obj, key, value):
                ok = False
                break
        if ok:
            result.append(obj)
    return result


def objects_to_keys(objs):
    '''converts a list of objects to their corresponding LMDB keys'''
    keys = []
    for obj in objs:
        key = lmdb_key.item_to_key(obj)
        keys.append(key)
    return keys


def sort_key(obj, ordering):
    '''generates a sort key tuple for an object based on ordering fields'''
    keys = []
    for field in ordering:
        if field.startswith("-"):
            attr = field[1:]
            val = get_attr(obj, attr)
            keys.append(_Descending(val))
        else:
            val = get_attr(obj, field)
            keys.append(val)
    return tuple(keys)

class _Descending:
    '''Wrapper that inverts comparisons for any datatype.'''
    def __init__(self, value):
        self.value = value
    def __lt__(self, other):
        return self.value > other.value
    def __gt__(self, other):
        return self.value < other.value
    def __eq__(self, other):
        return self.value == other.value
    def __le__(self, other):
        return self.value >= other.value
    def __ge__(self, other):
        return self.value <= other.value
    def __hash__(self):
        return hash(self.value)

def ensure_relations_loaded(queryset, attr_name):
    '''ensures that related objects for a given attribute are preloaded'''
    relations_to_class_map = queryset._data.cache.relations_to_class_map
    if attr_name in relations_to_class_map:
        cls = relations_to_class_map[attr_name]
        queryset._data.cache.preload_class_instances(cls)


def queryset_summary(qs):
    '''generates a colored string summary of the QuerySet'''

    R= "\033[91m"
    G= "\033[92m"
    B= "\033[94m"
    GR= "\033[90m"
    P = "\033[35m"
    RE= "\033[0m"

    parts = []

    for op, params in qs._filters:
        for key, value in params.items():

            # split attr__lookup
            if "__" in key:
                attr, lookup = key.rsplit("__", 1)
            else:
                attr, lookup = key, "eq"

            symbol = {
                "eq": "==",
                "gt": ">",
                "lt": "<",
                "gte": ">=",
                "lte": "<=",
                "in": "in"
            }.get(lookup, lookup)

            pretty_attr = attr.replace("__", ".")

            if isinstance(value, str):
                value_repr = f"'{value}'"
            else:
                value_repr = value

            # Build one segment: filter(duration > 0.1)
            m = f"{B}{op}{RE}({pretty_attr} {P}{symbol}{RE} {value_repr})"
            parts.append(m)
    if hasattr(qs, "_ordering") and qs._ordering:
        pretty_order = []
        for field in qs._ordering:
            if field.startswith("-"):
                pretty_order.append(f"-{field[1:].replace('__', '.')}")
            else:
                pretty_order.append(field.replace("__", "."))
        joined = ", ".join(pretty_order)
        parts.append(f"{B}order_by{RE}({joined})")

    joined = ", ".join(parts)
    return f"<{R}QuerySet:{RE} {joined}>"




