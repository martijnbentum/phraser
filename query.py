import lmdb_key
import operator

def get_class_object(cls, cache):
    data = Data(cls, cache)
    return QuerySet(data)

class Data:
    def __init__(self, cls, cache):
        self.cls = cls
        self.cache = cache
        self.object_type = cls.__name__
        self._get_keys(update = False)

    def _get_keys(self, update = False):
        d = self.cache.object_type_to_keys_dict(update = update)
        m = f'No keys found for object type: {self.object_type}'
        if self.object_type not in d:
            raise ValueError(m)
        self.keys = d[self.object_type]

    def load(self, keys = None):
        if keys is None: keys = self.keys
        objs = self.cache.load_many(keys)
        return objs


class QuerySet:
    def __init__(self, data, filters=None):
        self._data = data
        self._filters = filters or []

    def all(self):
        return QuerySet(self._data, self._filters)

    def filter(self, **kwargs):
        return QuerySet(self._data, self._filters + [("filter", kwargs)])
            

    def exclude(self, **kwargs):
        return QuerySet(self._data, self._filters + [("exclude", kwargs)])

    def _apply(self):
        if hasattr(self, '_objs'): return self._objs
        objs = self._data.load()
        for op, params in self._filters:
            if op == "filter":
                objs = filter_objects(objs, **params)
            elif op == "exclude":
                excluded = set(filter_objects(objs, **params))
                objs = [x for x in objs if x not in excluded]
        self._objs = objs
        return self._objs

    def __iter__(self):
        return iter(self._apply())

    def __len__(self):
        return len(self._apply())

    def __repr__(self):
        return f"<QuerySet filters={len(self._filters)}>"


OPS = {
    "gt":  operator.gt,
    "lt":  operator.lt,
    "gte": operator.ge,
    "lte": operator.le,
    "in":  lambda a, b: a in b,
    "eq":  operator.eq,
}

def get_attr(obj, path):
    """Resolve nested attributes (speaker__gender → obj.speaker.gender)."""
    for part in path.split("__"):
        obj = getattr(obj, part)
    return obj

def matches(obj, key, value):
    """
    Decide which operator to use based on Django-style lookup syntax.
    """
    if "__" in key:
        *path, lookup = key.split("__")
        path = "__".join(path)
        op = OPS.get(lookup)
        if op is None:
            # Fallback: treat everything except known lookups as eq
            return get_attr(obj, key) == value
        attr_val = get_attr(obj, path)
        return op(attr_val, value)
    else:
        # simple equality
        return get_attr(obj, key) == value

def filter_objects(objs, **filters):
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
    keys = []
    for obj in objs:
        key = lmdb_key.item_to_key(obj)
        keys.append(key)
    return keys
