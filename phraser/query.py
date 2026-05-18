from .comparison import OPS
from . import key_helper

class DoesNotExist(Exception):
    pass
class MultipleObjectsReturned(Exception):
    pass

R= "\033[91m"
G= "\033[92m"
B= "\033[94m"
GR= "\033[90m"
RE= "\033[0m"

def queryset_from_items(items, store):
    """
    Create a QuerySet for items.

    Example:
        qs = queryset_from_items(some_words, store)
        qs.filter(label="t").order_by("start")
    """
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
    if store is None:
        raise ValueError("store must be provided to queryset_from_items")

    data = Data(cls, store)
    keys = objects_to_keys(items)
    data.keys = keys
    return QuerySet(data)


def get_class_object(cls, store):
    data = Data(cls, store)
    return QuerySet(data)

class Data:
    def __init__(self, cls, store):
        self.cls = cls
        self.store = store
        self.object_type = cls.__name__
        self.rank = key_helper.CLASS_RANK_MAP[self.object_type]
        self._get_keys(update=False)

    def _get_keys(self, update=False):
        d = self.store.rank_to_keys_dict(update=update)
        m = f'No keys found for object type: {B}{self.object_type}{RE}'
        try:
            self.keys = d[self.rank]
        except KeyError:
            self.keys = []

    def load(self, keys=None):
        if keys is None:
            keys = self.keys
        return self.store.load_many(keys)


class QuerySet:
    def __init__(self, data, filters=None, ordering=None):
        self._data = data
        self._filters = filters or []
        self._ordering = ordering

    def all(self):
        return QuerySet(self._data, self._filters)

    def get_one(self):
        if self._filters or self._ordering:
            raise NotImplementedError("get_one() is only supported on unfiltered QuerySets.")
        return self._data.load([self._data.keys[0]])[0]

    def get_n(self, n):
        if self._filters or self._ordering:
            raise NotImplementedError("get_n() is only supported on unfiltered QuerySets.")
        keys = self._data.keys[:n]
        return self._data.load(keys)

    def get(self, **kwargs):
        results = list(self.filter(**kwargs))
        if not results:
            raise DoesNotExist(f'no element exists in database:{kwargs}')
        if len(results) > 1:
            m = f'multiple elements ({len(results)}) exist in database:{kwargs}'
            m += f'\nResults: {results}'
            raise MultipleObjectsReturned(m)
        return results[0]

    def get_or_none(self, **kwargs):
        try:
            return self.get(**kwargs)
        except DoesNotExist:
            return None

    def filter(self, **kwargs):
        return QuerySet(self._data, self._filters + [("filter", kwargs)])

    def exclude(self, **kwargs):
        return QuerySet(self._data, self._filters + [("exclude", kwargs)])

    def order_by(self, *fields):
        return QuerySet(self._data, self._filters, ordering=fields)

    def _apply(self):
        if hasattr(self, '_objs'):
            return self._objs
        objs = self._data.load()
        for op, params in self._filters:
            self.check_relations_loaded(params)
            if op == "filter":
                objs = filter_objects(objs, **params)
            elif op == "exclude":
                objs = [x for x in objs if not object_matches(x, **params)]
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
        for key in params.keys():
            attr_names = key.split("__")
            for attr_name in attr_names:
                ensure_relations_loaded(self, attr_name)

    @property
    def store(self):
        return self._data.store


def get_attr(obj, path):
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
    """
    parts = key.split("__")

    if parts[-1] in OPS:
        lookup = parts[-1]
        op = OPS[lookup]
        attr_path = "__".join(parts[:-1])
        attr_val, _ = get_attr(obj, attr_path)
        return op(attr_val, value)

    attr = parts[0]
    rest = "__".join(parts[1:]) if len(parts) > 1 else None

    attr_val = getattr(obj, attr)

    if isinstance(attr_val, list):
        if rest is None:
            return attr_val == value
        for child in attr_val:
            if matches(child, rest, value):
                return True
        return False

    if rest is None:
        return attr_val == value

    return matches(attr_val, rest, value)


def object_matches(obj, **params):
    for key, value in params.items():
        try:
            ok = matches(obj, key, value)
        except Exception as e:
            raise type(e)(f'{e} (lookup={key}, value={value}, obj={obj})')
        if not ok:
            return False
    return True


def filter_objects(objs, **filters):
    result = []
    for obj in objs:
        if object_matches(obj, **filters):
            result.append(obj)
    return result


def objects_to_keys(objs):
    keys = []
    for obj in objs:
        keys.append(obj.key)
    return keys


def sort_key(obj, ordering):
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
    relations_to_class_map = queryset._data.store.relations_to_class_map
    if attr_name in relations_to_class_map:
        cls = relations_to_class_map[attr_name]
        queryset._data.store.preload_class_instances(cls)


def queryset_summary(qs):
    R= "\033[91m"
    G= "\033[92m"
    B= "\033[94m"
    GR= "\033[90m"
    P = "\033[35m"
    RE= "\033[0m"

    parts = []

    for op, params in qs._filters:
        for key, value in params.items():

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
