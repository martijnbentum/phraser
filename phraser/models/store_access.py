'''Store lookup shared by Audio, Speaker and Segment.'''

from ..errors import ClosedStoreError, UnboundStoreError


def get_store(obj):
    '''Return the store obj is bound to; raise if unbound or closed.'''
    store = getattr(obj, '_store', None)
    name = obj.__class__.__name__
    if store is None:
        m = f'{name} is not bound to a Store. Create it with store=...'
        raise UnboundStoreError(m)
    if store.closed:
        m = f'{name} is bound to a closed Store. '
        m += 'Call store.open() to reopen it.'
        raise ClosedStoreError(m)
    return store


def exists_in_db(obj):
    '''Whether an object with obj's identity fields is in the store.'''
    cls = obj.__class__
    lookup = {k: getattr(obj, k) for k in cls.IDENTITY_FIELDS}
    existing = get_store(obj).query_for_class(cls).get_or_none(**lookup)
    return existing is not None
