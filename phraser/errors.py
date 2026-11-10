'''Exceptions shared by the store and the models.'''


class UnboundStoreError(RuntimeError):
    pass


class ClosedStoreError(RuntimeError):
    pass
