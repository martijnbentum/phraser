'''LMDB persistence: the Store, keys, and value packing.

Kept free of imports on purpose: models import storage.key_helper, so eager
imports here would load the store while the models are still loading.
'''
