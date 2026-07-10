# Query Key Snapshots And Store Phases

## Context

`Store.attach_query_roots()` builds query roots such as `store.audios`,
`store.phrases`, and `store.words`. Each query root owns a `Data` object with a
list of keys for that class.

Those keys are currently initialized from `Store.rank_to_keys_dict()`, which is
itself a cached scan of LMDB keys grouped by class rank.

## Intended Behavior

For now, writing/building a database and querying/using a database are treated
as distinct phases.

During a write phase, `Store.save()`, `Store.save_many()`, `Store.delete()`, and
`Store.delete_many()` update LMDB and the object cache, but they do not keep all
query key snapshots live. In particular, these views can be stale after writes:

- `Data.keys` on query roots
- `Store._rank_to_keys_dict`
- loaded objects in `Store._cache`

This is intentional. Updating every view on every write is possible, but it adds
bookkeeping and creates another source of complexity. Loader/build scripts should
finish their writes first, then call `Store.refresh_query_roots()` or reopen the
store before relying on store-level query roots such as `store.words`.

Direct key loads and freshly scanned label-index lookups are separate from this
query-root snapshot policy, but loaded model objects may still carry their own
relationship caches. Treat refresh/reopen as the boundary before analysis.

## Options Considered

### Keep `Data.keys` as a list and add `Data.key_set`

This preserves stable ordering and existing slicing behavior:

```python
keys = self._data.keys[:n]
```

It also allows cheap membership checks on save:

```python
if key not in self.key_set: self.keys.append(key)
```

Delete still has an accepted O(n) list removal cost:

```python
if key in self.key_set:
    self.key_set.remove(key)
    self.keys.remove(key)
```

### Derive `rank_to_keys_dict()` from query data

Once query roots exist, `rank_to_keys_dict()` could return data from the query
roots instead of maintaining a separate cached dictionary. That would make
`Data.keys` the live source of truth.

This is cleaner conceptually, but couples `Store.rank_to_keys_dict()` to query
root setup.

### Store-owned key registry

A stronger design would make `Store` own a single key registry per rank, and let
`Data` reference that registry. This gives one source of truth, but is a larger
refactor.

## Decision For Now

Keep query roots as phase snapshots.

Do not add a key registry, do not derive `rank_to_keys_dict()` from query roots,
and do not add `deleted_keys`/`active_keys`.

If query staleness becomes a practical problem, the preferred small next step is
to keep `Data.keys` as a list, add `Data.key_set`, and update query `Data` after
successful saves/deletes. `Store._rank_to_keys_dict` can remain a cached DB scan
unless a later change makes it the active source of truth.
