## Goal

Keep `phraser.segment_embeddings` simple after the `echoframe` LMDB lifecycle
fix by removing local store-resolution indirection and avoiding any phraser-side
store cache.

## Feature 1: Inline Store Construction

### Requirements

- `phraser` must not add a process-local store cache.
- `_resolve_store(...)` should be removed from
  `phraser/segment_embeddings.py`.
- Each public retrieval function should use the passed `store` when provided.
- When `store` is not provided, each public retrieval function should directly
  construct `echoframe.Store(store_root)` inline.
- This is a cleanup only; it should not otherwise change retrieval behavior.

### Tests

- Existing focused retrieval tests continue to pass.
- Passed `store` objects are used unchanged.
- No dedicated caching tests are needed in `phraser`; lifecycle ownership stays
  in `echoframe`.

### Open Questions

- None currently.

## Suggested Implementation Order

1. Remove `_resolve_store(...)`.
2. Inline the minimal `store is None` logic in the public retrieval functions.
3. Run the focused `phraser` retrieval tests.
