## Refactor Summary

`phraser.segment_embeddings` should stop owning retrieval orchestration and
become a thin forwarding layer onto `echoframe.segment_features`.


## Feature 1: Move Typed Loading To `echoframe.Store`

### Requirements

- `echoframe.Store` should own typed loading for:
  - embeddings
  - many embeddings
  - codebook
  - many codebooks
- The typed loaders should preserve the current object shapes and aggregation
  behavior used by `phraser.segment_embeddings`.

### Tests

- Typed loaders in `echoframe` reproduce the current `phraser` object behavior.


## Feature 2: Move Segment Retrieval Orchestration To `echoframe.segment_features`

### Requirements

- The orchestration for segment retrieval should move out of
  `phraser.segment_embeddings`.
- `echoframe.segment_features` should accept segment objects directly.
- `echoframe.segment_features` should expose public
  `segment_to_echoframe_key(...)`.
- `to-vector` should remain unchanged in this refactor.

### Tests

- Cache-hit and cache-miss behavior match the current public behavior.
- Batch retrieval still returns token containers.


## Feature 3: Keep `phraser.segment_embeddings` As A Thin Forwarder

### Requirements

- `phraser.segment_embeddings` should forward its public APIs directly to
  `echoframe.segment_features`.
- No phraser-specific retrieval logic should remain in the module.

### Tests

- Forwarding tests confirm delegation.
- Existing higher-level tests keep passing.
