## Goal

Add segment-level codebook-index retrieval to `phraser` with the same
store-or-compute behavior as `segment_embeddings.get_embeddings()`, backed by
`echoframe` storage and `to-vector` codebook extraction helpers.

Hubert codebook support is out of scope.

## Feature 1: EchoFrame Codebook Artifacts

### Requirements

- `echoframe` must support stored codebook matrices as first-class artifacts in
  addition to stored codebook indices.
- Codebook-matrix storage must use a distinct `output_type` from
  `codebook_indices` so metadata semantics stay unambiguous.
- `EchoframeMetadata` and `Store` lookup APIs should keep `layer` required for
  now.
- Use `layer=0` by convention for both `codebook_indices` and
  `codebook_matrix` artifacts.
- `echoframe` validation should enforce `layer=0` for both codebook artifact
  types.
- `codebook_indices` payloads must support storing one artifact containing all
  heads/codebooks for a segment.
- The `echoframe` public API should expose an immutable container for codebook
  indices, parallel to `Embeddings`.
- The codebook-index container must carry:
  - the stored indices payload
  - one or more `echoframe` keys for the stored index payload(s)
  - one or more `echoframe` keys for the referenced codebook matrix artifact(s)
  - model metadata needed to interpret the payload shape
- The codebook-index container must support lazy codebook loading from an
  attached `Store`, cache the loaded codebook matrix/matrices on first access,
  and expose a method that returns codevectors.
- The object should preserve model-specific structure rather than forcing one
  fully flattened abstraction.

### Tests

- Metadata validation accepts the new codebook-matrix `output_type`.
- Store put/load/find/exists paths work for codebook-matrix artifacts.
- Codebook-index container validates its metadata and array shape invariants.
- Codebook-index container loads linked codebook matrices through the store.
- Codebook-index container caches linked matrices after first load.
- Codebook-index container reconstructs codevectors correctly for:
  - `wav2vec2`
  - `spidr`

### Open Questions

- None currently.

## Feature 2: To-Vector Codebook Extraction Boundary

### Requirements

- `phraser` must be able to compute codebook indices and referenced codebook
  matrices through stable helper calls instead of embedding model-family
  internals directly in `segment_embeddings.py`.
- `wav2vec2` extraction must return:
  - frame-ordered codebook index pairs
  - the codebook matrix needed to reconstruct codevectors
- `spidr` extraction must return:
  - one segment payload containing all codebook heads
  - the codebook matrices for those heads
- `spidr` should return all available heads/codebooks rather than requiring a
  layer argument in the public API.
- `hubert` codebook support remains excluded.
- If existing `to-vector` helpers are not quite sufficient, add small wrapper
  helpers there rather than duplicating model-specific extraction logic in
  `phraser`.

### Tests

- `to-vector` helpers return stable index payload shapes for `wav2vec2`.
- `to-vector` helpers return stable index payload shapes for `spidr`.
- `to-vector` helpers expose codebook matrices in a form `echoframe` can store
  and the new container can consume.

### Open Questions

- Whether any tiny normalization helper should live in `to-vector` or
  `echoframe`; decide during implementation based on where shape semantics are
  clearest.

## Feature 3: Phraser Segment Codebook Retrieval API

### Requirements

- `phraser.segment_embeddings` must expose:
  - `get_codebook_indices(item, ...)`
  - `get_codebook_indices_batch(items, ...)`
- The call signature should mirror `get_embeddings(...)` and
  `get_embeddings_batch(...)` as closely as practical:
  - `collar`
  - `model_name`
  - `model`
  - `store`
  - `store_root`
  - `gpu`
  - `tags`
- The new functions must:
  - convert a segment into a stable `echoframe` key
  - check whether the required codebook-index payload is already stored
  - compute and store missing indices when needed
  - ensure the referenced codebook matrix artifact(s) are also stored
  - return the new `echoframe` codebook-index object
- `wav2vec2` retrieval should not require a public `layer` parameter.
- `spidr` retrieval should also not require a public `layer` parameter because
  all heads are returned together.
- Model-family branching should stay internal; the public API remains unified.
- Existing embedding retrieval behavior must not regress.

### Tests

- Cache-hit path returns stored codebook indices without recomputation.
- Cache-miss path computes, stores, and returns indices.
- Returned objects include both index payload keys and linked codebook-matrix
  keys.
- Batch retrieval returns a collection object parallel to
  `TokenEmbeddings`, or a close equivalent if a dedicated batch container is
  introduced.
- `wav2vec2` and `spidr` both work through the same public API.
- Existing embedding tests continue to pass.

### Open Questions

- Whether batch results should reuse a generic token-container pattern in
  `echoframe` or introduce a dedicated `TokenCodebooks` class.

## Feature 4: Shape And Semantics Documentation

### Requirements

- Document the returned payload semantics for each supported family.
- Document that `wav2vec2` payloads represent frame-major index pairs.
- Document that `spidr` payloads represent one segment artifact containing all
  heads/codebooks.
- Document how linked codebook matrices are resolved and cached.
- Document that Hubert codebook support is out of scope.

### Tests

- Documentation examples should be covered by lightweight public-API tests
  where practical.

### Open Questions

- None currently.

## Suggested Implementation Order

1. Add `echoframe` support for codebook-matrix artifacts and the immutable
   codebook-index container.
2. Tighten or add `to-vector` helpers so both supported model families return
   stable payloads plus codebook matrices.
3. Add `phraser.segment_embeddings` retrieval functions and batch support.
4. Add focused docs and public-api tests across the touched repos.
