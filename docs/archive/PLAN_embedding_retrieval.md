# Embedding Retrieval — Feature Plan (phraser)

Integration between phraser segment objects, echoframe storage, and
to-vector feature extraction. Entry point: `phraser/segment_embeddings.py`.

See `echoframe/PLAN_embedding_retrieval.md` for F1 (`Embeddings` class)
and F6 (eviction), which live in that repo.

---

## F2 — Frame selection before storage

**What:** Store only frames 100% within `[segment.start, segment.end]`,
not the full collared window.

**Requirements:**
- After computing hidden states, use `frame.Frames` +
  `select_frames(start_time, end_time, percentage_overlap=100)` to
  select frames within the original phone boundaries (in seconds)
- `_segment_window` must preserve original `start_ms`/`end_ms`
  (before collar) so they can be passed to frame selection
- Stored array shape: `[n_selected_frames, embed_dim]`

**Tests:**
- Given a phone 1100–1400ms, collar 500ms → model called with
  0.6s–1.9s, stored frames only within 1.1s–1.4s
- Phone at audio boundary (collar clamped) still selects correct frames
- No frames selected raises a clear error

---

## F3 — Multi-layer support (`layers` parameter)

**What:** Accept a list of layers, run model once, store each layer as
a separate echoframe entry.

**Requirements:**
- `layers` accepts `int` or `list[int]`; internally always treated
  as a list
- One forward pass → iterate over `layers`, store each separately
- Return `Embeddings` with `dims=('layers', 'frames', 'embed_dim')`
  when multiple layers; `('frames', 'embed_dim')` for a single int

**Tests:**
- `layers=[4, 6]` → two echoframe `put` calls, returned array shape
  `(2, n_frames, embed_dim)`
- `layer=4` (int) → shape `(n_frames, embed_dim)`, dims has no
  `'layers'`
- Out-of-range layer in list raises `ValueError` naming the bad layer

---

## F4 — Aggregation on retrieval

**What:** Apply aggregation to the frame dimension at call time
(not stored).

**Requirements:**
- `aggregation=None` → return all frames as-is
- `aggregation='mean'` → `np.mean(data, axis=frames_axis)`
- `aggregation='centroid'` → use `FrameSelection.mean_time()` to find
  the centroid frame index, return that single frame
- `dims` updated to reflect collapsed/removed `'frames'` axis

**Tests:**
- `aggregation='mean'` on shape `(3, 10, 768)` → `(3, 768)`,
  dims `('layers', 'embed_dim')`
- `aggregation=None` preserves shape
- `aggregation='centroid'` returns exactly one frame per layer
- Unknown aggregation string raises `ValueError`

---

## F5 — Batch function

**What:** `get_embeddings_batch(segments, layers, collar, model_name,
...)` → `dict[segment, Embeddings]`

**Requirements:**
- Check echoframe for all `(segment, layer)` pairs first; collect
  misses
- Group misses by `(audio_file, collar)` → one model call per group,
  extracting all needed layers at once
- Store computed entries; merge with cache hits
- Skipping already-stored entries makes it restartable

**Tests:**
- All hits → zero model calls
- Two segments from same audio file, both misses → exactly one model
  call
- Two segments from different audio files → two model calls
- Partial hit (one layer cached, one not) → model called once, only
  missing layer stored

---

## Notes

- F2–F5 live in `phraser/segment_embeddings.py` and
  `tests/test_segment_embeddings.py`
- `Embeddings` (F1) and eviction (F6) live in the `echoframe` repo;
  import as `from echoframe import Embeddings`
- `frame` is a hard dependency in `pyproject.toml` (already present)
- Collar is part of the echoframe identity key; model runs on
  `[segment.start - collar, segment.end + collar]` but only frames
  within `[segment.start, segment.end]` are stored
- Recency window and storage budget are `.env`-configurable in echoframe
