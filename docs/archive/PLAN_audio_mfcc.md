# Audio MFCC Features

## Requirements

- Replace `phraser.audio` with an `audio` package containing `audio.py`,
  `mfcc.py`, and `batch.py`.
- Add `phraser.audio.mfcc.mfcc(segment, wav2vec2_frames=True)`.
- Return 13 MFCCs, 13 deltas, and 13 delta-deltas as a `(frames, 39)`
  NumPy matrix.
- Use fixed speech settings: 40 mel bands, a 25 ms non-centered window,
  and a 10 ms computation hop anchored at audio time zero.
- Compute deltas on the full 10 ms grid with four neighboring frames of
  audio context on each side when available.
- Return the aligned 20 ms wav2vec2 frame grid by default, or the full
  10 ms grid when `wav2vec2_frames=False`.
- Include complete 25 ms frames that overlap any part of the segment.
- Add `recording_mfcc(audio)` for the full 10 ms recording matrix.
- Add `slice_recording_mfcc(matrix, segment, wav2vec2_frames=True)`.
- Add audio-grouped `mfcc_batch(...)` with merged frame ranges and optional
  process-based parallel execution.
- Expose lazy `Segment.mfcc`, cached transiently as `segment._mfcc`.
- Let `mfcc_batch(..., cache_on_segment=True)` reuse and populate the default
  20 ms segment cache. Full-rate 10 ms batches leave it unchanged.
- Keep package-level MFCC exports out of scope.

## Tests

- Check the 39-column output and expected frame counts at both frame rates.
- Check that 20 ms output matches alternating rows of the 10 ms output.
- Check that an interior segment receives the same contextual features as
  its corresponding frames in a broader segment.
- Check extraction at both recording boundaries.
- Check recording-wide extraction and slicing at both frame rates.
- Check sequential and parallel batch extraction preserve input order.
- Check lazy property caching, batch cache hits, opt-out behavior, and
  one-time cache warnings.
- Check invalid durations and ranges without an aligned frame.
