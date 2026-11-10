# phraser

`phraser` is a small Python package for working with time-aligned speech
annotations backed by LMDB. It models hierarchical speech data such as
`Audio`, `Phrase`, `Word`, `Syllable`, `Phone`, and `Speaker`, and provides
helpers for serialization, key generation, querying, and corpus-loading
scripts.

The package is aimed at workflows where you want to:

- store aligned speech annotations in LMDB
- load and traverse phrase and segment hierarchies
- query objects by attributes and relations
- build or inspect speech datasets programmatically

## Install

The project now uses `pyproject.toml` and can be installed as a normal Python
package.

Create a local `.env` file before using the package. A starter template is
provided in [`example_env`](./example_env).

### Install with `pip`

```bash
pip install git+https://github.com/martijnbentum/phraser.git
```

### Install with `uv pip`

```bash
uv pip install git+https://github.com/martijnbentum/phraser.git
```

### Editable install

With `pip`:

```bash
git clone git@github.com:martijnbentum/phraser.git
cd phraser
mkdir -p .build
pip install -e .
```

With `uv pip`:

```bash
git clone git@github.com:martijnbentum/phraser.git
cd phraser
mkdir -p .build
uv pip install -e .
```

Editable-install metadata is stored in `.build/` instead of creating
`*.egg-info` at the repository root.

## Git-based dependencies

This project has required Git-based runtime dependencies:

- `ssh-audio-play`
- `webmaus`
- `frame`
- `phone-mapper`

They are declared in [`pyproject.toml`](./pyproject.toml). The current package
configuration uses `git+https` URLs, so installation requires:

- Git to be installed
- network access to GitHub
- permission to access the referenced repositories when they are private

## Configuration

Copy [`example_env`](./example_env) to `.env` and update the paths for your
machine.

```bash
cp example_env .env
```

Supported variables:

- `PHRASER_DATA_DIR`: base data directory used by the package
- `PHRASER_DEFAULT_LMDB`: LMDB path for the default database
- `PHRASER_CGN_LMDB`: LMDB path for the CGN database

Example:

```dotenv
PHRASER_DATA_DIR=/path/to/phraser-data
PHRASER_DEFAULT_LMDB=/path/to/phraser-data/default_lmdb
PHRASER_CGN_LMDB=/path/to/phraser-data/cgn_lmdb
```

### `ssh_audio_play` and the same `.env`

Yes, the same `.env` can also be used for `ssh_audio_play` configuration if
that dependency reads settings from process environment variables. `phraser`
forwards any `.env` keys starting with `SSH_AUDIO_PLAY_` into `os.environ`
when the package configuration is loaded.

## Example

### Load the default cache

```python
from phraser import load_cache

load_cache()
```

### Access objects from the package API

Every segment is constructed with its identity: `label`, `start`, `end`,
`audio_id`, and `speaker_id`. Construction stages the object in memory;
nothing is written until an explicit save.

```python
from phraser import Audio, Phrase, Word, Syllable, Phone, Speaker, Store

store = Store("/path/to/lmdb")
audio = store.create(Audio, filename="example.wav")
speaker = store.create(Speaker, name="spk1", dataset="demo")

identity = dict(audio_id=audio.identifier, speaker_id=speaker.identifier)
phrase = store.create(Phrase, label="hello world", start=0, end=1200,
    **identity)
word = store.create(Word, label="hello", start=0, end=500, **identity)
syllable = store.create(Syllable, label="he", start=0, end=250, **identity)
phone = store.create(Phone, label="h", start=0, end=100, **identity)
```

### Stage and save a phrase tree

On a `Segment`, only `save` and `delete` touch the database; every other
method and property (`add_parent`, `add_children`, `replace_children`, ...)
works in memory. Linking stages a tree; persisting it is an explicit call
to `store.save_phrase_trees`, which validates each tree and writes nothing
when any validation fails.

```python
phrase.add_children([word])       # staged, never writes
word.add_children([syllable])
syllable.add_children([phone])
phrase.validate_tree()            # optional early check

store.save_phrase_trees([phrase])
```

### Convert TextGrid annotations

TextGrid conversion is store-bound staging. Pass a `Store`: conversion
suppresses individual constructor/link writes so the loader can build all
`Phrase`, `Word`, `Syllable`, and `Phone` objects in memory and batch-save them
later.

```python
from phraser import Store
from phraser.textgrids import loader

store = Store("/path/to/lmdb")
items = loader.textgrid_filename_to_database_objects(
    "example.TextGrid",
    audio=audio,
    speaker=speaker,
    store=store,
)

loader.save_textgrid_items(items, store=store, existing="append")
store.refresh_query_roots()
```

`save_textgrid_items()` supports `existing="append"`, `"add_missing"`,
`"replace"`, and `"upsert"`. Policies other than `"append"` run an existence
check using phrase identity `(audio_id, speaker_id, start)` and require an
existing `Audio` object.

### Build a CGN database from original AWD alignments

CGN has two separate import paths:

- `scripts/build_cgn_awd_textgrid_db.py` builds a fresh database directly from
  the original paired `.ort` phrase annotations and `.awd` word/phone
  alignments. It has no dependency on the WebMAUS CGN scripts or
  `phraser.textgrids`.
- `scripts/prepare_cgn_webmaus_import.py` and
  `scripts/load_cgn_webmaus_textgrids_to_db.py` are the legacy path for
  WebMAUS-generated TextGrids.

The AWD builder takes phrase boundaries from the speaker tiers in `.ort`,
word and phone timing from the matching speaker, `_FON`, and `_SEG` tiers in
`.awd`, and maps CGN phones to IPA with `phone_mapper.cgn.cgn_to_ipa`.
Syllable tiers are derived during import with `dutch_syllabifier`.

The builder is a Python/IPython workflow rather than a CLI. It recursively
collects files below `audio_dir`, `awd_dir`, and `ort_dir`, then matches them
by bare recording stem, for example `fn000001.awd`, `fn000001.ort`, and
`fn000001.wav`. Their relative subdirectories do not need to match. Duplicate
stems within any one source directory are rejected. The source directories
default to the production CGN locations:

- audio: `/vol/bigdata/corpora2/CGN2/data/audio/wav/`
- AWD: `../data/awd/`
- ORT: `../data/ort/`
- database: `../data/cgn_awd_lmdb`
- speakers: `/vol/bigdata/corpora2/CGN2/data/meta/text/speakers.txt`
- report: `../data/cgn_awd_import_report.json`

Relative paths are resolved from the IPython process's current working
directory. With all defaults, the import can be started as follows:

```python
from scripts.build_cgn_awd_textgrid_db import build_cgn_awd_database

report = build_cgn_awd_database(show_progress=True)

report.to_dict()
```

The default target is separate from the legacy CGN database. The builder
refuses the configured legacy CGN database and refuses any non-empty target
unless `resume=True` is supplied. Resume mode loads the database Audio records
once and fast-skips earlier recordings by normalized audio filename. It fully
audits the final three stored recordings, skipping each only when its staged
phrase signature matches. Missing label-index entries in that tail are
repaired in one batch; a partial or changed phrase signature is reported as
an error. This tail audit relies on deterministic source order and the rule
that database-write errors stop the import immediately.

Source-data errors occur before database writes, are reported and skipped,
and are retried on the next resume. Once database writing for a recording
starts, any write error aborts the run so an incomplete write cannot be
followed by later recordings. Set `strict_pairs=True` to require the audio,
ORT, and AWD directories to contain the same recording stems.

The import saves one recording at a time. `show_progress` controls the
corpus-level recording bar. LMDB retains its existing segment-batch progress
display. Equal-sized segment bars are the main segment rows followed by their
label-index entries; small bars usually represent newly saved speakers.

### Timing and shared-phone flags

Segments expose three independent boolean properties:

| Property | Intended objects | Meaning when set |
| --- | --- | --- |
| `timing_uncertain` | Any segment with a timing concern | A known concern about this object's timing, including a boundary inferred without acoustic verification |
| `shared_phone` | Phone | The source identifies this phone as shared across words |
| `missing_shared_phone` | Word or Syllable | A source-confirmed shared phone is missing from this object's linked phones |

False means no concern or sharing information has been recorded for that
flag; it does not mean the annotation was verified. Sharing alone does not
imply uncertain timing. These properties describe each object's own state:
setting one does not set flags on parents, children, or neighboring words.

```python
phone.shared_phone = True
word.missing_shared_phone = True
syllable.missing_shared_phone = True

# If this phone's boundary was inferred during a repair:
phone.timing_uncertain = True
syllable.timing_uncertain = True

store.save_many([phone, word, syllable], overwrite=True)
```

Assignments accept only `True` or `False` and modify memory until explicitly
saved. Flags do not change timing, parent links, `.phones` results, or audit
rules. In particular, `missing_shared_phone` records a known relationship
gap; it does not retrieve or attach the shared phone. Importers do not set
these flags automatically.

The existing flags byte stores duration assessments in bits 0–1,
`timing_uncertain` in bit 2, `shared_phone` in bit 3, and
`missing_shared_phone` in bit 4. Setters preserve every unrelated bit.
Existing zero-valued flags read False, and the binary layout is unchanged.

### Speech overlap

`segment.overlap` is true when the segment's time span intersects a
non-silence word from another speaker in the same audio. This applies to
phrases, words, syllables, and phones. Touching boundaries do not count.
A phrase includes its pauses, so it can overlap while all its words do not.
`segment.overlap_items` returns the word evidence for that status and is
equivalent to `segment.overlap_words`.

Use `segment.overlap_phrases`, `segment.overlap_syllables`, and
`segment.overlap_phones` to inspect intersecting other-speaker segments at
other levels. These report time intersections in the same audio, without
changing the word-based status rule. For example, a word during another
speaker's phrase pause can have nonempty `overlap_phrases` while its
`overlap_words` is empty and `overlap` is false. Phone results include
explicit silence phones when their spans intersect. Results use persisted
annotations and are ordered by start time, then end time.

`segment.overlap_all_items()` combines these results across the hierarchy:
phrases, words, syllables, then phones, ordered by time within each level.
A nonempty result does not imply `overlap=True`; only words determine status.

Marker overlap properties retain their separate annotation-link behavior.

Stored `overlap_code` values are `0` (computed false), `1` (computed true),
and `9` (not computed). Code `9` computes on demand from persisted words,
without writing. Existing `0` and `1` codes are used until recomputed.

```python
from phraser import set_overlap_codes, update_audio_overlap

# Before saving: provide every segment from every speaker in the audio.
items = []
for phrase in phrases:
    items.extend(phrase.items)
set_overlap_codes(items)

# After saving: scan all stored segments, including unlinked segments.
update_audio_overlap(audio)
```

The AWD builder computes codes after staging all speakers. The WebMAUS bulk
importer recomputes affected audios after saving. Other write workflows must
call `update_audio_overlap(audio)` after adding, deleting, or retiming
annotations. Code updates do not happen automatically on individual saves.
The legacy `check_overlap_audio` and `check_overlap_audios` entry points
preserve their names and calling pattern, and delegate to the new
computation. They intentionally replace the old phrase-reference results;
there is no separate legacy definition.

Existing databases need a full recomputation of all three codes; updating
only `9` leaves results from the old phrase-based reference definition.
No database migration runs automatically.

Run the migration on a consistent database copy with annotations unchanged.
From Python or IPython with the repository importable, first create a dry-run
report. Close any existing Store for this database before calling:

```python
from scripts.migrate_overlap import migrate_overlap

db_path = '/path/to/database-copy'
report = migrate_overlap(db_path)
report['summary']
report['report_path']
```

Dry runs open LMDB read-only. The JSON report contains old-to-new code
counts per level, examples, and the number of overlapping phrases without
overlapping owned words. To apply all codes and verify after reopening:

```python
report = migrate_overlap(db_path, apply=True)
report['complete'], report['verified']
```

Apply mode commits one audio at a time, including unlinked segments, and
changes only overlap-code bytes. Other stored fields and indexes remain
intact. Default reports are saved beside the database as
`<database>.overlap-dry-run.json` and `<database>.overlap-apply.json`.
Set `report_path='/path/to/report.json'` to choose another location. Existing
reports are protected against accidental overwrites. To continue an
interrupted apply run:

```python
report = migrate_overlap(db_path, apply=True, resume=True)
```

Resume checks the database path, definition version, audio inventory, and
fingerprints of completed audios. A changed completed audio is rejected.
An audio committed just before interruption but not yet checkpointed is
safely recomputed; its original transition counts may be lost. `limit=N`
processes at most N new audios in a batch. `complete` means every audio was
processed; `verified` means the processed audios passed verification after
reopening. Reports must be outside the database directory. Progress prints
by default; use `show_progress=False` to suppress it. After migrating, open
a fresh Store to read the updated results.

### Query loaded objects

```python
from phraser import Word, load_cache

load_cache()

short_words = Word.objects.filter(end__lt=500)
for word in short_words:
    print(word.label, word.start, word.end)
```

### Get one object

```python
from phraser import Audio, load_cache

load_cache()

audio = Audio.objects.get(filename="example.wav")
print(audio)
```

### Access stored embeddings

Segments (`Word`, `Syllable`, `Phone`, `Phrase`) can load their stored
hidden-state embeddings from an [`echoframe`](https://github.com/martijnbentum/echoframe)
store. Bind the echoframe store once with
`echoframe_store.attach_phraser_store(source_id, phraser_store)`, then call
`segment.embedding(...)`:

```python
# echoframe_store.attach_phraser_store('cgn-main', store) sets the binding

word = store.words.get(label="hello")
embedding = word.embedding("wav2vec2", layer=7)   # echoframe Embedding
embedding.data                                    # the hidden states
```

`embedding(model_name, layer, collar=500, store=None, fallback=False)` uses the
echoframe store bound to the segment's phraser store, or an explicit
`store=...` override. When `fallback=True` and nothing is stored for the
segment itself, it walks ancestors (e.g. `phone -> syllable -> word ->
phrase`) and returns the nearest ancestor embedding sliced to the segment as a
`SlicedEmbedding`:

```python
phone.embedding("wav2vec2", layer=7, fallback=True)   # sliced from an ancestor
```

This is a read-only accessor for already-stored hidden states; the
compute-and-store path lives in `phraser.segment_embeddings`.

### Extract MFCC features

Segments expose a lazily computed, cached MFCC matrix:

```python
phone = store.phones.get(label='p')
features = phone.mfcc

features.shape   # (number_of_frames, 39)
```

The 39 columns contain 13 static MFCCs, 13 deltas, and 13 delta-deltas.
Extraction uses 25 ms windows, 40 mel bands, and a 10 ms computation grid.
`Segment.mfcc` returns frames on the wav2vec2-aligned 20 ms grid and caches
the result as the transient `segment._mfcc` attribute.

The module-level functions provide explicit control:

```python
from phraser.audio.mfcc import (
    mfcc,
    recording_mfcc,
    slice_recording_mfcc,
)

aligned = mfcc(phone)   # default 20 ms frame grid
full_rate = mfcc(phone, wav2vec2_frames=False)   # full 10 ms grid

recording = recording_mfcc(phone.audio)   # always the full 10 ms grid
phone_frames = slice_recording_mfcc(recording, phone)
```

For multiple segments, `mfcc_batch` groups work by recording, merges nearby
frame ranges, and can process recordings in parallel:

```python
from phraser.audio.batch import mfcc_batch

matrices = mfcc_batch(phones, workers=8)

len(matrices) == len(phones)
matrices[0] is phones[0].mfcc
```

Batch results preserve input order. By default, existing segment caches are
reused and newly computed 20 ms matrices are cached on the segments. The
function warns once that cached matrices remain in memory. Use
`cache_on_segment=False` for transient results. A batch requested with
`wav2vec2_frames=False` returns 10 ms matrices and does not update
`segment.mfcc`.

### Access phone linguistic features

A `Phone` exposes static IPA reference data, derived purely from its label
(no audio, distinct from the neural `embedding(...)` above). The data lives
in `phraser/phonology/data/ipa_features.json` and is regenerated with
`python -m scripts.build_ipa_features`.

```python
from phraser import Phone

phone = Phone(label="p", start=0, end=100, save=False)

phone.type                       # 'consonant' (or 'vowel'), None if unknown
phone.linguistic_features        # full reference dict: type, place, manner,
                                 # voicing, and the 'features' sub-dict
phone.linguistic_features["place"]   # 'bilabial'
```

For the binary distinctive-feature matrix as a numeric vector (Hayes-style,
`+1 / -1 / 0`, where `0` = not applicable):

```python
phone.linguistic_features_vector   # tuple of +1/-1/0 in canonical order
phone.linguistic_features_names    # feature names, positionally aligned

dict(zip(phone.linguistic_features_names, phone.linguistic_features_vector))
# {'syllabic': -1, 'consonantal': 1, 'voice': -1, ...}
```

`linguistic_features_vector` covers only the distinctive-feature matrix (the
`features` sub-dict), not the articulatory descriptors. Both
`linguistic_features` and `linguistic_features_vector` return `None` for an
unknown label (e.g. `''` or `'(..)'` placeholders).

Stress is **not** part of this matrix - it is suprasegmental and lives on the
syllable. `Phone.stress` reads it from the parent syllable, so every phone in
the syllable (including consonants) reports the same value:

```python
phone.stress   # 'unstressed' / 'primary' / 'secondary' / 'unknown'
```

The underlying helpers (`get_phone_features`, `get_feature_vector`,
`FEATURE_ORDER`) live in `phraser.phonology.phone_features`.

## Copy recording materials to another store

```python
from phraser import Store, copy_audio_materials

source = Store(path='/path/to/source')
goal = copy_audio_materials(audio_keys, '/path/to/experiment/phraser_store',
    source_store=source)
# Work with goal.audios, goal.words, etc.; the caller owns both stores.
goal.close()
source.close()
```

`copy_audio_materials` lives in `phraser.copy_materials`. It copies the supplied
Audio records, all their phrases, words, syllables, and phones, and linked
speakers. Keys, identifiers, persisted metadata, and audio filenames are
preserved. Shared speakers and repeated Audio keys are copied once. Markers
are excluded, and links from speakers into unselected recordings are not
followed. The destination works independently of the source annotation
store; the audio files themselves stay at their original paths.

Copying reports key checking, destination creation, a recording-level
progress bar, query-root refresh, and final object/speaker counts and elapsed
time. Each recording's annotations are read and written in bulk, with the
destination object cache cleared between recordings.

The destination directory must be new or empty. The function returns an open
Store with refreshed query roots and normal label indexes. Source records
and cached objects remain unchanged. Recording selection, such as corpus
component or language filtering, belongs to the caller. Copying is performed
one recording at a time; an interrupted or failed copy can leave a partial
destination, which is not automatically overwritten on the next call.

## Sampling

Sampling returns existing objects without replacement and does not save or
copy anything. Provide recordings through `audios`, or pass an open `store`
to use its full population. No store is opened implicitly.

```python
from phraser import sample_audios, sample_speakers, sample_word_tokens
from phraser.sampling import cgn

audios = cgn.select_audios(store=store, components='befg', variant='nl')
words = sample_word_tokens('huis', audios=audios, n_tokens=100,
    min_duration_ms=100, max_duration_ms=1500,
    exclude_duration_outliers=True,
    gender='female', random_seed=0)
speakers = sample_speakers(store=store, n_speakers=10, gender='female',
    min_age=20, max_age=40)
recordings = sample_audios(store=store, n_audios=10, dataset='cgn',
    min_duration_ms=10000, random_seed=0)
```

`sample_phrase_tokens`, `sample_syllable_tokens`, and `sample_phone_tokens`
accept the same arguments as `sample_word_tokens`. `type_label` is an exact
label match; pass `None` to sample across labels. `n_tokens` is the total
for that call, not a quota per label. Word sampling defaults to inclusive
100–1500 ms bounds. Other segment levels default to no duration bounds.
Pass `None` to disable either bound.

All token samplers default to `exclude_duration_outliers=True`, excluding
tokens whose own recorded `duration_outlier` assessment is `short` or `long`.
Both `unset` (not assessed) and `within_range` remain eligible. Set the option
to `False` to ignore these assessments; absolute duration bounds still apply.
Sampling never calculates IQR fences, assesses durations, or changes flags.
Assessments must be populated separately; the current audit reports outliers
without writing these flags. A parent's flag does not exclude its children.

Speaker, duration, and flag restrictions apply before the final availability
check and random draw. Segment durations must be positive.
`speakers` accepts Speaker objects or byte keys
(one or a collection). Segment restrictions use each token's own speaker;
recording restrictions require one linked speaker matching all restrictions.

Speaker sampling returns unique speakers and supports `gender`, exact `age`,
inclusive `min_age`/`max_age`, `dataset`, and `speakers`. Without `audios`, it
uses all store speakers, including those without recordings. Age 0 means
unknown: range filters exclude it; `age=0` explicitly selects it. Recording
sampling supports inclusive duration bounds, `dataset`, `language`,
`dialect`, `speakers`, and `gender`.

All samplers raise `ValueError` if too few eligible objects remain. Set
`insufficient='drop'` to return an empty list instead. Empty input is valid;
requesting zero objects returns an empty list. A local random generator and
stable key ordering make seeded draws independent of input order without
changing global random state. Reuse selected keys to preserve a sample
across later annotation changes.

CGN selection checks the `comp-X/nl/filename` or `comp-X/vl/filename` path
layout and rejects unrecognized paths. Variant `nl`/`vl` is distinct from
Audio language metadata (`nld`). Component selection returns all matching
recordings; compose it with sampling or `copy_audio_materials` as needed.

## Repository layout

```text
phraser/
├── phraser/
├── scripts/
├── tests/
├── pyproject.toml
└── README.md
```

## Notes

- Importing `phraser` currently initializes the model cache through the package
  model layer.
- The corpus-loading scripts live in [`scripts/`](./scripts).
- A minimal packaging smoke test lives in [`tests/tester.py`](./tests/tester.py).
