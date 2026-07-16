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
from phraser import textgrid_loader

store = Store("/path/to/lmdb")
items = textgrid_loader.textgrid_filename_to_database_objects(
    "example.TextGrid",
    audio=audio,
    speaker=speaker,
    store=store,
)

textgrid_loader.save_textgrid_items(items, store=store, existing="append")
store.refresh_query_roots()
```

`save_textgrid_items()` supports `existing="append"`, `"add_missing"`,
`"replace"`, and `"upsert"`. Policies other than `"append"` run an existence
check using phrase identity `(audio_id, speaker_id, start)` and require an
existing `Audio` object.

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

### Access phone linguistic features

A `Phone` exposes static IPA reference data, derived purely from its label
(no audio, distinct from the neural `embedding(...)` above). The data lives
in `phraser/data/ipa_features.json` and is regenerated with
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
`FEATURE_ORDER`) live in `phraser.phone_features`.

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
