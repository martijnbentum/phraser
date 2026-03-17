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

### Install with `pip`

```bash
pip install .
```

### Install with `uv pip`

```bash
uv pip install .
```

### Editable install

With `pip`:

```bash
pip install -e .
```

With `uv pip`:

```bash
uv pip install -e .
```

## Git-based dependencies

This project has required Git-based runtime dependencies:

- `ssh-audio-play`
- `webmaus`
- `frame`

They are declared in [`pyproject.toml`](./pyproject.toml). The current package
configuration uses `git+ssh` URLs, so installation requires:

- Git to be installed
- SSH access to GitHub to be configured
- permission to access the referenced repositories

## Example

### Load the default cache

```python
from phraser import load_cache

load_cache()
```

### Access objects from the package API

```python
from phraser import Audio, Phrase, Word, Syllable, Phone, Speaker

audio = Audio(filename="example.wav", save=False)
speaker = Speaker(name="spk1", dataset="demo", save=False)

phrase = Phrase(
    label="hello world",
    start=0,
    end=1200,
    save=False,
)

word = Word(
    label="hello",
    start=0,
    end=500,
    save=False,
)

syllable = Syllable(
    label="he",
    start=0,
    end=250,
    save=False,
)

phone = Phone(
    label="h",
    start=0,
    end=100,
    save=False,
)
```

### Link objects together

```python
from phraser import Audio, Phrase, Word

audio = Audio(filename="example.wav", save=False)
phrase = Phrase(label="hello world", start=0, end=1200, save=False)
word = Word(label="hello", start=0, end=500, save=False)

phrase.add_audio(audio, update_database=False)
word.add_parent(phrase, update_database=False)
```

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
