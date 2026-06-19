'''Static IPA phone reference data: articulatory descriptors and a binary
distinctive-feature matrix per symbol.

The data lives in ``phraser/data/ipa_features.json`` and is loaded once
(cached) via importlib.resources, so it works for editable installs, wheels
and zipped imports alike. Regenerate the JSON with
``scripts/build_ipa_features.py``.

WORK IN PROGRESS: the binary distinctive-feature matrix is derived from
articulatory descriptors by rule and has NOT been fully verified. Several
values reflect analytic choices (e.g. palatals as +dorsal/-coronal,
glottals as -consonantal, trills +continuant vs taps -continuant). Verify
the relevant features against a reference before relying on them in
analysis. The ``type`` field (vowel/consonant) and articulatory descriptors
are reliable.
'''
import json
from functools import lru_cache
from importlib import resources

DATA_PACKAGE = 'phraser.data'
DATA_FILE = 'ipa_features.json'


@lru_cache(maxsize=1)
def load_ipa_features():
    '''Return the full {symbol: info} mapping (loaded and cached once).'''
    source = resources.files(DATA_PACKAGE).joinpath(DATA_FILE)
    with source.open(encoding='utf-8') as f:
        return json.load(f)


def get_phone_features(label):
    '''Return the reference info dict for an IPA symbol, or None if the
    symbol is unknown (e.g. '' or '(..)' placeholders).'''
    return load_ipa_features().get(label)
