'''Static IPA phone reference data: articulatory descriptors and a binary
distinctive-feature matrix per symbol.

The data lives in ``phraser/data/ipa_features.json`` and is loaded once
(cached) via importlib.resources, so it works for editable installs, wheels
and zipped imports alike. Regenerate the JSON with
``scripts/build_ipa_features.py``.

The binary distinctive-feature matrix was checked against the panphon
reference (ipa_all.csv): 620/630 shared-feature cells agree (~98.4%). The
remaining differences are deliberate analytic/convention choices, not
errors - e.g. laryngeal h (sonorant/consonantal/spread_glottis), the
labial-vs-round split on rounded vowels, and the backness/tenseness of
central and low vowels. Treat those contested features with care; the
``type`` field (vowel/consonant) and articulatory descriptors are reliable.
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
