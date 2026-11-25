'''Static IPA phone reference data: articulatory descriptors and a binary
distinctive-feature matrix per symbol.

The data lives in ``phraser/phonology/data/``: ``ipa_features.json`` for
Dutch and ``ipa_features_<iso>.json`` for the other languages. Each file is
loaded once (cached) via importlib.resources, so it works for editable
installs, wheels and zipped imports alike. Regenerate the JSON with
``scripts/build_ipa_features.py``.

Every lookup takes a language (ISO 639-3, as stored in ``Audio.language``).
An empty or None language means Dutch, so CGN stores and callers that pass
no language keep their behaviour. An unknown language raises ValueError
rather than silently falling back to the Dutch table.

The binary distinctive-feature matrix was checked against the panphon
reference (ipa_all.csv): 620/630 shared-feature cells agree (~98.4%). The
remaining differences are deliberate analytic/convention choices, not
errors - e.g. laryngeal h (sonorant/consonantal/spread_glottis), the
labial-vs-round split on rounded vowels, and the backness/tenseness of
central and low vowels. Treat those contested features with care; the
``type`` field (vowel/consonant) and articulatory descriptors are reliable.

(The ``stress`` feature was since removed from the matrix - it is
suprasegmental and lives on the syllable, not the label. It was a constant
``0`` and not part of the panphon comparison, so the figure above is
unaffected.)
'''
import json
from functools import lru_cache
from importlib import resources

DATA_PACKAGE = 'phraser.phonology.data'
DATA_FILE = 'ipa_features.json'
DEFAULT_LANGUAGE = 'nld'
LANGUAGES = ('nld', 'eng', 'fra', 'deu', 'hun', 'ita', 'pol', 'spa')

# Canonical, positional order of the binary distinctive features (Hayes 2009
# style). This is the source of truth for feature-vector layout: position i
# always means FEATURE_ORDER[i], so it must not be reordered. The build
# script imports this same list when generating the JSON.
FEATURE_ORDER = (
    'syllabic', 'long',
    'consonantal', 'sonorant', 'continuant', 'delayed_release',
    'approximant', 'nasal',
    'voice', 'spread_glottis', 'constricted_glottis',
    'labial', 'round', 'labiodental',
    'coronal', 'anterior', 'distributed', 'strident', 'lateral',
    'dorsal', 'high', 'low', 'front', 'back', 'tense',
)

# Numeric mapping for the feature-vector form. '0' (not applicable) maps to 0.
_VALUE_MAP = {'+': 1, '-': -1, '0': 0}


def resolve_language(language=None):
    '''Return the ISO code whose feature table applies: '' or None -> Dutch.
    Raises ValueError for a language without a feature file.'''
    if not language: return DEFAULT_LANGUAGE
    if language in LANGUAGES: return language
    m = f'no IPA feature table for language {language!r}; '
    m += f'known: {", ".join(LANGUAGES)}'
    raise ValueError(m)


def data_file(language=None):
    '''Return the feature file name for a language.'''
    language = resolve_language(language)
    if language == DEFAULT_LANGUAGE: return DATA_FILE
    return f'ipa_features_{language}.json'


def load_ipa_features(language=None):
    '''Return the full {symbol: info} mapping for a language (each file
    is loaded and cached once).'''
    return _load(resolve_language(language))


@lru_cache(maxsize=None)
def _load(language):
    source = resources.files(DATA_PACKAGE).joinpath(data_file(language))
    with source.open(encoding='utf-8') as f:
        return json.load(f)


def get_phone_features(label, language=None):
    '''Return the reference info dict for an IPA symbol, or None if the
    symbol is unknown (e.g. '' or '(..)' placeholders).'''
    return load_ipa_features(language).get(label)


def get_feature_vector(label, language=None):
    '''Return the binary distinctive-feature matrix (i.e. the ``features``
    sub-dict, not the full reference info) as a numeric tuple in
    FEATURE_ORDER, with +1/-1/0 (0 = not applicable). Returns None for
    unknown labels, mirroring get_phone_features.'''
    return _feature_vector(label, resolve_language(language))


@lru_cache(maxsize=None)
def _feature_vector(label, language):
    info = get_phone_features(label, language)
    if info is None:
        return None
    features = info['features']
    return tuple(_VALUE_MAP[features[name]] for name in FEATURE_ORDER)
