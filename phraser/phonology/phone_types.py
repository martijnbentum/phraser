from functools import lru_cache

from . import phone_features


OTHER_PHONE_LABELS = ('', '(..)')


def load_phone_types(language=None):
    '''Return {label: 'vowel'|'consonant'} from the language's IPA feature
    file, plus the non-speech placeholder labels mapped to 'other'.
    '' or None means Dutch (see phone_features.resolve_language).'''
    return dict(get_phone_types(language))


def get_phone_types(language=None):
    '''Cached {label: type} mapping for a language; do not mutate it.'''
    return _phone_types(phone_features.resolve_language(language))


@lru_cache(maxsize=None)
def _phone_types(language):
    phone_types = {}
    for label, info in phone_features.load_ipa_features(language).items():
        phone_types[label] = info['type']
    for label in OTHER_PHONE_LABELS:
        phone_types[label] = 'other'
    return phone_types


# Dutch table, kept for callers that only handle Dutch (CGN, syllabify).
PHONE_TYPES = get_phone_types(phone_features.DEFAULT_LANGUAGE)
