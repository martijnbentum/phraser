from . import phone_features


OTHER_PHONE_LABELS = ('', '(..)')


def load_phone_types():
    '''Return {label: 'vowel'|'consonant'} from ipa_features.json, plus the
    non-speech placeholder labels mapped to 'other'.'''
    phone_types = {}
    for label, info in phone_features.load_ipa_features().items():
        phone_types[label] = info['type']
    for label in OTHER_PHONE_LABELS:
        phone_types[label] = 'other'
    return phone_types


PHONE_TYPES = load_phone_types()
