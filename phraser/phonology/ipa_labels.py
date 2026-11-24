'''Phraser IPA label notation, as a reference check.

phone_mapper.maus already returns labels in this notation, so the Common
Voice import is a plain table lookup. This module states the convention in
phraser and tests check that phone_mapper output satisfies it
(normalize_ipa_label(label) == label), so equal sounds get equal strings
across languages:

- U+0342 (Greek perispomeni, a typo in the BAS English table for nasal
  schwa) becomes U+0303 (combining tilde).
- Unicode NFC: base letter plus combining mark (a + U+0303) and the
  precomposed letter (U+00E3) become the same string, composed where a
  single code point exists (ã, ç) and decomposed otherwise (ɛ̃, ə̃). The
  Dutch ipa_features.json keys are already NFC (ç is U+00E7).
- Affricates get a tie bar: tʃ -> t͡ʃ, dʒ -> d͡ʒ, ts -> t͡s, dz -> d͡z.
  MAUS writes gemination on the stop (tːʃ); it moves behind the affricate
  (t͡ʃː).
- Palatalisation precedes length: tːʲ -> tʲː.

The Dutch ipa_features.json keys are already in this form.
'''
import re
import unicodedata

TIE_BAR = '͡'
PERISPOMENI = '͂'
COMBINING_TILDE = '̃'
LONG = 'ː'
PALATALIZED = 'ʲ'

AFFRICATE = re.compile(r'^(t)(ː?)([ʃs])(ʲ?)(ː?)$|^(d)(ː?)([ʒz])(ʲ?)(ː?)$')


def normalize_ipa_label(label):
    '''Return one phone label in phraser IPA notation (see module doc).'''
    label = label.replace(PERISPOMENI, COMBINING_TILDE)
    label = unicodedata.normalize('NFC', label)
    label = add_affricate_tie_bar(label)
    return label.replace(LONG + PALATALIZED, PALATALIZED + LONG)


def add_affricate_tie_bar(label):
    '''Join a homorganic stop + sibilant label with a tie bar.'''
    match = AFFRICATE.match(label)
    if match is None: return label
    groups = [g for g in match.groups() if g is not None]
    stop, stop_long, fricative, palatalized, long = groups
    length = LONG if stop_long or long else ''
    return stop + TIE_BAR + fricative + palatalized + length


def normalize_ipa_sequence(text):
    '''Normalise a space-separated label sequence (MAS, KAN-MAU tiers).'''
    return ' '.join(normalize_ipa_label(label) for label in text.split(' '))
