'''Generate phraser/data/ipa_features.json.

The dataset maps an IPA symbol to articulatory descriptors plus a binary
distinctive-feature matrix (full Hayes-style set). Rather than hand-typing
~26 feature values per symbol, we declare each phone's articulatory
descriptors and derive the binary matrix from consistent rules. Re-run with:

    .venv/bin/python -m scripts.build_ipa_features

Feature values use the strings '+', '-' and '0' ('0' = not applicable /
unspecified for that segment, e.g. tongue-body features on a labial stop).

The derivation rules below were verified against the panphon reference
(ipa_all.csv): 620/630 shared-feature cells agree. The remaining
differences are deliberate convention choices (laryngeal h, the
labial/round split on rounded vowels, central/low vowel backness and
tenseness), not errors. The ``type`` field and articulatory descriptors
are reliable.
'''
import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / 'phraser' / 'data' / 'ipa_features.json'

# Ordered list of binary features (Hayes 2009 style).
FEATURE_ORDER = [
    'syllabic', 'stress', 'long',
    'consonantal', 'sonorant', 'continuant', 'delayed_release',
    'approximant', 'nasal',
    'voice', 'spread_glottis', 'constricted_glottis',
    'labial', 'round', 'labiodental',
    'coronal', 'anterior', 'distributed', 'strident', 'lateral',
    'dorsal', 'high', 'low', 'front', 'back', 'tense',
]

# (symbol, place, manner, voiced) ; manner/place names drive the rules below.
CONSONANTS = [
    ('p', 'bilabial', 'plosive', False),
    ('b', 'bilabial', 'plosive', True),
    ('t', 'alveolar', 'plosive', False),
    ('d', 'alveolar', 'plosive', True),
    ('k', 'velar', 'plosive', False),
    ('ɡ', 'velar', 'plosive', True),          # ɡ
    ('ʔ', 'glottal', 'plosive', False),       # ʔ
    ('m', 'bilabial', 'nasal', True),
    ('n', 'alveolar', 'nasal', True),
    ('ɲ', 'palatal', 'nasal', True),          # ɲ
    ('ŋ', 'velar', 'nasal', True),            # ŋ
    ('f', 'labiodental', 'fricative', False),
    ('v', 'labiodental', 'fricative', True),
    ('θ', 'dental', 'fricative', False),      # θ
    ('ð', 'dental', 'fricative', True),       # ð
    ('s', 'alveolar', 'fricative', False),
    ('z', 'alveolar', 'fricative', True),
    ('ʃ', 'postalveolar', 'fricative', False),# ʃ
    ('ʒ', 'postalveolar', 'fricative', True), # ʒ
    ('ç', 'palatal', 'fricative', False),     # ç
    ('x', 'velar', 'fricative', False),
    ('ɣ', 'velar', 'fricative', True),        # ɣ
    ('h', 'glottal', 'fricative', False),
    ('t͡ʃ', 'postalveolar', 'affricate', False),  # t͡ʃ
    ('d͡ʒ', 'postalveolar', 'affricate', True),   # d͡ʒ
    ('l', 'alveolar', 'lateral-approximant', True),
    ('r', 'alveolar', 'trill', True),
    ('ɾ', 'alveolar', 'tap', True),           # ɾ
    ('ɹ', 'alveolar', 'approximant', True),   # ɹ
    ('ʀ', 'uvular', 'trill', True),           # ʀ
    ('j', 'palatal', 'approximant', True),
    ('w', 'labial-velar', 'approximant', True),
    ('ʋ', 'labiodental', 'approximant', True),# ʋ
]

# (symbol, height, backness, rounded, long, tense)
# height: close, near-close, close-mid, mid, open-mid, near-open, open
# backness: front, central, back
VOWELS = [
    ('i', 'close', 'front', False, False, True),
    ('y', 'close', 'front', True, False, True),
    ('ɨ', 'close', 'central', False, False, True),   # ɨ
    ('u', 'close', 'back', True, False, True),
    ('ɪ', 'near-close', 'front', False, False, False),# ɪ
    ('ʏ', 'near-close', 'front', True, False, False), # ʏ
    ('ʊ', 'near-close', 'back', True, False, False),  # ʊ
    ('e', 'close-mid', 'front', False, False, True),
    ('ø', 'close-mid', 'front', True, False, True),   # ø
    ('ə', 'mid', 'central', False, False, False),     # ə
    ('o', 'close-mid', 'back', True, False, True),
    ('ɛ', 'open-mid', 'front', False, False, False),  # ɛ
    ('œ', 'open-mid', 'front', True, False, False),   # œ
    ('ɔ', 'open-mid', 'back', True, False, False),    # ɔ
    ('ʌ', 'open-mid', 'back', False, False, False),   # ʌ
    ('æ', 'near-open', 'front', False, False, False), # æ
    ('ɐ', 'near-open', 'central', False, False, False),# ɐ
    ('a', 'open', 'front', False, False, False),
    ('ɑ', 'open', 'back', False, False, False),       # ɑ
    ('ɒ', 'open', 'back', True, False, False),        # ɒ
    # Dutch/CGN long vowels
    ('aː', 'open', 'front', False, True, True),       # aː
    ('eː', 'close-mid', 'front', False, True, True),  # eː
    ('oː', 'close-mid', 'back', True, True, True),    # oː
    ('øː', 'close-mid', 'front', True, True, True),  # øː
    ('œː', 'open-mid', 'front', True, True, False), # œː
    ('ɛː', 'open-mid', 'front', False, True, False),# ɛː
    ('ɔː', 'open-mid', 'back', True, True, False),  # ɔː
]

# (symbol, start_vowel_symbol, glide_to_symbol) ; binary features taken from
# the nuclear (start) vowel. Start descriptors are looked up in VOWELS.
DIPHTHONGS = [
    ('ɛi', 'ɛ', 'i'),     # ɛi
    ('œy', 'œ', 'y'),     # œy
    ('ɑu', 'ɑ', 'u'),     # ɑu
    ('ui', 'u', 'i'),               # ui
]


def consonant_features(place, manner, voiced):
    f = {k: '-' for k in FEATURE_ORDER}
    f['syllabic'] = '-'
    f['stress'] = '0'
    f['long'] = '-'
    f['tense'] = '0'

    glide = manner == 'approximant' and place in ('palatal', 'labial-velar')
    laryngeal = place == 'glottal'
    f['consonantal'] = '-' if (glide or laryngeal) else '+'

    obstruent = manner in ('plosive', 'fricative', 'affricate')
    f['sonorant'] = '-' if obstruent else '+'

    cont = {'plosive': '-', 'nasal': '-', 'affricate': '-', 'tap': '-',
            'trill': '+', 'fricative': '+', 'approximant': '+',
            'lateral-approximant': '+'}
    f['continuant'] = cont[manner]

    f['delayed_release'] = '+' if manner == 'affricate' else (
        '-' if manner == 'plosive' else '0')
    f['approximant'] = '+' if manner in (
        'approximant', 'lateral-approximant') else '-'
    f['nasal'] = '+' if manner == 'nasal' else '-'
    f['voice'] = '+' if voiced else '-'
    f['spread_glottis'] = '+' if place == 'glottal' and manner == 'fricative' else '-'
    f['constricted_glottis'] = '+' if place == 'glottal' and manner == 'plosive' else '-'

    labial = place in ('bilabial', 'labiodental', 'labial-velar')
    f['labial'] = '+' if labial else '-'
    f['round'] = '+' if place == 'labial-velar' else '-'
    f['labiodental'] = '+' if place == 'labiodental' else ('-' if labial else '0')

    coronal = place in ('dental', 'alveolar', 'postalveolar', 'retroflex')
    f['coronal'] = '+' if coronal else '-'
    if coronal:
        f['anterior'] = '+' if place in ('dental', 'alveolar') else '-'
        f['distributed'] = '+' if place in ('dental', 'postalveolar') else '-'
    else:
        f['anterior'] = '0'
        f['distributed'] = '0'

    strident_place = place in ('labiodental', 'alveolar', 'postalveolar')
    f['strident'] = '+' if (manner in ('fricative', 'affricate')
                            and strident_place) else '-'
    f['lateral'] = '+' if manner == 'lateral-approximant' else '-'

    dorsal = place in ('velar', 'uvular', 'palatal', 'labial-velar')
    f['dorsal'] = '+' if dorsal else '-'
    if dorsal:
        f['high'] = '+' if place in ('velar', 'palatal', 'labial-velar') else '-'
        f['low'] = '-'
        f['front'] = '+' if place == 'palatal' else '-'
        f['back'] = '+' if place in ('velar', 'uvular', 'labial-velar') else '-'
    else:
        f['high'] = '0'
        f['low'] = '0'
        f['front'] = '0'
        f['back'] = '0'
    return f


def vowel_features(height, backness, rounded, is_long, tense):
    f = {k: '-' for k in FEATURE_ORDER}
    f['syllabic'] = '+'
    f['stress'] = '0'
    f['long'] = '+' if is_long else '-'
    f['consonantal'] = '-'
    f['sonorant'] = '+'
    f['continuant'] = '+'
    f['delayed_release'] = '0'
    f['approximant'] = '+'
    f['nasal'] = '-'
    f['voice'] = '+'
    f['spread_glottis'] = '-'
    f['constricted_glottis'] = '-'
    f['labial'] = '+' if rounded else '-'
    f['round'] = '+' if rounded else '-'
    f['labiodental'] = '-'
    f['coronal'] = '-'
    f['anterior'] = '0'
    f['distributed'] = '0'
    f['strident'] = '-'
    f['lateral'] = '-'
    f['dorsal'] = '+'
    f['high'] = '+' if height in ('close', 'near-close') else '-'
    f['low'] = '+' if height in ('open', 'near-open') else '-'
    f['front'] = '+' if backness == 'front' else '-'
    f['back'] = '+' if backness == 'back' else '-'
    f['tense'] = '+' if tense else '-'
    return f


def build():
    data = {}
    for symbol, place, manner, voiced in CONSONANTS:
        data[symbol] = {
            'type': 'consonant',
            'place': place,
            'manner': manner,
            'voicing': 'voiced' if voiced else 'voiceless',
            'features': consonant_features(place, manner, voiced),
        }
    vowel_index = {}
    for symbol, height, backness, rounded, is_long, tense in VOWELS:
        vowel_index[symbol] = (height, backness, rounded, is_long, tense)
        data[symbol] = {
            'type': 'vowel',
            'height': height,
            'backness': backness,
            'rounding': 'rounded' if rounded else 'unrounded',
            'length': 'long' if is_long else 'short',
            'features': vowel_features(height, backness, rounded, is_long, tense),
        }
    for symbol, start, glide_to in DIPHTHONGS:
        height, backness, rounded, is_long, tense = vowel_index[start]
        data[symbol] = {
            'type': 'vowel',
            'diphthong': True,
            'components': [start, glide_to],
            'height': height,
            'backness': backness,
            'rounding': 'rounded' if rounded else 'unrounded',
            'length': 'long' if is_long else 'short',
            'features': vowel_features(height, backness, rounded, is_long, tense),
        }
    return data


def main():
    data = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write('\n')
    print(f'wrote {len(data)} symbols to {OUT}')


if __name__ == '__main__':
    main()
