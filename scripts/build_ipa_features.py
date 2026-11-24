'''Generate phraser/phonology/data/ipa_features.json (Dutch) and
ipa_features_<iso>.json per language from scripts/ipa_declarations/<iso>.tsv.

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

Per-language files: each TSV row declares one symbol that occurs in that
language's data (descriptors only); the binary matrix is derived with the
same rules. Consonant modifiers: length long sets ``long``, palatalized adds
a palatal secondary articulation (dorsal, high, front), syllabic sets
``syllabic``. Diphthongs take features from their first component.
Non-syllabic vowels (German ɐ̯) set ``syllabic`` to '-' and keep type vowel
so they join the nucleus. R-coloured vowels (ɝ ɚ) get the descriptor
``rhotic``; the binary matrix
has no rhotic feature, so their vectors equal those of ɜ / ə. The
TSVs were drafted once by rules from MAUS label counts and are now the
reviewed source of truth; edit them per line.

The ``stress`` feature was since removed from the matrix - it is
suprasegmental and lives on the syllable, not the label. It was a constant
``0`` and not part of the panphon comparison, so the figure above is
unaffected.
'''
import json
from pathlib import Path

from phraser.phonology.phone_features import FEATURE_ORDER

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'phraser' / 'phonology' / 'data'
OUT = DATA / 'ipa_features.json'
DECLARATIONS = ROOT / 'scripts' / 'ipa_declarations'

# (symbol, place, manner, voiced) ; manner/place names drive the rules below.
CONSONANTS = [
    ('p', 'bilabial', 'plosive', False),
    ('b', 'bilabial', 'plosive', True),
    ('t', 'alveolar', 'plosive', False),
    ('d', 'alveolar', 'plosive', True),
    ('k', 'velar', 'plosive', False),
    ('g', 'velar', 'plosive', True),
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

# (symbol, height, backness, rounded, long, tense, nasal)
# height: close, near-close, close-mid, mid, open-mid, near-open, open
# backness: front, central, back
VOWELS = [
    ('i', 'close', 'front', False, False, True, False),
    ('y', 'close', 'front', True, False, True, False),
    ('ɨ', 'close', 'central', False, False, True, False),
    ('ʉ', 'close', 'central', True, False, False, False),
    ('u', 'close', 'back', True, False, True, False),
    ('ɪ', 'near-close', 'front', False, False, False, False),
    ('ʏ', 'near-close', 'front', True, False, False, False),
    ('ʊ', 'near-close', 'back', True, False, False, False),
    ('e', 'close-mid', 'front', False, False, True, False),
    ('ø', 'close-mid', 'front', True, False, True, False),
    ('ə', 'mid', 'central', False, False, False, False),
    ('o', 'close-mid', 'back', True, False, True, False),
    ('ɛ', 'open-mid', 'front', False, False, False, False),
    ('œ', 'open-mid', 'front', True, False, False, False),
    ('ɔ', 'open-mid', 'back', True, False, False, False),
    ('ʌ', 'open-mid', 'back', False, False, False, False),
    ('æ', 'near-open', 'front', False, False, False, False),
    ('ɐ', 'near-open', 'central', False, False, False, False),
    ('a', 'open', 'front', False, False, False, False),
    ('ɑ', 'open', 'back', False, False, False, False),
    ('ɒ', 'open', 'back', True, False, False, False),
    # Dutch/CGN long vowels
    ('iː', 'close', 'front', False, True, True, False),
    ('yː', 'close', 'front', True, True, True, False),
    ('uː', 'close', 'back', True, True, True, False),
    ('aː', 'open', 'front', False, True, True, False),
    ('eː', 'close-mid', 'front', False, True, True, False),
    ('oː', 'close-mid', 'back', True, True, True, False),
    ('øː', 'close-mid', 'front', True, True, True, False),
    ('œː', 'open-mid', 'front', True, True, False, False),
    ('ɛː', 'open-mid', 'front', False, True, False, False),
    ('ɔː', 'open-mid', 'back', True, True, False, False),
    ('ɒː', 'open', 'back', True, True, False, False),
    # CGN nasal vowels
    ('œ̃', 'open-mid', 'front', True, False, False, True),
    ('æ̃', 'near-open', 'front', False, False, False, True),
    ('ɑ̃ː', 'open', 'back', False, True, False, True),
    ('ɒ̃ː', 'open', 'back', True, True, False, True),
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
    f['long'] = '-'
    f['tense'] = '0'

    glide = manner == 'approximant' and place in ('palatal', 'labial-velar',
        'labial-palatal')
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

    labial = place in ('bilabial', 'labiodental', 'labial-velar',
        'labial-palatal')
    f['labial'] = '+' if labial else '-'
    f['round'] = '+' if place in ('labial-velar', 'labial-palatal') else '-'
    f['labiodental'] = '+' if place == 'labiodental' else ('-' if labial else '0')

    coronal = place in ('dental', 'alveolar', 'postalveolar', 'retroflex',
        'alveolo-palatal')
    f['coronal'] = '+' if coronal else '-'
    if coronal:
        f['anterior'] = '+' if place in ('dental', 'alveolar') else '-'
        distributed = ('dental', 'postalveolar', 'alveolo-palatal')
        f['distributed'] = '+' if place in distributed else '-'
    else:
        f['anterior'] = '0'
        f['distributed'] = '0'

    strident_place = place in ('labiodental', 'alveolar', 'postalveolar',
        'alveolo-palatal')
    f['strident'] = '+' if (manner in ('fricative', 'affricate')
                            and strident_place) else '-'
    f['lateral'] = '+' if manner == 'lateral-approximant' else '-'

    dorsal = place in ('velar', 'uvular', 'palatal', 'labial-velar',
        'labial-palatal', 'alveolo-palatal')
    f['dorsal'] = '+' if dorsal else '-'
    if dorsal:
        high = ('velar', 'palatal', 'labial-velar', 'labial-palatal',
            'alveolo-palatal')
        f['high'] = '+' if place in high else '-'
        f['low'] = '-'
        front = ('palatal', 'labial-palatal', 'alveolo-palatal')
        f['front'] = '+' if place in front else '-'
        f['back'] = '+' if place in ('velar', 'uvular', 'labial-velar') else '-'
    else:
        f['high'] = '0'
        f['low'] = '0'
        f['front'] = '0'
        f['back'] = '0'
    return f


def vowel_features(height, backness, rounded, is_long, tense, nasal=False):
    f = {k: '-' for k in FEATURE_ORDER}
    f['syllabic'] = '+'
    f['long'] = '+' if is_long else '-'
    f['consonantal'] = '-'
    f['sonorant'] = '+'
    f['continuant'] = '+'
    f['delayed_release'] = '0'
    f['approximant'] = '+'
    f['nasal'] = '+' if nasal else '-'
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
    for symbol, height, backness, rounded, is_long, tense, nasal in VOWELS:
        vowel_index[symbol] = (height, backness, rounded, is_long, tense,
            nasal)
        data[symbol] = {
            'type': 'vowel',
            'height': height,
            'backness': backness,
            'rounding': 'rounded' if rounded else 'unrounded',
            'length': 'long' if is_long else 'short',
            'nasality': 'nasal' if nasal else 'oral',
            'features': vowel_features(height, backness, rounded, is_long,
                tense, nasal),
        }
    for symbol, start, glide_to in DIPHTHONGS:
        height, backness, rounded, is_long, tense, nasal = vowel_index[start]
        data[symbol] = {
            'type': 'vowel',
            'diphthong': True,
            'components': [start, glide_to],
            'height': height,
            'backness': backness,
            'rounding': 'rounded' if rounded else 'unrounded',
            'length': 'long' if is_long else 'short',
            'nasality': 'nasal' if nasal else 'oral',
            'features': vowel_features(height, backness, rounded, is_long,
                tense, nasal),
        }
    return data


def read_declarations(path):
    '''Return one dict per TSV row of a language declaration file.'''
    lines = path.read_text(encoding='utf-8').splitlines()
    header = lines[0].split('\t')
    rows = []
    for line in lines[1:]:
        values = line.split('\t')
        pairs = zip(header, values)
        rows.append(dict(pairs))
    return rows


def declared_consonant(row):
    '''Return the info dict for a consonant row, applying modifiers.'''
    voiced = row['voicing'] == 'voiced'
    f = consonant_features(row['place'], row['manner'], voiced)
    info = {'type': 'consonant', 'place': row['place'],
        'manner': row['manner'], 'voicing': row['voicing']}
    if row['length'] == 'long':
        f['long'] = '+'
        info['length'] = 'long'
    if row['palatalized'] == 'yes':
        f.update(dorsal='+', high='+', low='-', front='+', back='-')
        info['secondary'] = 'palatalized'
    if row['syllabic'] == 'yes':
        f['syllabic'] = '+'
        info['syllabic'] = True
    info['features'] = f
    return info


def declared_vowel(row):
    '''Return the info dict for a vowel or diphthong row.'''
    rounded = row['rounding'] == 'rounded'
    is_long = row['length'] == 'long'
    tense = row['tense'] == 'tense'
    nasal = row['nasality'] == 'nasal'
    info = {'type': 'vowel', 'height': row['height'],
        'backness': row['backness'], 'rounding': row['rounding'],
        'length': row['length'], 'nasality': row['nasality']}
    info['features'] = vowel_features(row['height'], row['backness'],
        rounded, is_long, tense, nasal)
    if row['components']:
        info['diphthong'] = True
        info['components'] = row['components'].split(' ')
    if row['rhotic'] == 'yes': info['rhotic'] = True
    if row['syllabic'] == 'no':
        info['features']['syllabic'] = '-'
        info['syllabic'] = False
    return info


def build_language(path):
    '''Return {symbol: info} for one language declaration file.'''
    data = {}
    for row in read_declarations(path):
        symbol = row['symbol']
        if row['type'] == 'consonant': info = declared_consonant(row)
        elif row['type'] == 'vowel': info = declared_vowel(row)
        else: raise ValueError(f'{path.name}: no type for {symbol}')
        data[symbol] = info
    return data


def write_json(data, path):
    '''Write sorted, indented JSON and report the symbol count.'''
    with path.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write('\n')
    print(f'wrote {len(data)} symbols to {path}')


def main():
    DATA.mkdir(parents=True, exist_ok=True)
    write_json(build(), OUT)
    for path in sorted(DECLARATIONS.glob('*.tsv')):
        out = DATA / f'ipa_features_{path.stem}.json'
        write_json(build_language(path), out)


if __name__ == '__main__':
    main()
