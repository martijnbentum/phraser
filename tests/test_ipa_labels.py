import json
import unittest
from importlib import resources

from phraser.phonology.ipa_labels import normalize_ipa_label
from phraser.phonology.ipa_labels import normalize_ipa_sequence


class TestNormalizeIpaLabel(unittest.TestCase):
    def test_affricates_get_tie_bar(self):
        cases = {'tʃ': 't͡ʃ', 'dʒ': 'd͡ʒ', 'ts': 't͡s', 'dz': 'd͡z',
            'tsʲ': 't͡sʲ', 'dzʲ': 'd͡zʲ'}
        for label, expected in cases.items():
            self.assertEqual(normalize_ipa_label(label), expected)

    def test_geminate_affricate_length_moves_to_end(self):
        self.assertEqual(normalize_ipa_label('tːʃ'), 't͡ʃː')
        self.assertEqual(normalize_ipa_label('dːz'), 'd͡zː')

    def test_palatalisation_before_length(self):
        self.assertEqual(normalize_ipa_label('tːʲ'), 'tʲː')

    def test_nfc(self):
        self.assertEqual(normalize_ipa_label('ã'), 'ã')
        self.assertEqual(normalize_ipa_label('ç'), 'ç')

    def test_perispomeni_becomes_tilde(self):
        self.assertEqual(normalize_ipa_label('ə͂'), 'ə̃')

    def test_plain_labels_unchanged(self):
        for label in ('t', 's', 'ʃː', 'aɪ', '(...)', ''):
            self.assertEqual(normalize_ipa_label(label), label)

    def test_sequence(self):
        self.assertEqual(normalize_ipa_sequence('tʃ a'), 't͡ʃ a')

    def test_dutch_feature_keys_unchanged(self):
        source = resources.files('phraser.phonology.data')
        source = source.joinpath('ipa_features.json')
        keys = json.loads(source.read_text(encoding='utf-8'))
        changed = [key for key in keys if normalize_ipa_label(key) != key]
        self.assertEqual(changed, [])

    def test_language_feature_keys_normalised(self):
        source = resources.files('phraser.phonology.data')
        for path in source.iterdir():
            if not path.name.startswith('ipa_features_'): continue
            keys = json.loads(path.read_text(encoding='utf-8'))
            changed = [k for k in keys if normalize_ipa_label(k) != k]
            self.assertEqual(changed, [], path.name)

    def test_phone_mapper_maus_labels_normalised(self):
        from phone_mapper import maus
        for language, table in maus.maus_sampa_to_ipa.items():
            changed = [v for v in table.values() if normalize_ipa_label(v) != v]
            self.assertEqual(changed, [], language)


if __name__ == '__main__':
    unittest.main()
