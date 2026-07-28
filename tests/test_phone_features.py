import types
import unittest

from phone_mapper.cgn import cgn_to_ipa

from phraser import phone_features
from phraser.segment import Phone
from phraser.phone_types import PHONE_TYPES


class TestLoadIpaFeatures(unittest.TestCase):
    def test_loads_mapping(self):
        data = phone_features.load_ipa_features()
        self.assertIsInstance(data, dict)
        self.assertGreater(len(data), 0)

    def test_cached_single_instance(self):
        self.assertIs(phone_features.load_ipa_features(),
                      phone_features.load_ipa_features())

    def test_covers_phone_types(self):
        '''Every vowel/consonant in PHONE_TYPES must have feature data.'''
        data = phone_features.load_ipa_features()
        need = {k for k, v in PHONE_TYPES.items()
                if v in ('vowel', 'consonant')}
        self.assertTrue(need.issubset(set(data)), need - set(data))

    def test_type_agrees_with_phone_types(self):
        data = phone_features.load_ipa_features()
        for label, kind in PHONE_TYPES.items():
            if kind in ('vowel', 'consonant') and label in data:
                self.assertEqual(data[label]['type'], kind, label)

    def test_covers_cgn_to_ipa_values(self):
        data = phone_features.load_ipa_features()
        mapped = set(cgn_to_ipa.values())
        covered = mapped.issubset(data)
        missing = mapped - set(data)
        self.assertTrue(covered, missing)

    def test_cgn_to_ipa_values_have_matching_phone_types(self):
        data = phone_features.load_ipa_features()
        for label in set(cgn_to_ipa.values()):
            phone_type = PHONE_TYPES.get(label)
            self.assertIn(phone_type, ('vowel', 'consonant'), label)
            self.assertEqual(PHONE_TYPES[label], data[label]['type'], label)

    def test_entries_have_feature_matrix(self):
        for label, info in phone_features.load_ipa_features().items():
            self.assertIn('type', info, label)
            self.assertIn('features', info, label)
            for name, value in info['features'].items():
                self.assertIn(value, ('+', '-', '0'), (label, name, value))


class TestGetPhoneFeatures(unittest.TestCase):
    def test_known_symbol(self):
        info = phone_features.get_phone_features('p')
        self.assertEqual(info['type'], 'consonant')
        self.assertEqual(info['place'], 'bilabial')
        self.assertEqual(info['features']['voice'], '-')

    def test_strident_distinguishes_s_and_sh(self):
        s = phone_features.get_phone_features('s')['features']
        sh = phone_features.get_phone_features('ʃ')['features']
        self.assertNotEqual(s['anterior'], sh['anterior'])
        self.assertNotEqual(s['distributed'], sh['distributed'])

    def test_unknown_symbol_returns_none(self):
        self.assertIsNone(phone_features.get_phone_features(''))
        self.assertIsNone(phone_features.get_phone_features('(..)'))
        self.assertIsNone(phone_features.get_phone_features('nope'))

    def test_cgn_specific_consonant_features(self):
        info = phone_features.get_phone_features('g')
        self.assertEqual(info['type'], 'consonant')
        self.assertEqual(info['place'], 'velar')
        self.assertEqual(info['manner'], 'plosive')
        self.assertEqual(info['voicing'], 'voiced')

    def test_cgn_specific_vowel_features(self):
        central = phone_features.get_phone_features('ʉ')
        self.assertEqual(central['backness'], 'central')
        self.assertEqual(central['rounding'], 'rounded')
        for label in ('iː', 'uː', 'yː', 'ɒː', 'ɑ̃ː', 'ɒ̃ː'):
            info = phone_features.get_phone_features(label)
            self.assertEqual(info['length'], 'long', label)
            self.assertEqual(info['features']['long'], '+', label)
        for label in ('œ̃', 'æ̃', 'ɑ̃ː', 'ɒ̃ː'):
            info = phone_features.get_phone_features(label)
            self.assertEqual(info['nasality'], 'nasal', label)
            self.assertEqual(info['features']['nasal'], '+', label)


class TestFeatureVector(unittest.TestCase):
    def test_length_matches_feature_order(self):
        v = phone_features.get_feature_vector('p')
        self.assertEqual(len(v), len(phone_features.FEATURE_ORDER))

    def test_values_are_numeric(self):
        for value in phone_features.get_feature_vector('p'):
            self.assertIn(value, (-1, 0, 1))

    def test_positionally_aligned_with_names(self):
        v = phone_features.get_feature_vector('p')
        named = dict(zip(phone_features.FEATURE_ORDER, v))
        self.assertEqual(named['voice'], -1)
        self.assertEqual(named['consonantal'], 1)

    def test_unknown_label_is_none(self):
        self.assertIsNone(phone_features.get_feature_vector(''))
        self.assertIsNone(phone_features.get_feature_vector('(..)'))
        self.assertIsNone(phone_features.get_feature_vector('nope'))

    def test_returns_tuple_and_is_cached(self):
        self.assertIsInstance(phone_features.get_feature_vector('p'), tuple)
        self.assertIs(phone_features.get_feature_vector('p'),
                      phone_features.get_feature_vector('p'))

    def test_feature_order_has_no_stress(self):
        self.assertNotIn('stress', phone_features.FEATURE_ORDER)

    def test_feature_order_matches_json_keys(self):
        '''Every entry's feature keys must equal FEATURE_ORDER exactly, or
        get_feature_vector would KeyError / misalign for that symbol.'''
        expected = set(phone_features.FEATURE_ORDER)
        self.assertEqual(len(expected), len(phone_features.FEATURE_ORDER),
                         'FEATURE_ORDER has duplicate names')
        for label, info in phone_features.load_ipa_features().items():
            self.assertEqual(set(info['features']), expected, label)


class TestPhoneProperty(unittest.TestCase):
    def test_features_not_persisted_metadata(self):
        self.assertNotIn('linguistic_features', Phone.METADATA_FIELDS)

    def test_property_resolves_by_label(self):
        phone = types.SimpleNamespace(label='aː')
        info = Phone.linguistic_features.fget(phone)
        self.assertEqual(info['type'], 'vowel')
        self.assertEqual(info['length'], 'long')

    def test_property_none_for_unknown_label(self):
        phone = types.SimpleNamespace(label='')
        self.assertIsNone(Phone.linguistic_features.fget(phone))

    def test_type_property(self):
        consonant = types.SimpleNamespace(label='p')
        vowel = types.SimpleNamespace(label='aː')
        unknown = types.SimpleNamespace(label='')
        self.assertEqual(Phone.type.fget(consonant), 'consonant')
        self.assertEqual(Phone.type.fget(vowel), 'vowel')
        self.assertIsNone(Phone.type.fget(unknown))

    def test_linguistic_features_vector_property(self):
        phone = types.SimpleNamespace(label='p')
        self.assertEqual(Phone.linguistic_features_vector.fget(phone),
                         phone_features.get_feature_vector('p'))

    def test_linguistic_features_names_property(self):
        phone = types.SimpleNamespace(label='p')
        self.assertEqual(Phone.linguistic_features_names.fget(phone),
                         phone_features.FEATURE_ORDER)


if __name__ == '__main__':
    unittest.main()
