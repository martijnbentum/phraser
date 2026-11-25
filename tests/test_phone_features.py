import types
import unittest

from phone_mapper.cgn import cgn_to_ipa

from phraser.models import Phone
from phraser.phonology import phone_features
from phraser.phonology.phone_types import (OTHER_PHONE_LABELS, PHONE_TYPES,
    get_phone_types)


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
        phone = types.SimpleNamespace(label='aː', language='nld')
        info = Phone.linguistic_features.fget(phone)
        self.assertEqual(info['type'], 'vowel')
        self.assertEqual(info['length'], 'long')

    def test_property_none_for_unknown_label(self):
        phone = types.SimpleNamespace(label='', language='nld')
        self.assertIsNone(Phone.linguistic_features.fget(phone))

    def test_type_property(self):
        consonant = types.SimpleNamespace(label='p', language='nld')
        vowel = types.SimpleNamespace(label='aː', language='nld')
        unknown = types.SimpleNamespace(label='', language='nld')
        self.assertEqual(Phone.type.fget(consonant), 'consonant')
        self.assertEqual(Phone.type.fget(vowel), 'vowel')
        self.assertIsNone(Phone.type.fget(unknown))

    def test_linguistic_features_vector_property(self):
        phone = types.SimpleNamespace(label='p', language='nld')
        self.assertEqual(Phone.linguistic_features_vector.fget(phone),
                         phone_features.get_feature_vector('p'))

    def test_linguistic_features_names_property(self):
        phone = types.SimpleNamespace(label='p', language='nld')
        self.assertEqual(Phone.linguistic_features_names.fget(phone),
                         phone_features.FEATURE_ORDER)

    def test_language_selects_table(self):
        '''ɕ is Polish only; the same Phone property finds it there.'''
        dutch = types.SimpleNamespace(label='ɕ', language='nld')
        polish = types.SimpleNamespace(label='ɕ', language='pol')
        self.assertIsNone(Phone.type.fget(dutch))
        self.assertEqual(Phone.type.fget(polish), 'consonant')

    def test_language_from_audio(self):
        audio = types.SimpleNamespace(language='pol')
        phone = types.SimpleNamespace(audio=audio)
        self.assertEqual(Phone.language.fget(phone), 'pol')

    def test_language_defaults_to_dutch(self):
        no_language = types.SimpleNamespace(audio=types.SimpleNamespace(
            language=''))
        no_audio = types.SimpleNamespace(audio=None)
        self.assertEqual(Phone.language.fget(no_language), 'nld')
        self.assertEqual(Phone.language.fget(no_audio), 'nld')


class TestLanguageTables(unittest.TestCase):
    def test_default_is_dutch_file(self):
        self.assertIs(phone_features.load_ipa_features(),
            phone_features.load_ipa_features('nld'))
        self.assertIs(phone_features.load_ipa_features(''),
            phone_features.load_ipa_features('nld'))
        self.assertEqual(phone_features.data_file('nld'), 'ipa_features.json')

    def test_every_language_loads(self):
        for language in phone_features.LANGUAGES:
            data = phone_features.load_ipa_features(language)
            self.assertGreater(len(data), 0, language)

    def test_unknown_language_raises(self):
        with self.assertRaises(ValueError):
            phone_features.load_ipa_features('tur')

    def test_phone_types_per_language(self):
        for language in phone_features.LANGUAGES:
            types_ = get_phone_types(language)
            data = phone_features.load_ipa_features(language)
            for label, info in data.items():
                self.assertEqual(types_[label], info['type'], label)
            for label in OTHER_PHONE_LABELS:
                self.assertEqual(types_[label], 'other')

    def test_dutch_phone_types_unchanged(self):
        self.assertIs(PHONE_TYPES, get_phone_types('nld'))
        self.assertIs(PHONE_TYPES, get_phone_types(None))

    def test_feature_vector_per_language(self):
        self.assertIsNone(phone_features.get_feature_vector('ɕ', 'nld'))
        self.assertIsNotNone(phone_features.get_feature_vector('ɕ', 'pol'))


if __name__ == '__main__':
    unittest.main()
