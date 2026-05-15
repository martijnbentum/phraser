import importlib.util
import types
import unittest
from pathlib import Path

MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'syllable_structure.py')


def load_module():
    spec = importlib.util.spec_from_file_location(
        'syllable_structure_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_phone(label):
    return types.SimpleNamespace(label=label)


m = load_module()


class TestCheckConsecutiveNumbers(unittest.TestCase):
    def test_empty(self):
        self.assertTrue(m.check_consecutive_numbers([]))

    def test_single(self):
        self.assertTrue(m.check_consecutive_numbers([5]))

    def test_consecutive(self):
        self.assertTrue(m.check_consecutive_numbers([1, 2, 3]))

    def test_consecutive_from_zero(self):
        self.assertTrue(m.check_consecutive_numbers([0, 1, 2]))

    def test_gap(self):
        self.assertFalse(m.check_consecutive_numbers([1, 3]))

    def test_gap_at_end(self):
        self.assertFalse(m.check_consecutive_numbers([1, 2, 4]))

    def test_not_ascending(self):
        self.assertFalse(m.check_consecutive_numbers([2, 1]))


class TestPhonesToVowelIndices(unittest.TestCase):
    def test_all_consonants(self):
        phones = [make_phone('s'), make_phone('t'), make_phone('r')]
        self.assertEqual(m.phones_to_vowel_indices(phones), [])

    def test_single_vowel(self):
        phones = [make_phone('s'), make_phone('ɑ'), make_phone('t')]
        self.assertEqual(m.phones_to_vowel_indices(phones), [1])

    def test_diphthong(self):
        phones = [make_phone('s'), make_phone('ɛ'), make_phone('i'), make_phone('t')]
        self.assertEqual(m.phones_to_vowel_indices(phones), [1, 2])

    def test_vowel_first(self):
        phones = [make_phone('ɑ'), make_phone('t')]
        self.assertEqual(m.phones_to_vowel_indices(phones), [0])

    def test_vowel_last(self):
        phones = [make_phone('t'), make_phone('ɑ')]
        self.assertEqual(m.phones_to_vowel_indices(phones), [1])

    def test_empty(self):
        self.assertEqual(m.phones_to_vowel_indices([]), [])

    def test_unknown_label_raises(self):
        phones = [make_phone('s'), make_phone('Q')]
        with self.assertRaises(ValueError):
            m.phones_to_vowel_indices(phones)

    def test_non_consecutive_vowels_raises(self):
        phones = [make_phone('ɑ'), make_phone('t'), make_phone('ɑ')]
        with self.assertRaises(ValueError):
            m.phones_to_vowel_indices(phones)

    def test_custom_phone_types(self):
        phone_types = {'a': 'vowel', 'b': 'consonant'}
        phones = [make_phone('b'), make_phone('a'), make_phone('b')]
        self.assertEqual(m.phones_to_vowel_indices(phones,
            phone_types=phone_types), [1])


if __name__ == '__main__':
    unittest.main()
