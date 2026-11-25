import unittest

from phraser import utils
from phraser.corpora.common_voice import speaker_metadata as m
from phraser.models import Speaker


class TestGender(unittest.TestCase):
    def test_values(self):
        self.assertEqual(m.cv_gender('male_masculine'), 'male')
        self.assertEqual(m.cv_gender('female_feminine'), 'female')
        self.assertEqual(m.cv_gender('other'), 'other')
        self.assertEqual(m.cv_gender(''), 'unknown')

    def test_every_value_has_a_code(self):
        for name in m.CV_GENDER.values():
            self.assertIn(name, utils.gender_dict)

    def test_other_code_round_trips(self):
        speaker = Speaker(name='x', dataset='common_voice', gender='other')
        self.assertEqual(speaker.gender_code, 3)
        self.assertEqual(speaker.gender(), 'other')

    def test_existing_codes_unchanged(self):
        self.assertEqual(utils.gender_dict['unknown'], 9)
        self.assertEqual(utils.gender_dict['female'], 1)
        self.assertEqual(utils.gender_dict['male'], 2)

    def test_unknown_value_raises(self):
        with self.assertRaises(ValueError):
            m.cv_gender('non_binary')


class TestAge(unittest.TestCase):
    def test_bands(self):
        self.assertEqual(m.cv_age('teens'), 15)
        self.assertEqual(m.cv_age('fourties'), 45)
        self.assertEqual(m.cv_age('nineties'), 95)
        self.assertEqual(m.cv_age(''), 0)

    def test_fits_u8(self):
        self.assertTrue(all(0 <= age < 256 for age in m.CV_AGE.values()))

    def test_unknown_value_raises(self):
        with self.assertRaises(ValueError):
            m.cv_age('100+')


if __name__ == '__main__':
    unittest.main()
