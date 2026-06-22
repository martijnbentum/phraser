import unittest

from phraser import textgrid_loader
from phraser.segment import Phone, Syllable


def make_phone(label, start, end):
    return Phone(label=label, start=start, end=end, save=False, store=None)


def make_syllable(start, end):
    return Syllable(label='syl', start=start, end=end, save=False, store=None)


class TestPositionOnIngest(unittest.TestCase):
    '''find_and_add_phones_to_syllable should assign onset/nucleus/coda to the
    phones during loading (save_to_db=False, in-memory).'''

    def test_assigns_positions_during_ingest(self):
        syllable = make_syllable(0, 400)
        phones = [make_phone('s', 0, 100), make_phone('t', 100, 200),
                  make_phone('a', 200, 300), make_phone('r', 300, 400)]
        textgrid_loader.find_and_add_phones_to_syllable(syllable, phones,
            save_to_db=False)
        self.assertEqual([p.position for p in phones],
            ['onset', 'onset', 'nucleus', 'coda'])
        self.assertEqual([p.position_code for p in phones], [1, 1, 2, 3])

    def test_opt_out_leaves_positions_unknown(self):
        syllable = make_syllable(0, 200)
        phones = [make_phone('t', 0, 100), make_phone('a', 100, 200)]
        textgrid_loader.find_and_add_phones_to_syllable(syllable, phones,
            save_to_db=False, assign_positions=False)
        self.assertEqual([p.position for p in phones], ['unknown', 'unknown'])

    def test_odd_transcription_is_swallowed(self):
        '''An unknown phone label raises ValueError inside assignment; the
        loader should swallow it and leave phones at the default.'''
        syllable = make_syllable(0, 200)
        phones = [make_phone('zzz', 0, 100), make_phone('a', 100, 200)]
        textgrid_loader.find_and_add_phones_to_syllable(syllable, phones,
            save_to_db=False)
        self.assertEqual([p.position for p in phones], ['unknown', 'unknown'])


if __name__ == '__main__':
    unittest.main()
