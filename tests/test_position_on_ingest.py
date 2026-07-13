import unittest
import io
import shutil
import tempfile
from contextlib import redirect_stdout
from types import SimpleNamespace

from phraser import Store
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


class Tier(list):
    pass


def interval(mark, min_time=0, max_time=1):
    return SimpleNamespace(mark=mark, minTime=min_time, maxTime=max_time)


class MiniTextGrid:
    def __init__(self, names=None, tiers=None):
        if names is None:
            names = ['ORT-MAU', 'KAN-MAU']
        if tiers is None:
            tiers = [
                Tier([interval('test')]),
                Tier([interval('t ɛ s t')]),
            ]
        self.names = names
        self.tiers = tiers

    def getNames(self):
        return self.names


class TestTextGridStoreBoundStaging(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self._tmpdir)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_textgrid_conversion_requires_store_even_when_not_saving(self):
        with self.assertRaisesRegex(ValueError, 'store is required'):
            list(textgrid_loader.textgrid_to_words(MiniTextGrid(),
                save_to_db=False, store=None))

    def test_textgrid_overwrite_option_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'overwrite=True'):
            textgrid_loader.textgrid_filename_to_database_objects(
                'does-not-need-to-exist.TextGrid',
                overwrite=True,
                store=self.store,
            )

    def test_missing_word_tier_raises_value_error(self):
        tg = MiniTextGrid(names=['KAN-MAU'], tiers=[Tier([interval('t')])])

        with self.assertRaisesRegex(ValueError, "ORT-MAU.*words"):
            list(textgrid_loader.textgrid_to_words(tg, store=self.store))

    def test_missing_syllable_tier_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "MAS.*syllables"):
            list(textgrid_loader.textgrid_to_syllables(MiniTextGrid(),
                store=self.store))

    def test_missing_phone_tier_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "MAU.*phones"):
            list(textgrid_loader.textgrid_to_phones(MiniTextGrid(),
                store=self.store))

    def test_word_and_ipa_tier_lengths_must_match(self):
        tg = MiniTextGrid(tiers=[
            Tier([interval('one'), interval('two', 1, 2)]),
            Tier([interval('w ʌ n')]),
        ])

        with self.assertRaisesRegex(ValueError, 'matching interval counts'):
            list(textgrid_loader.textgrid_to_words(tg, store=self.store))

    def test_word_and_ipa_interval_times_must_match(self):
        tg = MiniTextGrid(tiers=[
            Tier([interval('one', 0, 1)]),
            Tier([interval('w ʌ n', 0.1, 1)]),
        ])

        with self.assertRaisesRegex(ValueError, 'matching start/end times'):
            list(textgrid_loader.textgrid_to_words(tg, store=self.store))

    def test_save_to_db_false_stages_store_bound_objects_without_writing(self):
        before = len(self.store.DB.all_keys())
        words = list(textgrid_loader.textgrid_to_words(MiniTextGrid(),
            save_to_db=False, store=self.store))
        after = len(self.store.DB.all_keys())

        self.assertEqual(len(words), 1)
        self.assertIs(words[0].store, self.store)
        self.assertEqual((before, after), (0, 0))
        self.assertTrue(self.store.is_db_saving_allowed())

    def test_interval_to_word_does_not_reuse_previous_ipa(self):
        ort_one = SimpleNamespace(mark='one', minTime=0, maxTime=1)
        ipa_one = SimpleNamespace(mark='w ʌ n', minTime=0, maxTime=1)
        ort_two = SimpleNamespace(mark='two', minTime=1, maxTime=2)

        word_one = textgrid_loader.interval_to_word(ort_one, ipa_one,
            store=self.store)
        word_two = textgrid_loader.interval_to_word(ort_two, store=self.store)

        self.assertEqual(word_one.ipa, 'w ʌ n')
        self.assertEqual(word_two.ipa, '')


if __name__ == '__main__':
    unittest.main()
