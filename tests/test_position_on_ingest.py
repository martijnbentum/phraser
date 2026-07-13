import unittest
import io
import shutil
import tempfile
from contextlib import redirect_stdout
from types import SimpleNamespace

from phraser import Store
from phraser import models
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


def make_textgrid_items(store, audio_id=None, speaker_id=None, start=100,
    end=300, label='old phrase', filename='old.TextGrid'):
    if audio_id is None: audio_id = b'\x01' * 8
    if speaker_id is None: speaker_id = b'\x02' * 8
    phrase = models.Phrase(label=label, start=start, end=end,
        audio_id=audio_id, speaker_id=speaker_id, filename=filename,
        store=store, save=False)
    word = models.Word(label=label.split()[0], start=start, end=start + 100,
        audio_id=audio_id, speaker_id=speaker_id, store=store, save=False)
    word.add_parent(phrase, update_database=False)
    return [word, phrase]


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
                store=None))

    def test_textgrid_overwrite_option_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'overwrite=True'):
            textgrid_loader.textgrid_filename_to_database_objects(
                'does-not-need-to-exist.TextGrid',
                overwrite=True,
                store=self.store,
            )

    def test_batch_loader_requires_audio_and_textgrid_lengths_to_match(self):
        with self.assertRaisesRegex(ValueError,
            'audio_filenames.*textgrid_filenames'):
            textgrid_loader.load_audios_textgrids_to_db(
                ['one.wav', 'two.wav'],
                ['one.TextGrid'],
                speakers=None,
                save_to_db=False,
                store=self.store,
            )

    def test_batch_loader_requires_speaker_length_to_match(self):
        with self.assertRaisesRegex(ValueError, 'textgrid_filenames.*speakers'):
            textgrid_loader.load_audios_textgrids_to_db(
                ['one.wav'],
                ['one.TextGrid'],
                speakers=['speaker-one', 'speaker-two'],
                save_to_db=False,
                store=self.store,
            )

    def test_speaker_batch_loader_requires_audio_and_textgrid_lengths_to_match(self):
        with self.assertRaisesRegex(ValueError,
            'audio_filenames.*textgrid_filenames'):
            textgrid_loader.load_speaker_audios_textgrids_to_db(
                'speaker-one',
                ['one.wav', 'two.wav'],
                ['one.TextGrid'],
                save_to_db=False,
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
            store=self.store))
        after = len(self.store.DB.all_keys())

        self.assertEqual(len(words), 1)
        self.assertIs(words[0].store, self.store)
        self.assertEqual((before, after), (0, 0))

    def test_low_level_word_generator_does_not_accept_save_to_db(self):
        with self.assertRaises(TypeError):
            list(textgrid_loader.textgrid_to_words(MiniTextGrid(),
                save_to_db=True, store=self.store))

    def test_low_level_syllable_generator_does_not_accept_save_to_db(self):
        tg = MiniTextGrid(names=['MAS'], tiers=[Tier([interval('syl')])])

        with self.assertRaises(TypeError):
            list(textgrid_loader.textgrid_to_syllables(tg,
                save_to_db=True, store=self.store))

    def test_low_level_phone_generator_does_not_accept_save_to_db(self):
        tg = MiniTextGrid(names=['MAU'], tiers=[Tier([interval('t')])])

        with self.assertRaises(TypeError):
            list(textgrid_loader.textgrid_to_phones(tg,
                save_to_db=True, store=self.store))

    def test_interval_to_word_does_not_reuse_previous_ipa(self):
        ort_one = SimpleNamespace(mark='one', minTime=0, maxTime=1)
        ipa_one = SimpleNamespace(mark='w ʌ n', minTime=0, maxTime=1)
        ort_two = SimpleNamespace(mark='two', minTime=1, maxTime=2)

        word_one = textgrid_loader.interval_to_word(ort_one, ipa_one,
            store=self.store)
        word_two = textgrid_loader.interval_to_word(ort_two, store=self.store)

        self.assertEqual(word_one.ipa, 'w ʌ n')
        self.assertEqual(word_two.ipa, '')

    def test_save_textgrid_items_append_does_not_check_existing(self):
        old_items = make_textgrid_items(self.store)
        new_items = make_textgrid_items(self.store, label='new phrase',
            filename='new.TextGrid')
        textgrid_loader.save_textgrid_items(old_items, store=self.store)

        action = textgrid_loader.save_textgrid_items(new_items,
            store=self.store, existing='append')
        phrase_keys = list(self.store.DB.audio_id_to_child_keys(
            old_items[-1].audio_id, 'Phrase'))

        self.assertEqual(action, 'added')
        self.assertEqual(len(phrase_keys), 2)

    def test_save_textgrid_items_add_missing_skips_existing(self):
        old_items = make_textgrid_items(self.store)
        new_items = make_textgrid_items(self.store, label='new phrase',
            filename='new.TextGrid')
        textgrid_loader.save_textgrid_items(old_items, store=self.store)

        action = textgrid_loader.save_textgrid_items(new_items,
            store=self.store, existing='add_missing')

        self.assertEqual(action, 'skipped')
        self.assertFalse(self.store.DB.key_exists(new_items[-1].key))

    def test_save_textgrid_items_replace_swaps_matching_phrase_tree(self):
        old_items = make_textgrid_items(self.store)
        new_items = make_textgrid_items(self.store, label='new phrase',
            filename='new.TextGrid')
        old_keys = [item.key for item in old_items]
        old_label_index_keys = textgrid_loader.items_to_label_index_keys(
            old_items)
        textgrid_loader.save_textgrid_items(old_items, store=self.store)

        action = textgrid_loader.save_textgrid_items(new_items,
            store=self.store, existing='replace')

        self.assertEqual(action, 'replaced')
        for key in old_keys:
            self.assertFalse(self.store.DB.key_exists(key))
        for key in old_label_index_keys:
            self.assertFalse(self.store.DB.key_exists(key,
                db_name='label_segment'))
        for item in new_items:
            self.assertTrue(self.store.DB.key_exists(item.key))

    def test_save_textgrid_items_replace_requires_existing_match(self):
        items = make_textgrid_items(self.store)

        with self.assertRaisesRegex(ValueError, 'replace requires'):
            textgrid_loader.save_textgrid_items(items, store=self.store,
                existing='replace')

    def test_save_textgrid_items_upsert_adds_or_replaces(self):
        add_items = make_textgrid_items(self.store, start=100)
        replace_items = make_textgrid_items(self.store, start=100,
            label='replacement', filename='replacement.TextGrid')

        add_action = textgrid_loader.save_textgrid_items(add_items,
            store=self.store, existing='upsert')
        replace_action = textgrid_loader.save_textgrid_items(replace_items,
            store=self.store, existing='upsert')

        self.assertEqual(add_action, 'added')
        self.assertEqual(replace_action, 'replaced')
        self.assertFalse(self.store.DB.key_exists(add_items[-1].key))
        self.assertTrue(self.store.DB.key_exists(replace_items[-1].key))

    def test_save_textgrid_items_existence_check_requires_audio(self):
        items = make_textgrid_items(self.store, audio_id=b'\x00' * 8)

        with self.assertRaisesRegex(ValueError, 'audio_id'):
            textgrid_loader.save_textgrid_items(items, store=self.store,
                existing='add_missing')

    def test_save_textgrid_items_multiple_matches_raise(self):
        audio_id = b'\x03' * 8
        first = make_textgrid_items(self.store, audio_id=audio_id)
        second = make_textgrid_items(self.store, audio_id=audio_id,
            filename='duplicate.TextGrid')
        incoming = make_textgrid_items(self.store, audio_id=audio_id,
            filename='incoming.TextGrid')
        textgrid_loader.save_textgrid_items(first, store=self.store)
        textgrid_loader.save_textgrid_items(second, store=self.store)

        with self.assertRaisesRegex(ValueError, 'multiple matching phrases'):
            textgrid_loader.save_textgrid_items(incoming, store=self.store,
                existing='replace')

    def test_save_textgrid_items_rejects_unknown_policy(self):
        items = make_textgrid_items(self.store)

        with self.assertRaisesRegex(ValueError, 'existing must be one of'):
            textgrid_loader.save_textgrid_items(items, store=self.store,
                existing='unknown')


if __name__ == '__main__':
    unittest.main()
