'''Verify grouped key inventories, capped retention, and entry sampling.'''

from contextlib import redirect_stdout
import io
import random
import tempfile
import unittest

import phraser
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word


class TestLexicon(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        with redirect_stdout(io.StringIO()):
            self.store = phraser.Store(path=self.directory.name)
        self.addCleanup(self.store.close)
        self.young = self.store.create(Speaker, name='young', dataset='test',
            gender='female', age=25, save=True)
        self.older = self.store.create(Speaker, name='older', dataset='test',
            gender='male', age=60, save=True)
        self.first = self.make_recording('first.wav', self.young, 100)
        self.second = self.make_recording('second.wav', self.older, 200)
        self.store.refresh_query_roots()

    def make_recording(self, filename, speaker, duration):
        '''Persist nine complete trees in one recording.'''
        audio = self.store.create(Audio, filename=filename, duration=10000,
            save=True)
        speaker.add_audio(audio)
        phrases = []
        for index in range(9):
            label = 'boom' if index == 8 else 'huis'
            phrases.append(self.make_tree(audio, speaker, label,
                index * 1000, duration + index * 10))
        self.store.save_phrase_trees(phrases)
        return audio

    def make_tree(self, audio, speaker, label, start, duration):
        '''Create a linked phrase, word, syllable, and phone.'''
        identity = {'audio_id': audio.identifier,
            'speaker_id': speaker.identifier, 'store': self.store}
        end = start + duration
        phrase = Phrase(label, start, end, **identity)
        word = Word(label, start, end, **identity)
        syllable = Syllable(label, start, end, **identity)
        phone = Phone(label[0], start, end, **identity)
        syllable.add_children([phone])
        word.add_children([syllable])
        phrase.add_children([word])
        return phrase

    def test_default_inventory_counts_keys_and_mapping(self):
        lexicon = phraser.build_word_lexicon(store=self.store)
        self.assertIs(lexicon.segment_class, Word)
        self.assertEqual(lexicon.labels, ['boom', 'huis'])
        self.assertEqual(list(lexicon), ['boom', 'huis'])
        self.assertEqual(len(lexicon), 2)
        self.assertEqual(lexicon.counts, {'boom': 2, 'huis': 16})
        entry = lexicon['huis']
        self.assertIsInstance(entry, phraser.Entry)
        self.assertEqual(entry.label, 'huis')
        self.assertEqual(entry.total_count, 16)
        self.assertEqual(entry.retained_count, 16)
        self.assertEqual(entry.keys, sorted(set(entry.keys)))
        words = self.store.load_many(entry.keys)
        self.assertTrue(all(word.label == 'huis' for word in words))
        labels, counts = lexicon.labels, lexicon.counts
        labels.clear()
        counts.clear()
        self.assertEqual(len(lexicon), 2)
        self.assertEqual(lexicon.counts['huis'], 16)
        with self.assertRaises(KeyError): lexicon['missing']

    def test_cap_counts_only_and_cap_exceeding_population(self):
        full = phraser.build_word_lexicon(store=self.store)
        for cap in (0, 3, 100):
            with self.subTest(cap=cap):
                lexicon = phraser.build_word_lexicon(store=self.store,
                    max_keys_per_type=cap)
                self.assertEqual(lexicon.counts, full.counts)
                for label, entry in lexicon.items():
                    self.assertEqual(entry.retained_count,
                        min(cap, entry.total_count))
                    self.assertEqual(len(set(entry.keys)), entry.retained_count)
                    self.assertTrue(set(entry.keys) <= set(full[label].keys))
                self.assertEqual(lexicon.report, full.report)

    def test_capped_build_is_reproducible_across_recording_order(self):
        state = random.getstate()
        first = phraser.build_word_lexicon(audios=[self.first, self.second],
            max_keys_per_type=3, random_seed=42)
        second = phraser.build_word_lexicon(audios=[self.second, self.first],
            max_keys_per_type=3, random_seed=42)
        self.assertEqual(dict(first), dict(second))
        self.assertEqual(random.getstate(), state)
        self.assertEqual(first['huis'].keys, sorted(first['huis'].keys))

    def test_builders_share_sampler_restrictions_and_report_losses(self):
        word = self.first.words[0]
        word.duration_outlier = 'long'
        self.store.save(word, overwrite=True)
        raw = self.store.DB.load(word.key)
        lexicon = phraser.build_word_lexicon(store=self.store,
            speakers=[self.young], gender='female', max_duration_ms=175)
        self.assertEqual(lexicon.counts, {'huis': 7})
        tokens, report = phraser.sample_word_tokens(None, store=self.store,
            n_tokens=7, speakers=[self.young], gender='female',
            max_duration_ms=175, return_report=True)
        self.assertEqual(set(lexicon['huis'].keys), {w.key for w in tokens})
        self.assertEqual(lexicon.report.stages, report.stages)
        self.assertEqual(lexicon.report.starting_count, 18)
        self.assertEqual(lexicon.report.eligible_count, 7)
        self.assertIsNone(lexicon.report.requested_count)
        self.assertIsNone(lexicon.report.selected_count)
        self.assertIsNone(lexicon.report.unselected_count)
        self.assertIsNone(lexicon.report.shortfall)
        self.assertNotIn('Requested:', str(lexicon.report))
        self.assertEqual(self.store.DB.load(word.key), raw)
        included = phraser.build_word_lexicon(store=self.store,
            exclude_duration_outliers=False)
        self.assertEqual(included.counts, {'boom': 2, 'huis': 16})

    def test_all_segment_classes_and_duplicate_recordings(self):
        cases = [(phraser.build_phone_lexicon, Phone, {'b': 1, 'h': 8}),
            (phraser.build_syllable_lexicon, Syllable, {'boom': 1, 'huis': 8}),
            (phraser.build_phrase_lexicon, Phrase, {'boom': 1, 'huis': 8})]
        for builder, segment_class, counts in cases:
            with self.subTest(segment_class=segment_class):
                lexicon = builder(audios=[self.first, self.first])
                self.assertIs(lexicon.segment_class, segment_class)
                self.assertEqual(lexicon.counts, counts)
                self.assertEqual(lexicon.report.starting_count, 9)

    def test_entry_sampling_is_read_only_and_uses_retained_count(self):
        lexicon = phraser.build_word_lexicon(store=self.store,
            max_keys_per_type=3)
        entry = lexicon['huis']
        keys = entry.keys.copy()
        state = random.getstate()
        first = entry.sample(n_tokens=2, random_seed=5)
        self.assertEqual(first, entry.sample(n_tokens=2, random_seed=5))
        self.assertEqual(len(set(first)), 2)
        self.assertTrue(set(first) <= set(keys))
        self.assertEqual(entry.keys, keys)
        self.assertEqual(entry.total_count, 16)
        self.assertEqual(random.getstate(), state)
        with self.assertRaises(phraser.InsufficientSampleError) as caught:
            entry.sample(n_tokens=4)
        self.assertEqual(caught.exception.report.eligible_count, 3)
        self.assertEqual(caught.exception.report.shortfall, 1)
        self.assertEqual(entry.sample(4, insufficient='drop'), [])
        self.assertEqual(entry.sample(0), [])
        self.store.close()
        self.assertEqual(len(entry.sample(1)), 1)

    def test_empty_inventory_and_invalid_arguments(self):
        lexicon = phraser.build_word_lexicon(audios=[])
        self.assertEqual(lexicon.labels, [])
        self.assertEqual(lexicon.counts, {})
        self.assertEqual(lexicon.report.eligible_count, 0)
        options = [{'max_keys_per_type': -1}, {'max_keys_per_type': True},
            {'max_keys_per_type': 1.5}, {'random_seed': True},
            {'min_duration_ms': 200, 'max_duration_ms': 100},
            {'exclude_duration_outliers': None}, {'gender': 'invalid'}]
        for kwargs in options:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                phraser.build_word_lexicon(audios=[], **kwargs)
        with self.assertRaisesRegex(ValueError, 'provide store'):
            phraser.build_word_lexicon()

    def test_entry_and_lexicon_validate_inconsistent_metadata(self):
        with self.assertRaises(ValueError): phraser.Entry('a', [b'key'], 0)
        with self.assertRaises(ValueError): phraser.Entry('a', [], -1)
        entry = phraser.Entry('a', [], 0)
        with self.assertRaises(ValueError): phraser.Lexicon(Audio, {})
        with self.assertRaises(ValueError):
            phraser.Lexicon(Phone, {'b': entry})
        with self.assertRaises(ValueError): entry.sample(-1)
        with self.assertRaises(ValueError): entry.sample(0, insufficient='all')


if __name__ == '__main__':
    unittest.main()
