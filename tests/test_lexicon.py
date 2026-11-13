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
        '''Create an assessed, linked phrase, word, syllable, and phone.'''
        identity = {'audio_id': audio.identifier,
            'speaker_id': speaker.identifier, 'store': self.store}
        end = start + duration
        phrase = Phrase(label, start, end, **identity)
        word = Word(label, start, end, **identity)
        syllable = Syllable(label, start, end, **identity)
        phone = Phone(label[0], start, end, **identity)
        for segment in (phrase, word, syllable, phone):
            segment.duration_outlier = 'within_range'
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
                    max_retained_tokens_per_type=cap)
                self.assertEqual(lexicon.counts, full.counts)
                for label, entry in lexicon.items():
                    self.assertEqual(entry.retained_count,
                        min(cap, entry.total_count))
                    self.assertEqual(len(set(entry.keys)), entry.retained_count)
                    self.assertTrue(set(entry.keys) <= set(full[label].keys))
                self.assertEqual(lexicon.report, full.report)

    def test_retained_token_access_preserves_counts_and_order(self):
        lexicon = phraser.build_word_lexicon(store=self.store,
            max_retained_tokens_per_type=3)
        expected = lexicon['boom'].keys + lexicon['huis'].keys
        self.assertEqual(lexicon.token_keys, expected)
        self.assertEqual(lexicon.n_tokens, 5)
        self.assertEqual(lexicon.retained_counts, {'boom': 2, 'huis': 3})
        self.assertEqual(lexicon.counts, {'boom': 2, 'huis': 16})
        tokens = lexicon.load_tokens(self.store)
        self.assertEqual([token.key for token in tokens], expected)
        self.assertEqual([token.label for token in tokens],
            ['boom'] * 2 + ['huis'] * 3)
        self.assertEqual(repr(lexicon),
            'Lexicon(Word, 2 types, 5 retained tokens, 18 eligible tokens)')
        lexicon.token_keys.clear()
        lexicon.retained_counts.clear()
        self.assertEqual(lexicon.token_keys, expected)
        self.assertEqual(lexicon.retained_counts, {'boom': 2, 'huis': 3})

    def test_progress_and_quiet_build_select_the_same_tokens(self):
        output = io.StringIO()
        with redirect_stdout(output):
            shown = phraser.build_word_lexicon(store=self.store,
                max_retained_tokens_per_type=3)
        text = output.getvalue()
        self.assertIn('Loading lexicon recordings...', text)
        self.assertIn('Loaded 2 recordings in ', text)
        self.assertIn('Recordings 1/2', text)
        self.assertIn('Recordings 2/2 | words scanned 18 | eligible 18', text)
        self.assertIn('types 2 | elapsed ', text)
        self.assertIn('ETA ', text)
        self.assertIn('Lexicon ready: 2 types; 5 retained tokens', text)
        output = io.StringIO()
        with redirect_stdout(output):
            quiet = phraser.build_word_lexicon(store=self.store,
                max_retained_tokens_per_type=3, verbose=False)
        self.assertEqual(output.getvalue(), '')
        self.assertEqual(shown.token_keys, quiet.token_keys)
        self.assertEqual(shown.report, quiet.report)

    def test_empty_and_counts_only_token_access(self):
        for minimum in (None, 100):
            with self.subTest(min_tokens_per_type=minimum):
                lexicon = phraser.build_word_lexicon(store=self.store,
                    min_tokens_per_type=minimum, max_retained_tokens_per_type=0)
                self.assertEqual(lexicon.token_keys, [])
                self.assertEqual(lexicon.n_tokens, 0)
                self.assertEqual(lexicon.load_tokens(self.store), [])
                self.assertEqual(sum(lexicon.retained_counts.values()), 0)
                self.assertEqual(len(lexicon), 2 if minimum is None else 0)

    def test_lexicon_sampling_drops_rare_types_and_preserves_inventory(self):
        lexicon = phraser.build_word_lexicon(store=self.store,
            max_retained_tokens_per_type=4)
        original = lexicon.token_keys
        self.assertEqual(lexicon.sample(2).retained_counts,
            {'boom': 2, 'huis': 2})
        state = random.getstate()
        sampled = lexicon.sample(n_tokens=3, random_seed=42)
        self.assertIsInstance(sampled, phraser.Lexicon)
        self.assertEqual(sampled.labels, ['huis'])
        self.assertEqual(sampled.retained_counts, {'huis': 3})
        self.assertEqual(sampled.counts, {'huis': 16})
        self.assertTrue(set(sampled.token_keys) <= set(lexicon['huis'].keys))
        self.assertEqual(sampled.token_keys, sorted(sampled.token_keys))
        self.assertEqual(sampled.token_keys,
            lexicon.sample(3, random_seed=42).token_keys)
        self.assertEqual(lexicon.token_keys, original)
        self.assertEqual(random.getstate(), state)
        self.assertEqual(sampled.report, lexicon.report)
        self.assertIsNot(sampled.report, lexicon.report)
        report = sampled.sampling_report
        self.assertEqual(report.starting_count, 6)
        self.assertEqual(report.eligible_count, 4)
        self.assertEqual(report.selected_count, 3)
        self.assertEqual(report.stages[0].types_removed, 1)
        sampled['huis'].keys.clear()
        self.assertEqual(lexicon.token_keys, original)
        self.assertIsNone(lexicon.sampling_report)

    def test_sampling_uses_retained_keys_and_handles_insufficient_types(self):
        lexicon = phraser.build_word_lexicon(store=self.store,
            max_retained_tokens_per_type=2)
        self.assertEqual(len(lexicon.sample(3)), 0)
        with self.assertRaises(phraser.InsufficientSampleError):
            lexicon.sample(3, insufficient='raise')
        for kwargs in ({'n_tokens': -1}, {'n_tokens': True},
            {'n_tokens': 1.5}, {'insufficient': 'partial'}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                lexicon.sample(**kwargs)

    def test_defaults_exclude_timing_concerns_and_overlapping_speech(self):
        phrase = self.make_tree(self.first, self.older, 'intruder', 50, 100)
        self.store.save_phrase_trees([phrase])
        self.store.refresh_query_roots()
        words = self.first.words
        target = next(word for word in words if word.start == 0)
        uncertain = next(word for word in words if word.start == 1000)
        uncertain.timing_uncertain = True
        self.store.save(uncertain, overwrite=True)
        raw = self.store.DB.load(target.key)
        lexicon = phraser.build_word_lexicon(audios=[self.first])
        self.assertNotIn(target.key, lexicon.token_keys)
        self.assertNotIn(uncertain.key, lexicon.token_keys)
        stages = {stage.condition: stage for stage in lexicon.report.stages}
        self.assertGreater(stages['Exclude overlapping speech'].removed, 0)
        self.assertEqual(stages['Recorded timing uncertain'].removed, 1)
        included = phraser.build_word_lexicon(audios=[self.first],
            exclude_overlap=False, exclude_timing_uncertain=False)
        self.assertIn(target.key, included.token_keys)
        self.assertIn(uncertain.key, included.token_keys)
        self.assertEqual(self.store.DB.load(target.key), raw)

    def test_capped_build_is_reproducible_across_recording_order(self):
        state = random.getstate()
        first = phraser.build_word_lexicon(audios=[self.first, self.second],
            max_retained_tokens_per_type=3, random_seed=42)
        second = phraser.build_word_lexicon(audios=[self.second, self.first],
            max_retained_tokens_per_type=3, random_seed=42)
        self.assertEqual(dict(first), dict(second))
        self.assertEqual(random.getstate(), state)
        self.assertEqual(first['huis'].keys, sorted(first['huis'].keys))

    def test_builders_share_sampler_restrictions_and_report_losses(self):
        word = self.first.words[0]
        word.duration_outlier = 'long'
        self.store.save(word, overwrite=True)
        raw = self.store.DB.load(word.key)
        lexicon = phraser.build_word_lexicon(store=self.store,
            speakers=[self.young], gender='female', max_duration_ms=175,
            exclude_timing_uncertain=False, exclude_overlap=False)
        self.assertEqual(lexicon.counts, {'huis': 7})
        tokens, report = phraser.sample_word_tokens(None, store=self.store,
            n_tokens=7, speakers=[self.young], gender='female',
            max_duration_ms=175, return_report=True)
        self.assertEqual(set(lexicon['huis'].keys), {w.key for w in tokens})
        stages = lexicon.report.stages
        built = [(s.condition, s.before, s.after) for s in stages]
        sampled = [(s.condition, s.before, s.after) for s in report.stages]
        self.assertEqual(built, sampled)
        self.assertTrue(all(s.types_before is None for s in report.stages))
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

    def test_phone_count_and_timing_uncertain_restrictions(self):
        word = self.first.words[0]
        word.timing_uncertain = True
        self.store.save(word, overwrite=True)
        lexicon = phraser.build_word_lexicon(store=self.store, min_phones=1,
            exclude_timing_uncertain=True)
        self.assertEqual(lexicon.counts, {'boom': 2, 'huis': 15})
        conditions = [stage.condition for stage in lexicon.report.stages]
        self.assertEqual(conditions[0], 'At least 1 phones')
        self.assertEqual(conditions[-1], 'Exclude overlapping speech')
        self.assertEqual(lexicon.report.stages[-2].removed, 1)
        lexicon = phraser.build_word_lexicon(store=self.store, min_phones=2)
        self.assertEqual(lexicon.counts, {})
        self.assertEqual(lexicon.report.stages[0].removed, 18)
        lexicon = phraser.build_syllable_lexicon(store=self.store,
            min_phones=2)
        self.assertEqual(lexicon.counts, {})

    def test_unassessed_store_raises_and_one_recording_does_not(self):
        for word in self.first.words: word.duration_outlier = 'unset'
        lexicon = phraser.build_word_lexicon(store=self.store)
        self.assertEqual(lexicon.counts, {'boom': 1, 'huis': 8})
        stages = {stage.condition: stage for stage in lexicon.report.stages}
        self.assertEqual(stages['Duration flag assessed'].removed, 9)
        for word in self.second.words: word.duration_outlier = 'unset'
        with self.assertRaisesRegex(ValueError, 'not assessed'):
            phraser.build_word_lexicon(store=self.store)

    def test_min_tokens_per_type_drops_rare_labels_before_cap(self):
        lexicon = phraser.build_word_lexicon(store=self.store,
            min_tokens_per_type=3, max_retained_tokens_per_type=3)
        self.assertEqual(lexicon.labels, ['huis'])
        self.assertEqual(lexicon['huis'].total_count, 16)
        self.assertEqual(lexicon['huis'].retained_count, 3)
        stage = lexicon.report.stages[-1]
        self.assertEqual(stage.condition, 'At least 3 tokens per type')
        self.assertEqual((stage.before, stage.after), (18, 16))
        self.assertEqual(lexicon.report.eligible_count, 16)
        lexicon = phraser.build_word_lexicon(store=self.store,
            min_tokens_per_type=2)
        self.assertEqual(lexicon.counts, {'boom': 2, 'huis': 16})

    def test_report_counts_types_and_labels_falling_below_minimum(self):
        lexicon = phraser.build_word_lexicon(store=self.store,
            min_tokens_per_type=2, max_duration_ms=175)
        stages = {stage.condition: stage for stage in lexicon.report.stages}
        stage = stages['Duration 100–175 ms']
        self.assertEqual((stage.before, stage.after), (18, 8))
        self.assertEqual((stage.types_before, stage.types_after), (2, 1))
        self.assertEqual(stage.types_removed, 1)
        self.assertEqual(stage.types_below_minimum, 1)
        text = str(lexicon.report)
        row = next(line for line in text.splitlines()
            if line.startswith('Duration 100'))
        self.assertEqual(row.split()[-5:], ['8', '10', '1', '1', '1'])
        self.assertIn('Below quota: types newly below 2 tokens', text)
        stage = lexicon.report.stages[-1]
        self.assertEqual((stage.types_before, stage.types_after), (1, 1))
        self.assertEqual(stage.types_below_minimum, 0)
        lexicon = phraser.build_word_lexicon(store=self.store,
            max_duration_ms=175)
        stage = lexicon.report.stages[1]
        self.assertEqual(stage.types_after, 1)
        self.assertIsNone(stage.types_below_minimum)
        self.assertNotIn('fell below', str(lexicon.report))

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
            max_retained_tokens_per_type=3)
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
        options = [{'max_retained_tokens_per_type': -1},
            {'max_retained_tokens_per_type': True},
            {'max_retained_tokens_per_type': 1.5}, {'min_tokens_per_type': -1},
            {'min_tokens_per_type': 1.5}, {'random_seed': True},
            {'min_duration_ms': 200, 'max_duration_ms': 100},
            {'exclude_duration_outliers': None}, {'gender': 'invalid'},
            {'min_phones': -1}, {'min_phones': 1.5},
            {'exclude_timing_uncertain': None}]
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
