'''Exercise constrained sampling against real Phraser hierarchies.'''

from contextlib import redirect_stdout
import io
import random
import tempfile
import unittest

import phraser
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word
from phraser.sampling import cgn


class TestSampling(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        with redirect_stdout(io.StringIO()):
            self.store = phraser.Store(path=self.directory.name)
        self.addCleanup(self.store.close)
        self.young = self.store.create(Speaker, name='young', dataset='cgn',
            gender='female', age=25, save=True)
        self.older = self.store.create(Speaker, name='older', dataset='cgn',
            gender='male', age=60, save=True)
        self.unknown = self.store.create(Speaker, name='unknown',
            dataset='cgn', save=True)
        self.first = self.make_audio('/data/comp-b/nl/first.wav', 2000)
        self.second = self.make_audio('/data/comp-c/vl/second.wav', 4000)
        self.young.add_audio(self.first)
        self.young.add_audio(self.second)
        self.older.add_audio(self.first)
        phrases = []
        for index, duration in enumerate([100, 100, 100, 100, 100, 900]):
            phrases.append(self.make_tree(self.first, self.young, 'huis',
                index * 1000, duration))
        phrases.append(self.make_tree(self.first, self.older, 'huis',
            7000, 100))
        phrases.append(self.make_tree(self.second, self.young, 'boom',
            0, 300))
        self.store.save_phrase_trees(phrases)
        self.store.refresh_query_roots()

    def make_audio(self, filename, duration):
        '''Persist a recording with sampling metadata.'''
        return self.store.create(Audio, filename=filename, duration=duration,
            dataset='cgn', language='nld', dialect='nl-NL', save=True)

    def make_tree(self, audio, speaker, label, start, duration):
        '''Build an assessed phrase with one word, syllable, and phone.'''
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

    def test_word_filters_use_token_speaker_and_inclusive_durations(self):
        words = phraser.sample_word_tokens('huis', store=self.store,
            n_tokens=5, gender='female', min_duration_ms=100,
            max_duration_ms=100)
        self.assertEqual(len(words), 5)
        self.assertTrue(all(word.speaker == self.young for word in words))
        self.assertTrue(all(word.duration == 100 for word in words))

    def test_sampling_does_not_recalculate_duration_outliers(self):
        words = phraser.sample_word_tokens(None, store=self.store,
            n_tokens=7, gender='female')
        self.assertEqual(sorted(word.duration for word in words),
            [100, 100, 100, 100, 100, 300, 900])
        words = phraser.sample_word_tokens('huis', store=self.store,
            n_tokens=1, min_duration_ms=900, max_duration_ms=900)
        self.assertEqual(words[0].duration, 900)

    def test_only_within_range_at_every_segment_level(self):
        cases = [
            (phraser.sample_word_tokens, self.second.words[0]),
            (phraser.sample_phrase_tokens, self.second.phrases[0]),
            (phraser.sample_syllable_tokens, self.second.syllables[0]),
            (phraser.sample_phone_tokens, self.second.phones[0])]
        for function, token in cases:
            for state in ('unset', 'short', 'long', 'within_range'):
                with self.subTest(level=token.object_type, state=state):
                    token.duration_outlier = state
                    self.store.save(token, overwrite=True)
                    raw = self.store.DB.load(token.key)
                    if state == 'unset':
                        with self.assertRaisesRegex(ValueError,
                            'not assessed'):
                            function(token.label, audios=[self.second],
                                n_tokens=1)
                    else:
                        result = function(token.label, audios=[self.second],
                            n_tokens=1, insufficient='drop')
                        expected = [token] if state == 'within_range' else []
                        self.assertEqual(result, expected)
                    result = function(token.label, audios=[self.second],
                        n_tokens=1, include_only_within_range=False)
                    self.assertEqual(result, [token])
                    self.assertEqual(self.store.DB.load(token.key), raw)
                    self.assertEqual(token.duration_outlier, state)

    def test_unassessed_tokens_are_reported_separately(self):
        token = self.first.words[0]
        token.duration_outlier = 'unset'
        words, report = phraser.sample_word_tokens('huis', store=self.store,
            n_tokens=1, return_report=True)
        stages = {stage.condition: stage for stage in report.stages}
        self.assertEqual(stages['Duration flag assessed'].removed, 1)
        self.assertEqual(stages['Duration flag within_range'].removed, 0)
        for word in self.first.words: word.duration_outlier = 'unset'
        with self.assertRaisesRegex(ValueError, 'not assessed for these word'):
            phraser.sample_word_tokens('huis', audios=[self.first],
                n_tokens=1)
        words = phraser.sample_word_tokens('huis', audios=[self.first],
            n_tokens=1, include_only_within_range=False)
        self.assertEqual(len(words), 1)

    def test_parent_flag_does_not_exclude_word_and_bounds_still_apply(self):
        self.second.phrases[0].duration_outlier = 'long'
        words = phraser.sample_word_tokens('boom', audios=[self.second],
            n_tokens=1)
        self.assertEqual(words, self.second.words)
        words[0].duration_outlier = 'short'
        with self.assertRaisesRegex(ValueError, 'only 0'):
            phraser.sample_word_tokens('boom', audios=[self.second],
                n_tokens=1)
        words = phraser.sample_word_tokens('boom', audios=[self.second],
            n_tokens=1, include_only_within_range=False, max_duration_ms=200,
            insufficient='drop')
        self.assertEqual(words, [])

    def test_explicit_levels_and_recording_scope(self):
        functions = [phraser.sample_phrase_tokens,
            phraser.sample_syllable_tokens, phraser.sample_phone_tokens]
        for function in functions:
            label = 'b' if function == phraser.sample_phone_tokens else 'boom'
            tokens = function(label, audios=[self.second], n_tokens=1)
            self.assertEqual(tokens[0].audio_key, self.second.key)
        words = phraser.sample_word_tokens('huis', audios=[self.second],
            n_tokens=1, insufficient='drop')
        self.assertEqual(words, [])

    def test_reproducible_order_and_unchanged_global_random_state(self):
        state = random.getstate()
        audios = [self.first, self.second]
        first = phraser.sample_word_tokens(None, audios=audios, n_tokens=4)
        second = phraser.sample_word_tokens(None, audios=reversed(audios),
            n_tokens=4)
        self.assertEqual([word.key for word in first],
            [word.key for word in second])
        self.assertEqual(random.getstate(), state)

    def test_speaker_objects_and_keys(self):
        for speakers in ([self.older], [self.older.key], self.older):
            words = phraser.sample_word_tokens('huis', store=self.store,
                speakers=speakers, n_tokens=1)
            self.assertEqual(words[0].speaker_key, self.older.key)

    def test_unique_speakers_age_range_exact_age_and_unknown(self):
        speakers = phraser.sample_speakers(store=self.store, n_speakers=1,
            min_age=25, max_age=25, gender='female')
        self.assertEqual(speakers, [self.young])
        speakers = phraser.sample_speakers(audios=[self.first, self.second],
            n_speakers=2)
        self.assertEqual({s.key for s in speakers},
            {self.young.key, self.older.key})
        speakers = phraser.sample_speakers(store=self.store, n_speakers=1,
            age=60)
        self.assertEqual(speakers, [self.older])
        speakers = phraser.sample_speakers(store=self.store, n_speakers=1,
            age=0)
        self.assertEqual(speakers, [self.unknown])
        speakers = phraser.sample_speakers(store=self.store, n_speakers=2,
            min_age=0)
        self.assertNotIn(self.unknown, speakers)

    def test_audio_filters_require_same_matching_speaker(self):
        audios = phraser.sample_audios(store=self.store, n_audios=1,
            min_duration_ms=2000, max_duration_ms=2000, dataset='cgn',
            language='nld', dialect='nl-NL')
        self.assertEqual(audios, [self.first])
        audios = phraser.sample_audios(store=self.store, n_audios=1,
            speakers=[self.older], gender='female', insufficient='drop')
        self.assertEqual(audios, [])

    def test_duplicates_do_not_inflate_population(self):
        with self.assertRaisesRegex(ValueError, 'only 1'):
            phraser.sample_audios(audios=[self.first, self.first], n_audios=2)

    def test_sampling_reopened_store_uses_persisted_demographics(self):
        path = self.store.path
        self.store.close()
        with redirect_stdout(io.StringIO()):
            reopened = phraser.Store(path=path)
        self.addCleanup(reopened.close)
        speakers = phraser.sample_speakers(store=reopened, n_speakers=1,
            min_age=25, max_age=25, gender='female')
        self.assertEqual(speakers[0].key, self.young.key)
        words = phraser.sample_word_tokens('huis', store=reopened,
            n_tokens=5, gender='female')
        self.assertEqual(len(words), 5)

    def test_recording_without_duration_needs_no_duration_filter(self):
        audio = Audio(filename='/data/comp-b/nl/no-duration.wav')
        self.assertEqual(phraser.sample_audios(audios=[audio], n_audios=1),
            [audio])
        audios = phraser.sample_audios(audios=[audio], n_audios=1,
            min_duration_ms=0, insufficient='drop')
        self.assertEqual(audios, [])

    def test_cgn_selection_validates_and_composes(self):
        audios = cgn.select_audios(store=self.store, components='bb',
            variant='nl')
        self.assertEqual(audios, [self.first])
        words = phraser.sample_word_tokens('huis', audios=audios, n_tokens=1)
        self.assertEqual(words[0].audio_key, self.first.key)
        with self.assertRaises(ValueError):
            cgn.select_audios(audios=[], components='z')
        invalid = Audio(filename='/other/file.wav')
        with self.assertRaisesRegex(ValueError, 'CGN recording path'):
            cgn.select_audios(audios=[invalid])

    def test_empty_shortage_zero_and_invalid_options(self):
        with self.assertRaisesRegex(ValueError, 'provide store'):
            phraser.sample_word_tokens('huis', n_tokens=1)
        with self.assertRaisesRegex(ValueError, 'only 0'):
            phraser.sample_word_tokens('missing', store=self.store)
        self.assertEqual(phraser.sample_audios(audios=[], n_audios=0), [])
        options = [{'n_tokens': -1}, {'random_seed': True},
            {'min_duration_ms': 200, 'max_duration_ms': 100},
            {'include_only_within_range': None}, {'gender': 'invalid'},
            {'insufficient': 'anything'}]
        for kwargs in options:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                phraser.sample_word_tokens('huis', audios=[], **kwargs)
        with self.assertRaises(ValueError):
            phraser.sample_speakers(audios=[], min_age=50, max_age=20)

    def test_token_report_attributes_sequential_losses_and_draw(self):
        token = self.first.words[0]
        token.duration_outlier = 'short'
        words, report = phraser.sample_word_tokens('huis', store=self.store,
            n_tokens=2, gender='female', max_duration_ms=200,
            return_report=True)
        self.assertEqual(report.starting_count, 8)
        self.assertEqual(report.type_label, 'huis')
        counts = [(s.before, s.after, s.removed) for s in report.stages]
        self.assertEqual(counts, [(8, 7, 1), (7, 6, 1), (6, 6, 0),
            (6, 5, 1), (5, 5, 0), (5, 4, 1)])
        self.assertEqual(report.requested_count, 2)
        self.assertEqual(report.eligible_count, 4)
        self.assertEqual(report.selected_count, len(words))
        self.assertEqual(report.unselected_count, 2)
        self.assertEqual(report.shortfall, 0)
        self.assertIn('removed 1', str(report))
        self.assertIn('Losses are sequential', str(report))
        expected = phraser.sample_word_tokens('huis', store=self.store,
            n_tokens=2, gender='female', max_duration_ms=200)
        self.assertEqual([word.key for word in words],
            [word.key for word in expected])

    def test_overlapping_failures_are_attributed_once(self):
        words, report = phraser.sample_word_tokens('huis', store=self.store,
            speakers=[self.young], gender='female', n_tokens=1,
            return_report=True)
        stages = {stage.condition: stage for stage in report.stages}
        self.assertEqual(stages['Speaker selection'].removed, 1)
        self.assertEqual(stages['Gender female'].removed, 0)
        self.assertEqual(report.eligible_count, 6)
        self.assertEqual(len(words), 1)

    def test_shortage_error_carries_report_without_opt_in(self):
        with self.assertRaises(phraser.InsufficientSampleError) as caught:
            phraser.sample_word_tokens('huis', store=self.store,
                n_tokens=10, gender='female', max_duration_ms=200)
        report = caught.exception.report
        self.assertIsInstance(caught.exception, ValueError)
        self.assertEqual(report.eligible_count, 5)
        self.assertEqual(report.shortfall, 5)
        self.assertEqual(report.selected_count, 0)
        self.assertIn('Shortfall: 5', str(report))
        words, dropped = phraser.sample_word_tokens('huis', store=self.store,
            n_tokens=10, gender='female', max_duration_ms=200,
            insufficient='drop', return_report=True)
        self.assertEqual(words, [])
        self.assertEqual(dropped, report)

    def test_reports_for_all_other_segment_levels(self):
        functions = [phraser.sample_phrase_tokens,
            phraser.sample_syllable_tokens, phraser.sample_phone_tokens]
        for function in functions:
            with self.subTest(function=function.__name__):
                label = 'b' if 'phone' in function.__name__ else 'boom'
                tokens, report = function(label, audios=[self.second],
                    n_tokens=1, return_report=True)
                self.assertEqual(report.starting_count, 1)
                self.assertEqual(report.selected_count, 1)
                self.assertEqual(tokens[0].label, label)

    def test_speaker_report_counts_unique_speakers_and_age_losses(self):
        speakers, report = phraser.sample_speakers(store=self.store,
            n_speakers=1, dataset='cgn', min_age=30, return_report=True)
        self.assertEqual(speakers, [self.older])
        self.assertEqual(report.starting_count, 3)
        self.assertEqual(report.stages[-1].condition, 'Age >= 30 years')
        self.assertEqual(report.stages[-1].removed, 2)
        speakers, report = phraser.sample_speakers(audios=[self.first,
            self.second, self.first], n_speakers=1,
            gender='female', age=25, return_report=True)
        self.assertEqual(report.starting_count, 2)
        self.assertEqual(report.eligible_count, 1)
        self.assertEqual(report.stages[-1].condition, 'Age 25')

    def test_audio_report_preserves_combined_speaker_restrictions(self):
        audios, report = phraser.sample_audios(store=self.store, n_audios=1,
            dataset='cgn', language='nld', dialect='nl-NL',
            min_duration_ms=2000, speakers=[self.older], gender='female',
            insufficient='drop', return_report=True)
        self.assertEqual(audios, [])
        self.assertEqual(report.starting_count, 2)
        self.assertEqual(report.stages[-2].condition, 'Speaker selection')
        self.assertEqual(report.stages[-2].removed, 1)
        self.assertEqual(report.stages[-1].condition, 'Gender female')
        self.assertEqual(report.stages[-1].removed, 1)
        self.assertEqual(report.shortfall, 1)

    def test_report_empty_inputs_zero_request_and_invalid_option(self):
        audios, report = phraser.sample_audios(audios=[], n_audios=0,
            return_report=True)
        self.assertEqual(audios, [])
        self.assertEqual(report.starting_count, 0)
        self.assertEqual(report.stages, [])
        self.assertEqual(report.eligible_count, 0)
        speakers, report = phraser.sample_speakers(store=self.store,
            n_speakers=0, return_report=True)
        self.assertEqual(report.eligible_count, 3)
        self.assertEqual(report.unselected_count, 3)
        with self.assertRaisesRegex(ValueError, 'return_report'):
            phraser.sample_audios(audios=[], n_audios=0, return_report=1)

    def test_report_rejects_inconsistent_stage_counts(self):
        report = phraser.SelectionReport('words', 10, 2)
        report.add_stage('Speaker selection', 10, 5)
        with self.assertRaises(ValueError):
            report.add_stage('Gender female', 10, 4)
        with self.assertRaises(ValueError):
            report.add_stage('Gender female', 5, 6)


if __name__ == '__main__':
    unittest.main()
