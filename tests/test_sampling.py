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
        '''Build a complete phrase with one word, syllable, and phone.'''
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

    def test_recorded_outlier_states_at_every_segment_level(self):
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
                    result = function(token.label, audios=[self.second],
                        n_tokens=1, insufficient='drop')
                    expected = [] if token.is_duration_outlier else [token]
                    self.assertEqual(result, expected)
                    result = function(token.label, audios=[self.second],
                        n_tokens=1, exclude_duration_outliers=False)
                    self.assertEqual(result, [token])
                    self.assertEqual(self.store.DB.load(token.key), raw)
                    self.assertEqual(token.duration_outlier, state)

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
            n_tokens=1, exclude_duration_outliers=False, max_duration_ms=200,
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
            {'exclude_duration_outliers': None}, {'gender': 'invalid'},
            {'insufficient': 'anything'}]
        for kwargs in options:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                phraser.sample_word_tokens('huis', audios=[], **kwargs)
        with self.assertRaises(ValueError):
            phraser.sample_speakers(audios=[], min_age=50, max_age=20)


if __name__ == '__main__':
    unittest.main()
