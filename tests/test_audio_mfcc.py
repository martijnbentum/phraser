import tempfile
import unittest
import warnings
from pathlib import Path
from unittest import mock

import numpy as np
import soundfile

import phraser.audio.batch as audio_batch
import phraser.audio.mfcc as mfcc_module
from phraser.audio.batch import mfcc_batch
from phraser.audio.mfcc import mfcc, recording_mfcc, slice_recording_mfcc
from phraser.models import Audio, Phone, Phrase, Syllable, Word


SAMPLE_RATE = 16000
AUDIO_DURATION = 1000
IDENTITY = {'audio_id': b'\x01' * 8, 'speaker_id': b'\x02' * 8}


class TestMfcc(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.filename = str(Path(cls.directory.name) / 'features.wav')
        times = np.arange(SAMPLE_RATE, dtype=np.float32) / SAMPLE_RATE
        phase = 2 * np.pi * (180 * times + 300 * times ** 2)
        signal = 0.4 * np.sin(phase)
        soundfile.write(cls.filename, signal, SAMPLE_RATE, subtype='FLOAT')
        cls.audio = Audio(filename=cls.filename, duration=AUDIO_DURATION,
            sample_rate=SAMPLE_RATE, n_channels=1)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def make_segment(self, start, end, segment_class=Phrase):
        segment = segment_class(
            label='test', start=start, end=end, **IDENTITY)
        segment._audio = self.audio
        return segment

    def test_supports_each_segment_class(self):
        for segment_class in (Phrase, Word, Syllable, Phone):
            with self.subTest(segment_class=segment_class.__name__):
                segment = self.make_segment(
                    100, 200, segment_class=segment_class)
                result = mfcc(segment)
                self.assertEqual(result.shape, (39, 6))
                self.assertEqual(result.dtype, np.float32)

    def test_full_rate_has_twice_the_aligned_frames(self):
        segment = self.make_segment(100, 200)
        aligned = mfcc(segment)
        full_rate = mfcc(segment, wav2vec2_frames=False)
        self.assertEqual(aligned.shape, (39, 6))
        self.assertEqual(full_rate.shape, (39, 12))
        np.testing.assert_allclose(aligned, full_rate[:, ::2],
            rtol=1e-5, atol=1e-5)

    def test_segment_property_computes_once_and_returns_cached_matrix(self):
        segment = self.make_segment(100, 200)
        compute = mfcc_module.mfcc
        with mock.patch.object(
            mfcc_module, 'mfcc', wraps=compute) as compute_mfcc:
            first = segment.mfcc
            second = segment.mfcc
        self.assertIs(first, second)
        self.assertIs(first, segment._mfcc)
        self.assertEqual(compute_mfcc.call_count, 1)

    def test_recording_matrix_can_be_sliced_at_both_frame_rates(self):
        segment = self.make_segment(100, 200)
        recording = recording_mfcc(self.audio)
        self.assertEqual(recording.shape, (39, 98))
        aligned = slice_recording_mfcc(recording, segment)
        full_rate = slice_recording_mfcc(
            recording, segment, wav2vec2_frames=False)
        expected_aligned = mfcc(segment)
        expected_full_rate = mfcc(segment, wav2vec2_frames=False)
        np.testing.assert_allclose(aligned, expected_aligned,
            rtol=1e-5, atol=1e-5)
        np.testing.assert_allclose(full_rate, expected_full_rate,
            rtol=1e-5, atol=1e-5)

    def test_context_matches_corresponding_broader_segment_frames(self):
        inner_segment = self.make_segment(200, 300)
        broader_segment = self.make_segment(100, 400)
        inner = mfcc(inner_segment, wav2vec2_frames=False)
        broader = mfcc(broader_segment, wav2vec2_frames=False)
        np.testing.assert_allclose(inner, broader[:, 10:22],
            rtol=1e-5, atol=1e-5)

    def test_short_segment_at_recording_start_uses_boundary_padding(self):
        segment = self.make_segment(0, 5)
        result = mfcc(segment)
        self.assertEqual(result.shape, (39, 1))
        finite = np.isfinite(result)
        self.assertTrue(finite.all())

    def test_short_segment_at_recording_end_uses_boundary_padding(self):
        segment = self.make_segment(975, 985)
        result = mfcc(segment)
        self.assertEqual(result.shape, (39, 1))
        finite = np.isfinite(result)
        self.assertTrue(finite.all())

    def test_rejects_non_positive_duration(self):
        segment = self.make_segment(100, 100)
        with self.assertRaisesRegex(ValueError, 'positive duration'):
            mfcc(segment)

    def test_rejects_segment_outside_audio(self):
        segment = self.make_segment(990, 1010)
        with self.assertRaisesRegex(ValueError, 'fit within its audio'):
            mfcc(segment)

    def test_rejects_range_without_wav2vec2_aligned_frame(self):
        segment = self.make_segment(986, 1000)
        with self.assertRaisesRegex(ValueError, 'no complete aligned'):
            mfcc(segment)

    def test_requires_boolean_frame_flag(self):
        segment = self.make_segment(100, 200)
        with self.assertRaisesRegex(TypeError, 'must be a boolean'):
            mfcc(segment, wav2vec2_frames=20)

    def test_batch_matches_individual_extraction_in_input_order(self):
        segments = [
            self.make_segment(300, 400, Phone),
            self.make_segment(100, 200, Word),
            self.make_segment(150, 250, Syllable),
        ]
        expected = [mfcc(segment) for segment in segments]
        compute = audio_batch.mfcc_module._mfcc_frame_range
        with mock.patch.object(
            audio_batch.mfcc_module, '_mfcc_frame_range',
            wraps=compute) as compute_range:
            results = mfcc_batch(segments, workers=1)
        self.assertEqual(compute_range.call_count, 1)
        for result, expected_matrix in zip(results, expected):
            np.testing.assert_allclose(
                result, expected_matrix, rtol=1e-5, atol=1e-5)
        for segment, result in zip(segments, results):
            self.assertIs(segment._mfcc, result)

    def test_batch_uses_existing_segment_cache(self):
        segment = self.make_segment(100, 200)
        cached = segment.mfcc
        compute = audio_batch.mfcc_module._mfcc_frame_range
        with mock.patch.object(
            audio_batch.mfcc_module, '_mfcc_frame_range',
            wraps=compute) as compute_range:
            results = mfcc_batch([segment], workers=1)
        self.assertEqual(compute_range.call_count, 0)
        self.assertIs(results[0], cached)

    def test_batch_can_skip_segment_cache(self):
        segment = self.make_segment(100, 200)
        results = mfcc_batch(
            [segment], workers=1, cache_on_segment=False)
        has_cache = hasattr(segment, '_mfcc')
        expected = mfcc(segment)
        self.assertFalse(has_cache)
        np.testing.assert_allclose(results[0], expected,
            rtol=1e-5, atol=1e-5)

    def test_cache_warning_is_emitted_once(self):
        first = self.make_segment(100, 200)
        second = self.make_segment(200, 300)
        with mock.patch.object(
            audio_batch, '_CACHE_WARNING_EMITTED', False):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                mfcc_batch([first], workers=1)
                mfcc_batch([second], workers=1)
        cache_warnings = []
        for item in caught:
            message = str(item.message)
            if 'cached on segments' in message: cache_warnings.append(item)
        self.assertEqual(len(cache_warnings), 1)

    def test_full_rate_batch_warns_and_does_not_update_cache(self):
        segment = self.make_segment(100, 200)
        with mock.patch.object(
            audio_batch, '_FULL_RATE_WARNING_EMITTED', False):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                mfcc_batch(
                    [segment], wav2vec2_frames=False, workers=1)
        has_cache = hasattr(segment, '_mfcc')
        self.assertFalse(has_cache)
        messages = [str(item.message) for item in caught]
        self.assertIn(
            '10 ms MFCC results do not update segment.mfcc caches',
            messages)

    def test_batch_supports_parallel_audio_groups(self):
        second_filename = str(
            Path(self.directory.name) / 'second-features.wav')
        signal, sample_rate = soundfile.read(
            self.filename, dtype='float32')
        soundfile.write(
            second_filename, signal, sample_rate, subtype='FLOAT')
        second_audio = Audio(
            filename=second_filename, duration=AUDIO_DURATION,
            sample_rate=SAMPLE_RATE, n_channels=1)
        first = self.make_segment(100, 200, Phrase)
        second = self.make_segment(200, 300, Phrase)
        second._audio = second_audio
        results = mfcc_batch([second, first], workers=2)
        expected_second = mfcc(second)
        expected_first = mfcc(first)
        np.testing.assert_allclose(results[0], expected_second,
            rtol=1e-5, atol=1e-5)
        np.testing.assert_allclose(results[1], expected_first,
            rtol=1e-5, atol=1e-5)

    def test_empty_batch_returns_empty_list(self):
        result = mfcc_batch([], workers=2)
        self.assertEqual(result, [])

    def test_batch_validates_worker_count(self):
        segment = self.make_segment(100, 200)
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            mfcc_batch([segment], workers=0)

    def test_batch_validates_cache_flag(self):
        segment = self.make_segment(100, 200)
        with self.assertRaisesRegex(TypeError, 'must be a boolean'):
            mfcc_batch([segment], cache_on_segment='yes')
