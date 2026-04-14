import importlib.util
import sys
import types
import unittest
import warnings
from pathlib import Path

import numpy as np


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_segment(key='seg-001', start=1100, end=1400,
                 filename='audio.wav', duration=2000):
    audio = types.SimpleNamespace(filename=filename, duration=duration)
    return types.SimpleNamespace(key=key, start=start, end=end, audio=audio)


class FakeFrame:
    def __init__(self, index):
        self.index = index


class FakeFrames:
    def select_frames(self, start_time, end_time, percentage_overlap=None):
        return [FakeFrame(0), FakeFrame(1), FakeFrame(2)]


def make_fake_frame_module():
    return types.SimpleNamespace(
        make_frames_from_outputs=lambda outputs, **kwargs: FakeFrames())


class FakeStore:
    def __init__(self, stored=None):
        self._stored = dict(stored or {})
        self.put_calls = []

    def exists(self, phraser_key, collar, model_name, output_type, layer,
               match='exact'):
        return (phraser_key, collar, model_name, output_type, layer) \
            in self._stored

    def put(self, phraser_key, collar, model_name, output_type, layer,
            data, tags=None):
        self.put_calls.append(
            (phraser_key, collar, model_name, output_type, layer))
        self._stored[(phraser_key, collar, model_name, output_type, layer)] \
            = data

    def load(self, phraser_key, collar, model_name, output_type, layer,
             match='exact'):
        key = (phraser_key, collar, model_name, output_type, layer)
        if key not in self._stored:
            raise ValueError('not found in fake store')
        return self._stored[key]


def make_fake_to_vector(n_layers=13, n_frames=5, hidden_dim=8):
    def filename_to_vector(filename, start, end, model, gpu, numpify_output):
        hidden_states = [
            np.zeros((1, n_frames, hidden_dim)) for _ in range(n_layers)
        ]
        return types.SimpleNamespace(hidden_states=hidden_states)

    return types.SimpleNamespace(filename_to_vector=filename_to_vector)


class ModuleFixture(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self._old = {}

    def _patch(self, name, fake):
        self._old[name] = sys.modules.get(name)
        sys.modules[name] = fake

    def tearDown(self):
        for name, old in self._old.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


# ── F3 tests ──────────────────────────────────────────────────────────────────

class TestGetEmbeddingsBatchReturnType(ModuleFixture):

    def setUp(self):
        super().setUp()
        self._patch('to_vector', make_fake_to_vector())
        self._patch('frame', make_fake_frame_module())

    def _make_store_with_segment(self, key, layer, collar=500,
                                 model_name='wav2vec2',
                                 n_frames=3, hidden_dim=8):
        store = FakeStore()
        data = np.zeros((n_frames, hidden_dim))
        store._stored[(key, collar, model_name, 'hidden_state', layer)] = data
        return store

    def test_batch_returns_token_embeddings_instance(self):
        from echoframe import TokenEmbeddings
        seg = make_segment(key='seg-a')
        store = self._make_store_with_segment('seg-a', layer=4)
        result = self.module.get_embeddings_batch(
            [seg], layers=4, store=store, model='dummy')
        self.assertIsInstance(result, TokenEmbeddings)

    def test_batch_token_count_matches_segment_count(self):
        seg_a = make_segment(key='seg-a', filename='audio.wav')
        seg_b = make_segment(key='seg-b', filename='audio.wav')
        store = FakeStore()
        for key in ('seg-a', 'seg-b'):
            store._stored[(key, 500, 'wav2vec2', 'hidden_state', 4)] = \
                np.zeros((3, 8))
        result = self.module.get_embeddings_batch(
            [seg_a, seg_b], layers=4, store=store, model='dummy')
        self.assertEqual(result.token_count, 2)

    def test_batch_token_order_matches_segment_order(self):
        from echoframe.metadata import EchoframeMetadata
        seg_a = make_segment(key='seg-first', filename='audio.wav')
        seg_b = make_segment(key='seg-second', filename='audio.wav')
        store = FakeStore()
        for key in ('seg-first', 'seg-second'):
            store._stored[(key, 500, 'wav2vec2', 'hidden_state', 4)] = \
                np.zeros((3, 8))
        result = self.module.get_embeddings_batch(
            [seg_a, seg_b], layers=4, store=store, model='dummy')
        expected_first = EchoframeMetadata(
            phraser_key='seg-first', collar=500, model_name='wav2vec2',
            output_type='hidden_state', layer=4).echoframe_key
        expected_second = EchoframeMetadata(
            phraser_key='seg-second', collar=500, model_name='wav2vec2',
            output_type='hidden_state', layer=4).echoframe_key
        self.assertEqual(result.echoframe_keys[0], expected_first)
        self.assertEqual(result.echoframe_keys[1], expected_second)

    def test_batch_duplicate_segment_key_triggers_warning(self):
        seg_a = make_segment(key='seg-dup', filename='audio.wav')
        seg_b = make_segment(key='seg-dup', filename='audio.wav')
        store = FakeStore()
        store._stored[('seg-dup', 500, 'wav2vec2', 'hidden_state', 4)] = \
            np.zeros((3, 8))
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            result = self.module.get_embeddings_batch(
                [seg_a, seg_b], layers=4, store=store, model='dummy')
        self.assertTrue(
            any('duplicate' in str(w.message).lower() for w in caught),
            msg='Expected a duplicate warning but none was raised')
        self.assertEqual(result.token_count, 1)

    def test_batch_echoframe_keys_one_per_token(self):
        seg_a = make_segment(key='seg-x', filename='audio.wav')
        seg_b = make_segment(key='seg-y', filename='audio.wav')
        store = FakeStore()
        for key in ('seg-x', 'seg-y'):
            store._stored[(key, 500, 'wav2vec2', 'hidden_state', 4)] = \
                np.zeros((3, 8))
        result = self.module.get_embeddings_batch(
            [seg_a, seg_b], layers=4, store=store, model='dummy')
        self.assertEqual(len(result.echoframe_keys), result.token_count)


if __name__ == '__main__':
    unittest.main()
