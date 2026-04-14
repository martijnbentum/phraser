import importlib.util
import types
import unittest
from pathlib import Path

import numpy as np

FAKE_MODEL = object()


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_segment(key=b'\x01\x02', start=1100, end=1400,
                 filename='audio.wav', duration=2000):
    audio = types.SimpleNamespace(filename=filename, duration=duration)
    return types.SimpleNamespace(key=key, start=start, end=end, audio=audio)


class FakeSegment:
    def __init__(self, key=b'\x01\x02', start=1100, end=1400,
                 filename='audio.wav', duration=2000):
        self.key = key
        self.start = start
        self.end = end
        self.audio = types.SimpleNamespace(filename=filename,
                                           duration=duration)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return isinstance(other, FakeSegment) and self.key == other.key


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
        self._old[name] = getattr(self.module, name)
        setattr(self.module, name, fake)

    def tearDown(self):
        for name, old in self._old.items():
            setattr(self.module, name, old)


# ── F2 tests ─────────────────────────────────────────────────────────────────

class TestFrameAggregationParam(ModuleFixture):

    def setUp(self):
        super().setUp()
        self._patch('to_vector', make_fake_to_vector())
        self._patch('frame', make_fake_frame_module())

    def test_get_embeddings_accepts_frame_aggregation_param(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation=None,
            store=store, model=FAKE_MODEL)
        self.assertIsNotNone(result)

    def test_get_embeddings_old_aggregation_param_raises(self):
        store = FakeStore()
        with self.assertRaises(TypeError):
            self.module.get_embeddings(
                make_segment(), layers=4, aggregation=None,
                store=store, model=FAKE_MODEL)

    def test_frame_aggregation_none_dims_contain_frames(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation=None,
            store=store, model=FAKE_MODEL)
        self.assertIn('frames', result.dims)

    def test_frame_aggregation_mean_dims_no_frames(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='mean',
            store=store, model=FAKE_MODEL)
        self.assertNotIn('frames', result.dims)

    def test_frame_aggregation_mean_stored_on_embeddings(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='mean',
            store=store, model=FAKE_MODEL)
        self.assertEqual(result.frame_aggregation, 'mean')

    def test_frame_aggregation_centroid_dims_no_frames(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='centroid',
            store=store, model=FAKE_MODEL)
        self.assertNotIn('frames', result.dims)

    def test_frame_aggregation_centroid_stored_on_embeddings(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='centroid',
            store=store, model=FAKE_MODEL)
        self.assertEqual(result.frame_aggregation, 'centroid')

    def test_invalid_frame_aggregation_raises(self):
        store = FakeStore()
        with self.assertRaises(ValueError):
            self.module.get_embeddings(
                make_segment(), layers=4, frame_aggregation='unknown',
                store=store, model=FAKE_MODEL)


if __name__ == '__main__':
    unittest.main()
