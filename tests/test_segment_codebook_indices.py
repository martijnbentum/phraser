import importlib.util
import sys
import types
import unittest
from pathlib import Path

import numpy as np
from echoframe import Codebook, TokenCodebooks


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    sys.modules.setdefault('frame', types.SimpleNamespace())
    if 'to_vector' not in sys.modules:
        package = types.ModuleType('to_vector')
        package.__path__ = []
        sys.modules['to_vector'] = package
    if 'to_vector.model_registry' not in sys.modules:
        sys.modules['to_vector.model_registry'] = types.SimpleNamespace(
            filename_model_type=lambda model: 'spidr'
            if str(model) == 'spidr' else None,
            model_to_type=lambda model: 'spidr'
            if model == 'spidr' else 'wav2vec2-pretraining',
        )
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_segment(key=b'\x01\x02', start=1100, end=1400,
    filename='audio.wav', duration=2000):
    audio = types.SimpleNamespace(filename=filename, duration=duration)
    return types.SimpleNamespace(key=key, start=start, end=end, audio=audio)


class FakeFrame:
    def __init__(self, index):
        self.index = index


class FakeFrames:
    def select_frames(self, start_time, end_time, percentage_overlap=None):
        return [FakeFrame(0), FakeFrame(1), FakeFrame(2)]


def make_fake_frame_module(frames=None):
    fake_frames = frames or FakeFrames()
    return types.SimpleNamespace(
        make_frames_from_outputs=lambda outputs, **kwargs: fake_frames)


class FakeMetadata:
    def __init__(self, entry_id):
        self.entry_id = entry_id


class FakeStore:
    def __init__(self, stored=None):
        self._stored = dict(stored or {})
        self.put_calls = []
        self.load_calls = []

    def _entry_id(self, key):
        return '|'.join(map(str, key))

    def exists(self, phraser_key, collar, model_name, output_type, layer,
        match='exact'):
        return (phraser_key, collar, model_name, output_type, layer) in (
            self._stored)

    def put(self, phraser_key, collar, model_name, output_type, layer, data,
        tags=None):
        key = (phraser_key, collar, model_name, output_type, layer)
        self.put_calls.append(key)
        self._stored[key] = np.asarray(data)
        return FakeMetadata(self._entry_id(key))

    def find_one(self, phraser_key, collar, model_name, output_type, layer,
        match='exact'):
        key = (phraser_key, collar, model_name, output_type, layer)
        if key not in self._stored:
            return None
        return FakeMetadata(self._entry_id(key))

    def load(self, phraser_key, collar, model_name, output_type, layer,
        match='exact'):
        key = (phraser_key, collar, model_name, output_type, layer)
        self.load_calls.append(key)
        return self._stored[key]

    def load_with_echoframe_key(self, echoframe_key):
        for key, value in self._stored.items():
            if self._entry_id(key) == echoframe_key:
                return value
        raise ValueError('echoframe_key not found')


def make_fake_to_vector(artifacts, counter=None):
    def filename_to_vector(filename, start, end, model, gpu,
        numpify_output):
        return types.SimpleNamespace(hidden_states=[np.zeros((1, 4, 3))])

    def filename_to_codebook_artifacts(audio_filename, start, end, model,
        gpu):
        if counter is not None:
            counter['n'] += 1
        return artifacts

    return types.SimpleNamespace(
        filename_to_vector=filename_to_vector,
        filename_to_codebook_artifacts=filename_to_codebook_artifacts,
    )


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


class TestGetCodebookIndices(ModuleFixture):
    def setUp(self):
        super().setUp()
        self._patch('frame', make_fake_frame_module())

    def test_cache_miss_stores_selected_wav2vec2_frames(self):
        artifacts = types.SimpleNamespace(
            indices=np.array([[0, 3], [1, 2], [2, 1], [3, 0]]),
            codebook_matrix=np.array([
                [1.0, 2.0],
                [3.0, 4.0],
                [5.0, 6.0],
                [7.0, 8.0],
            ]),
            model_architecture='wav2vec2',
        )
        self._patch('to_vector', make_fake_to_vector(artifacts))
        store = FakeStore()

        result = self.module.get_codebook_indices(
            make_segment(), store=store, model='dummy')

        self.assertIsInstance(result, Codebook)
        stored = store._stored[('0102', 500, 'wav2vec2',
            'codebook_indices', 0)]
        np.testing.assert_array_equal(stored, np.array([
            [0, 3],
            [1, 2],
            [2, 1],
        ]))
        np.testing.assert_array_equal(result.to_codevectors(), np.array([
            [1.0, 2.0, 7.0, 8.0],
            [3.0, 4.0, 5.0, 6.0],
            [5.0, 6.0, 3.0, 4.0],
        ]))

    def test_cache_hit_skips_codebook_compute(self):
        artifacts = types.SimpleNamespace(
            indices=np.array([[0, 1]]),
            codebook_matrix=np.array([[1.0], [2.0]]),
            model_architecture='wav2vec2',
        )
        counter = {'n': 0}
        self._patch('to_vector', make_fake_to_vector(artifacts, counter))
        store = FakeStore(stored={
            ('0102', 500, 'wav2vec2', 'codebook_indices', 0):
                np.array([[0, 1]]),
            ('0102', 500, 'wav2vec2', 'codebook_matrix', 0):
                np.array([[1.0], [2.0]]),
        })

        self.module.get_codebook_indices(
            make_segment(), store=store, model='dummy')

        self.assertEqual(counter['n'], 0)

    def test_spidr_returns_frame_major_indices(self):
        artifacts = types.SimpleNamespace(
            indices=np.array([[1, 0], [0, 1], [1, 1], [0, 0]]),
            codebook_matrix=np.array([
                [[1.0], [2.0]],
                [[3.0], [4.0]],
            ]),
            model_architecture='spidr',
        )
        self._patch('to_vector', make_fake_to_vector(artifacts))
        store = FakeStore()

        result = self.module.get_codebook_indices(
            make_segment(), store=store, model='spidr')

        np.testing.assert_array_equal(result.data, np.array([
            [1, 0],
            [0, 1],
            [1, 1],
        ]))
        np.testing.assert_array_equal(result.to_codevectors(), np.array([
            [[2.0], [3.0]],
            [[1.0], [4.0]],
            [[2.0], [4.0]],
        ]))


class TestGetCodebookIndicesBatch(ModuleFixture):
    def setUp(self):
        super().setUp()
        self._patch('frame', make_fake_frame_module())

    def test_batch_returns_token_collection(self):
        artifacts = types.SimpleNamespace(
            indices=np.array([[0, 1], [1, 0], [0, 1], [1, 0]]),
            codebook_matrix=np.array([[1.0], [2.0]]),
            model_architecture='wav2vec2',
        )
        self._patch('to_vector', make_fake_to_vector(artifacts))
        store = FakeStore()

        result = self.module.get_codebook_indices_batch([
            make_segment(key=b'\x01'),
            make_segment(key=b'\x02', start=1200, end=1500),
        ], store=store, model='dummy')

        self.assertIsInstance(result, TokenCodebooks)
        self.assertEqual(result.token_count, 2)


if __name__ == '__main__':
    unittest.main()
