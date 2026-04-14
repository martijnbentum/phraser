import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest import mock


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    original = sys.modules.get('echoframe')
    fake_segment_features = types.SimpleNamespace(
        get_embeddings=mock.Mock(),
        get_embeddings_batch=mock.Mock(),
        get_codebook_indices=mock.Mock(),
        get_codebook_indices_batch=mock.Mock(),
    )
    sys.modules['echoframe'] = types.SimpleNamespace(
        segment_features=fake_segment_features)
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if original is None:
        del sys.modules['echoframe']
    else:
        sys.modules['echoframe'] = original
    return module


def make_segment():
    audio = types.SimpleNamespace(filename='audio.wav', duration=2000)
    return types.SimpleNamespace(key=b'\x01\x02', start=1100, end=1400,
        audio=audio)


class TestForwarders(unittest.TestCase):
    def setUp(self):
        self.module = load_module()

    def test_get_embeddings_forwards_all_arguments(self):
        segment = make_segment()
        store = object()
        model = object()
        tags = ['a', 'b']
        expected = object()
        with mock.patch.object(self.module.segment_features, 'get_embeddings',
            return_value=expected) as patched:
            result = self.module.get_embeddings(segment, [4, 6], collar=750,
                model_name='spidr', frame_aggregation='mean', model=model,
                store=store, store_root='custom-store', gpu=True, tags=tags)

        self.assertIs(result, expected)
        patched.assert_called_once_with(segment, [4, 6], collar=750,
            model_name='spidr', frame_aggregation='mean', model=model,
            store=store, store_root='custom-store', gpu=True, tags=tags)

    def test_get_embeddings_batch_forwards_all_arguments(self):
        segments = [make_segment(), make_segment()]
        store = object()
        model = object()
        expected = object()
        with mock.patch.object(self.module.segment_features,
            'get_embeddings_batch', return_value=expected) as patched:
            result = self.module.get_embeddings_batch(segments, 4,
                collar=250, model_name='wav2vec2',
                frame_aggregation='centroid', model=model, store=store,
                store_root='custom-store', gpu=False, tags=['x'])

        self.assertIs(result, expected)
        patched.assert_called_once_with(segments, 4, collar=250,
            model_name='wav2vec2', frame_aggregation='centroid',
            model=model, store=store, store_root='custom-store',
            gpu=False, tags=['x'])

    def test_get_codebook_indices_forwards_all_arguments(self):
        segment = make_segment()
        store = object()
        model = object()
        expected = object()
        with mock.patch.object(self.module.segment_features,
            'get_codebook_indices', return_value=expected) as patched:
            result = self.module.get_codebook_indices(segment, collar=900,
                model_name='spidr', model=model, store=store,
                store_root='custom-store', gpu=True, tags=['tag'])

        self.assertIs(result, expected)
        patched.assert_called_once_with(segment, collar=900,
            model_name='spidr', model=model, store=store,
            store_root='custom-store', gpu=True, tags=['tag'])

    def test_get_codebook_indices_batch_forwards_all_arguments(self):
        segments = [make_segment()]
        store = object()
        model = object()
        expected = object()
        with mock.patch.object(self.module.segment_features,
            'get_codebook_indices_batch', return_value=expected) as patched:
            result = self.module.get_codebook_indices_batch(segments,
                collar=300, model_name='wav2vec2', model=model, store=store,
                store_root='custom-store', gpu=False, tags=['tag'])

        self.assertIs(result, expected)
        patched.assert_called_once_with(segments, collar=300,
            model_name='wav2vec2', model=model, store=store,
            store_root='custom-store', gpu=False, tags=['tag'])


if __name__ == '__main__':
    unittest.main()
