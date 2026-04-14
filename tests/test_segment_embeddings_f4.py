import importlib.util
import unittest
from pathlib import Path

import numpy as np
from echoframe.metadata import EchoframeMetadata


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_key(phraser_key, collar, model_name, layer):
    return EchoframeMetadata(
        phraser_key=phraser_key,
        collar=collar,
        model_name=model_name,
        output_type='hidden_state',
        layer=layer,
    ).echoframe_key


class TestBuildEmbeddingsF4(unittest.TestCase):

    def setUp(self):
        self.module = load_module()

    def _single_layer_arrays(self):
        return [np.random.rand(5, 8)]

    def _multi_layer_arrays(self, n=3):
        return [np.random.rand(5, 8) for _ in range(n)]

    def test_build_embeddings_single_layer_key_forwarded(self):
        arrays = self._single_layer_arrays()
        layers_list = [4]
        key = _make_key('seg-abc', 500, 'wav2vec2', 4)
        echoframe_keys = (key,)

        emb = self.module._build_embeddings(
            arrays, layers_list, single_layer=True,
            frame_aggregation=None, echoframe_keys=echoframe_keys)

        self.assertEqual(emb.echoframe_keys, echoframe_keys)

    def test_build_embeddings_multi_layer_keys_forwarded(self):
        layers_list = [2, 4, 6]
        arrays = self._multi_layer_arrays(n=len(layers_list))
        echoframe_keys = tuple(
            _make_key('seg-xyz', 250, 'hubert', l) for l in layers_list)

        emb = self.module._build_embeddings(
            arrays, layers_list, single_layer=False,
            frame_aggregation=None, echoframe_keys=echoframe_keys)

        self.assertEqual(emb.echoframe_keys, echoframe_keys)

    def test_build_embeddings_single_layer_echoframe_keys_length_one(self):
        arrays = self._single_layer_arrays()
        layers_list = [3]
        key = _make_key('seg-def', 500, 'wav2vec2', 3)
        echoframe_keys = (key,)

        emb = self.module._build_embeddings(
            arrays, layers_list, single_layer=True,
            frame_aggregation=None, echoframe_keys=echoframe_keys)

        self.assertEqual(len(emb.echoframe_keys), 1)

    def test_build_embeddings_multi_layer_keys_length_matches_layers(self):
        layers_list = [1, 3, 5, 7]
        arrays = self._multi_layer_arrays(n=len(layers_list))
        echoframe_keys = tuple(
            _make_key('seg-multi', 500, 'wav2vec2', l) for l in layers_list)

        emb = self.module._build_embeddings(
            arrays, layers_list, single_layer=False,
            frame_aggregation=None, echoframe_keys=echoframe_keys)

        self.assertEqual(len(emb.echoframe_keys), len(layers_list))


if __name__ == '__main__':
    unittest.main()
