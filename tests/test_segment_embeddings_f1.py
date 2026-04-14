import importlib.util
import unittest
from pathlib import Path

from echoframe.metadata import EchoframeMetadata


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestMakeEchoframeKey(unittest.TestCase):

    def setUp(self):
        self.module = load_module()

    def test_make_echoframe_key_returns_nonempty_string(self):
        key = self.module._make_echoframe_key(
            phraser_key='seg-abc',
            collar=500,
            model_name='wav2vec2',
            layer=3,
        )
        self.assertIsInstance(key, str)
        self.assertTrue(len(key) > 0)

    def test_make_echoframe_key_matches_metadata_computation(self):
        phraser_key = 'seg-xyz'
        collar = 250
        model_name = 'hubert'
        layer = 7

        expected = EchoframeMetadata(
            phraser_key=phraser_key,
            collar=collar,
            model_name=model_name,
            output_type='hidden_state',
            layer=layer,
        ).echoframe_key

        result = self.module._make_echoframe_key(
            phraser_key=phraser_key,
            collar=collar,
            model_name=model_name,
            layer=layer,
        )

        self.assertEqual(result, expected)

    def test_make_echoframe_key_different_layers_produce_different_keys(self):
        kwargs = dict(phraser_key='seg-abc', collar=500, model_name='wav2vec2')
        key_layer3 = self.module._make_echoframe_key(layer=3, **kwargs)
        key_layer5 = self.module._make_echoframe_key(layer=5, **kwargs)
        self.assertNotEqual(key_layer3, key_layer5)

    def test_make_echoframe_key_different_collars_produce_different_keys(self):
        kwargs = dict(phraser_key='seg-abc', model_name='wav2vec2', layer=3)
        key_collar100 = self.module._make_echoframe_key(collar=100, **kwargs)
        key_collar500 = self.module._make_echoframe_key(collar=500, **kwargs)
        self.assertNotEqual(key_collar100, key_collar500)


if __name__ == '__main__':
    unittest.main()
