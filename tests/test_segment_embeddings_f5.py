import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


class TestImportHelpers(ModuleFixture):

    def test_module_exports_embeddings_class(self):
        from echoframe import Embeddings
        result = self.module.Embeddings
        self.assertIs(result, Embeddings)

    def test_module_exports_token_embeddings_class(self):
        from echoframe import TokenEmbeddings
        result = self.module.TokenEmbeddings
        self.assertIs(result, TokenEmbeddings)

    def test_module_exports_metadata_class(self):
        from echoframe.metadata import EchoframeMetadata
        result = self.module.EchoframeMetadata
        self.assertIs(result, EchoframeMetadata)

    def test_missing_echoframe_raises_import_error_on_module_load(self):
        self._patch('echoframe', None)
        self._patch('echoframe.metadata', None)

        with self.assertRaises(ImportError) as ctx:
            load_module()
        self.assertIn('echoframe', str(ctx.exception).lower())


if __name__ == '__main__':
    unittest.main()
