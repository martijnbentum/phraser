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

    def test_import_embeddings_returns_embeddings_class(self):
        from echoframe import Embeddings
        result = self.module._import_embeddings()
        self.assertIs(result, Embeddings)

    def test_import_token_embeddings_returns_token_embeddings_class(self):
        from echoframe import TokenEmbeddings
        result = self.module._import_token_embeddings()
        self.assertIs(result, TokenEmbeddings)

    def test_import_metadata_returns_echoframe_metadata_class(self):
        from echoframe.metadata import EchoframeMetadata
        result = self.module._import_metadata()
        self.assertIs(result, EchoframeMetadata)

    def test_missing_echoframe_raises_import_error_with_message(self):
        # Setting a module to None in sys.modules causes ImportError when
        # Python tries to do 'from <module> import <name>'.
        self._patch('echoframe', None)
        self._patch('echoframe.metadata', None)

        # Reload so the helpers perform fresh imports against patched sys.modules
        module = load_module()

        with self.assertRaises(ImportError) as ctx:
            module._import_embeddings()
        self.assertIn('echoframe', str(ctx.exception).lower())

        with self.assertRaises(ImportError) as ctx:
            module._import_token_embeddings()
        self.assertIn('echoframe', str(ctx.exception).lower())

        with self.assertRaises(ImportError) as ctx:
            module._import_metadata()
        self.assertIn('echoframe', str(ctx.exception).lower())


if __name__ == '__main__':
    unittest.main()
