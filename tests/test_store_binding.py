import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store, UnboundStoreError, open_store
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word
from phraser.query import QuerySet


class TestStoreBinding(unittest.TestCase):
    """High-priority tests for the store-binding refactor (store_by_codex branch)."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            cls.store = open_store(path=cls._tmpdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    # ------------------------------------------------------------------ #
    # 1. open_store attaches all six query roots
    # ------------------------------------------------------------------ #

    def test_open_store_attaches_query_roots(self):
        for attr in ('audios', 'phrases', 'words', 'syllables', 'phones', 'speakers'):
            with self.subTest(attr=attr):
                self.assertTrue(hasattr(self.store, attr),
                                f'store missing query root: {attr}')
                self.assertIsInstance(getattr(self.store, attr), QuerySet)

    # ------------------------------------------------------------------ #
    # 2. store.create_*() factory methods bind objects to the store
    # ------------------------------------------------------------------ #

    def test_store_create_methods_bind_objects(self):
        objs = {
            'Audio':    self.store.create_audio(filename='bind.wav', save=False),
            'Phrase':   self.store.create_phrase(label='bp', start=0, end=100, save=False),
            'Word':     self.store.create_word(label='bw', start=0, end=50, save=False),
            'Syllable': self.store.create_syllable(label='bs', start=0, end=50, save=False),
            'Phone':    self.store.create_phone(label='t', start=0, end=50, save=False),
            'Speaker':  self.store.create_speaker(name='bs', dataset='test', save=False),
        }
        for class_name, obj in objs.items():
            with self.subTest(class_name=class_name):
                self.assertIs(obj._store, self.store)

    # ------------------------------------------------------------------ #
    # 3. Direct constructor with store= binds without using the factory
    # ------------------------------------------------------------------ #

    def test_explicit_store_constructor_binding(self):
        word = Word(label='ctor', start=0, end=100, store=self.store, save=False)
        self.assertIs(word._store, self.store)

    # ------------------------------------------------------------------ #
    # 4. Unbound objects raise UnboundStoreError on DB operations
    # ------------------------------------------------------------------ #

    def test_unbound_object_raises_for_db_operations(self):
        word = Word(label='unbound', start=0, end=100, store=None, save=False)

        with self.assertRaises(UnboundStoreError):
            _ = word.store

        with self.assertRaises(UnboundStoreError):
            word.save()

        with self.assertRaises(UnboundStoreError):
            _ = word.exists_in_db

    # ------------------------------------------------------------------ #
    # 5. get_or_create without store= raises UnboundStoreError
    # ------------------------------------------------------------------ #

    def test_get_or_create_requires_store(self):
        with self.assertRaises(UnboundStoreError):
            Word.get_or_create(label='no_store', start=0, end=100, audio_key=None)

    # ------------------------------------------------------------------ #
    # 6. get_or_create returns the existing object when identity fields match
    # ------------------------------------------------------------------ #

    def test_get_or_create_uses_store_query_root(self):
        self.store.create_word(label='preexist', start=400, end=500)
        # Refresh the word query root's key list from the live environment.
        self.store.words._data._get_keys(update=True)

        found, created = Word.get_or_create(
            label='preexist', start=400, end=500,
            audio_key=None,
            store=self.store,
        )

        self.assertFalse(created)
        self.assertEqual(found.label, 'preexist')

    # ------------------------------------------------------------------ #
    # 7. save → cache-clear → load preserves data and rebinds to store
    # ------------------------------------------------------------------ #

    def test_store_save_and_load_rebinds_loaded_object(self):
        # Audio requires an explicit duration; there is no class-level default.
        audio = self.store.create_audio(filename='slr_test.wav', duration=0)
        key = audio.key
        self.store._cache.clear()

        loaded = self.store.load(key)

        self.assertEqual(loaded.filename, 'slr_test.wav')
        self.assertIs(loaded._store, self.store)

    # ------------------------------------------------------------------ #
    # 8. load_many uses the bulk DB results — DB.load() must not be called
    #    inside the loop (regression for the load_many bug fix)
    # ------------------------------------------------------------------ #

    def test_store_load_many_uses_bulk_results(self):
        a1 = self.store.create_audio(filename='bulk1.wav', duration=0)
        a2 = self.store.create_audio(filename='bulk2.wav', duration=0)
        keys = [a1.key, a2.key]
        self.store._cache.clear()

        single_calls = []
        original_load = self.store.DB.load
        self.store.DB.load = lambda key, **kw: (
            single_calls.append(key) or original_load(key, **kw)
        )
        try:
            loaded = self.store.load_many(keys)
        finally:
            self.store.DB.load = original_load

        self.assertEqual(
            single_calls, [],
            f'DB.load() was called {len(single_calls)} time(s) during bulk load',
        )
        self.assertEqual(len(loaded), 2)
        self.assertEqual(
            {obj.filename for obj in loaded},
            {'bulk1.wav', 'bulk2.wav'},
        )

    # ------------------------------------------------------------------ #
    # 9. Public API exports the new symbols; load_cache is removed
    # ------------------------------------------------------------------ #

    def test_public_api_exports_store_symbols(self):
        import phraser
        self.assertTrue(hasattr(phraser, 'Store'))
        self.assertTrue(hasattr(phraser, 'UnboundStoreError'))
        self.assertTrue(hasattr(phraser, 'open_store'))
        self.assertFalse(
            hasattr(phraser, 'load_cache'),
            'load_cache should no longer be exported from phraser',
        )


if __name__ == '__main__':
    unittest.main()
