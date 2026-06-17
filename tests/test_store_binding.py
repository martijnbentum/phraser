import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import ClosedStoreError, Store, UnboundStoreError
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word
from phraser.query import QuerySet


class TestStoreBinding(unittest.TestCase):
    """High-priority tests for the store-binding refactor (store_by_codex branch)."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            cls.store = Store(path=cls._tmpdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    # ------------------------------------------------------------------ #
    # 1. Store() attaches all six query roots
    # ------------------------------------------------------------------ #

    def test_store_attaches_query_roots(self):
        for attr in ('audios', 'phrases', 'words', 'syllables', 'phones', 'speakers'):
            with self.subTest(attr=attr):
                self.assertTrue(hasattr(self.store, attr),
                                f'store missing query root: {attr}')
                self.assertIsInstance(getattr(self.store, attr), QuerySet)

    # ------------------------------------------------------------------ #
    # 2. store.create() factory method binds objects to the store
    # ------------------------------------------------------------------ #

    def test_store_create_methods_bind_objects(self):
        objs = {
            'Audio':    self.store.create(Audio, filename='bind.wav', save=False),
            'Phrase':   self.store.create(Phrase, label='bp', start=0, end=100, save=False),
            'Word':     self.store.create(Word, label='bw', start=0, end=50, save=False),
            'Syllable': self.store.create(Syllable, label='bs', start=0, end=50, save=False),
            'Phone':    self.store.create(Phone, label='t', start=0, end=50, save=False),
            'Speaker':  self.store.create(Speaker, name='bs', dataset='test', save=False),
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
        self.store.create(Word, label='preexist', start=400, end=500)
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
        audio = self.store.create(Audio, filename='slr_test.wav', duration=0)
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
        a1 = self.store.create(Audio, filename='bulk1.wav', duration=0)
        a2 = self.store.create(Audio, filename='bulk2.wav', duration=0)
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
        self.assertTrue(hasattr(phraser, 'ClosedStoreError'))
        self.assertFalse(
            hasattr(phraser, 'open_store'),
            'open_store should no longer be exported from phraser',
        )
        self.assertFalse(
            hasattr(phraser, 'load_cache'),
            'load_cache should no longer be exported from phraser',
        )

    # ------------------------------------------------------------------ #
    # 10. Store close/open lifecycle
    # ------------------------------------------------------------------ #

    def _fresh_store(self):
        """A throwaway store with its own temp dir (cleaned up after the test).

        Lifecycle tests must not use the shared cls.store, since closing it
        would break the env other tests in this class depend on.
        """
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        with redirect_stdout(io.StringIO()):
            store = Store(path=tmpdir)
        return store

    def test_close_sets_state_and_clears_cache(self):
        store = self._fresh_store()
        store.create(Audio, filename='close.wav', duration=0)
        self.assertTrue(store._cache, 'save should populate the cache')

        store.close()

        self.assertTrue(store.closed)
        self.assertFalse(store.is_open())
        self.assertEqual(store._cache, {})

    def test_open_restores_usability(self):
        store = self._fresh_store()
        audio = store.create(Audio, filename='reopen.wav', duration=0)
        key = audio.key

        store.close()
        store.open()

        self.assertFalse(store.closed)
        self.assertTrue(store.is_open())
        loaded = store.load(key)
        self.assertEqual(loaded.filename, 'reopen.wav')

    def test_close_is_idempotent(self):
        store = self._fresh_store()
        store.close()
        store.close()  # second close must not raise
        self.assertTrue(store.closed)

    def test_closed_store_raises_on_model_access(self):
        store = self._fresh_store()
        audio = store.create(Audio, filename='model.wav', duration=0)
        store.close()

        with self.assertRaises(ClosedStoreError):
            _ = audio.store
        with self.assertRaises(ClosedStoreError):
            audio.save()
        with self.assertRaises(ClosedStoreError):
            _ = audio.exists_in_db
        with self.assertRaises(ClosedStoreError):
            _ = audio.phrases

    def test_closed_store_raises_on_direct_api(self):
        store = self._fresh_store()
        audio = store.create(Audio, filename='direct.wav', duration=0)
        key = audio.key
        store.close()

        with self.assertRaises(ClosedStoreError):
            store.load(key)
        with self.assertRaises(ClosedStoreError):
            store.load_many([key])
        with self.assertRaises(ClosedStoreError):
            store.save(audio)
        with self.assertRaises(ClosedStoreError):
            store.save_many([audio])
        with self.assertRaises(ClosedStoreError):
            store.delete(key)
        with self.assertRaises(ClosedStoreError):
            store.delete_many([key])
        with self.assertRaises(ClosedStoreError):
            store.audios.get_or_none(filename='direct.wav')


if __name__ == '__main__':
    unittest.main()
