import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import key_helper
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

    def test_phrase_equality_uses_audio_speaker_start(self):
        audio_id = b'\x01' * 8
        speaker_id = b'\x02' * 8
        left = Phrase(label='old text', start=100, end=200,
            audio_id=audio_id, speaker_id=speaker_id, filename='old.TextGrid',
            save=False)
        right = Phrase(label='new text', start=100, end=250,
            audio_id=audio_id, speaker_id=speaker_id, filename='new.TextGrid',
            save=False)

        self.assertEqual(left, right)
        self.assertEqual(hash(left), hash(right))

    def test_phrase_equality_differs_by_identity_fields(self):
        audio_id = b'\x01' * 8
        speaker_id = b'\x02' * 8
        phrase = Phrase(label='text', start=100, end=200,
            audio_id=audio_id, speaker_id=speaker_id, filename='same.TextGrid',
            save=False)

        cases = [
            Phrase(label='text', start=100, end=200, audio_id=b'\x03' * 8,
                speaker_id=speaker_id, filename='same.TextGrid', save=False),
            Phrase(label='text', start=100, end=200, audio_id=audio_id,
                speaker_id=b'\x04' * 8, filename='same.TextGrid', save=False),
            Phrase(label='text', start=101, end=200, audio_id=audio_id,
                speaker_id=speaker_id, filename='same.TextGrid', save=False),
        ]

        for other in cases:
            with self.subTest(other=other):
                self.assertNotEqual(phrase, other)

    # ------------------------------------------------------------------ #
    # 3. Direct constructor with store= binds without using the factory
    # ------------------------------------------------------------------ #

    def test_explicit_store_constructor_binding(self):
        word = Word(label='ctor', start=0, end=100, store=self.store, save=False)
        self.assertIs(word._store, self.store)

    def test_segment_constructor_stages_by_default(self):
        word = Word(label='staged', start=0, end=100, store=self.store,
            audio_id=b'\x01' * 8)

        self.assertFalse(self.store.DB.key_exists(word.key))

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

    def test_word_ipa_round_trips_through_storage(self):
        word = self.store.create(
            Word, label='test', start=600, end=700, ipa='t ɛ s t',
            audio_id=b'\x01' * 8, save=True)
        key = word.key
        self.store._cache.clear()

        loaded = self.store.load(key)

        self.assertEqual(loaded.ipa, 't ɛ s t')
        self.assertFalse(hasattr(loaded, 'ipa_label'))

    def test_loaded_add_speaker_persists(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        old_speaker = store.create(Speaker, name='old', dataset='test')
        new_speaker = store.create(Speaker, name='new', dataset='test')
        phrase = store.create(Phrase, label='speaker test', start=0, end=100,
            audio_id=b'\x01' * 8, speaker_id=old_speaker.identifier,
            filename='test.TextGrid', save=True)
        key = phrase.key
        store._cache.clear()

        loaded = store.load(key)
        loaded.add_speaker(old_speaker, propagate=False)
        loaded.add_speaker(new_speaker, propagate=False)
        store._cache.clear()
        reloaded = store.load(key)

        self.assertEqual(reloaded.speaker_id, new_speaker.identifier)

    def test_apply_speaker_id_invalidates_stale_speaker_cache(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        old_speaker = store.create(Speaker, name='old-cache', dataset='test')
        new_speaker = store.create(Speaker, name='new-cache', dataset='test')
        phrase = store.create(Phrase, label='cache test', start=0, end=100,
            audio_id=b'\x01' * 8, speaker_id=old_speaker.identifier,
            filename='test.TextGrid', save=True)
        phrase._speaker = old_speaker

        changed = phrase._apply_speaker_id(new_speaker.identifier)

        self.assertFalse(hasattr(phrase, '_speaker'))
        self.assertTrue(changed)

    def test_save_rejects_segment_without_audio(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        word = store.create(Word, label='missing audio', start=0, end=100)

        with self.assertRaisesRegex(ValueError, 'cannot be saved without audio'):
            word.save()

        self.assertFalse(hasattr(word, '_key'))
        self.assertFalse(store.DB.key_exists(word.key))

    def test_save_many_validates_all_segments_before_writing(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        valid = store.create(Word, label='valid', start=0, end=100,
            audio_id=b'\x01' * 8)
        invalid = store.create(Word, label='invalid', start=100, end=200)

        with self.assertRaisesRegex(ValueError, 'cannot be saved without audio'):
            store.save_many([valid, invalid])

        self.assertFalse(store.DB.key_exists(valid.key))
        self.assertFalse(store.DB.key_exists(invalid.key))

    def test_update_validates_before_deleting_old_record(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        word = store.create(Word, label='update', start=0, end=100,
            audio_id=b'\x01' * 8, save=True)
        old_key = word.key
        word.audio_id = b'\x00' * 8

        with self.assertRaisesRegex(ValueError, 'cannot be saved without audio'):
            store.update(old_key, word)

        self.assertTrue(store.DB.key_exists(old_key))

    def test_persisted_segment_audio_cannot_change(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        old_audio = store.create(Audio, filename='old.wav', duration=1000)
        new_audio = store.create(Audio, filename='new.wav', duration=1000)
        phrase = store.create(Phrase, label='audio test', start=0, end=100,
            audio_id=old_audio.identifier, filename='test.TextGrid', save=True)
        old_key = phrase.key

        with self.assertRaisesRegex(ValueError, 'cannot change after persistence'):
            phrase.add_audio(new_audio, update_database=False, propagate=False)

        self.assertEqual(phrase.audio_id, old_audio.identifier)
        self.assertTrue(store.DB.key_exists(old_key))

    def test_save_rejects_direct_persisted_audio_change(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        phrase = store.create(Phrase, label='direct change', start=0, end=100,
            audio_id=b'\x01' * 8, filename='test.TextGrid', save=True)
        old_key = phrase.key
        phrase.audio_id = b'\x02' * 8

        with self.assertRaisesRegex(ValueError, 'cannot change after persistence'):
            phrase.save()

        self.assertTrue(store.DB.key_exists(old_key))
        self.assertFalse(store.DB.key_exists(
            key_helper.instance_to_key(phrase)))

    def test_staged_segment_audio_can_change(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        old_audio = store.create(Audio, filename='staged-old.wav', duration=1000)
        new_audio = store.create(Audio, filename='staged-new.wav', duration=1000)
        phrase = store.create(Phrase, label='staged audio', start=0, end=100,
            filename='test.TextGrid')

        phrase.add_audio(old_audio, update_database=False, propagate=False)
        phrase.add_audio(new_audio, update_database=False, propagate=False)
        phrase.save()

        self.assertEqual(phrase.audio_id, new_audio.identifier)
        self.assertEqual(phrase._key, key_helper.instance_to_key(phrase))
        self.assertTrue(store.DB.key_exists(phrase.key))

    def test_save_many_sets_persisted_key(self):
        store = self._fresh_store()
        self.addCleanup(store.close)
        phrase = store.create(Phrase, label='batch', start=0, end=100,
            audio_id=b'\x01' * 8, filename='test.TextGrid')

        store.save_many([phrase])

        self.assertEqual(phrase._key, key_helper.instance_to_key(phrase))

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
