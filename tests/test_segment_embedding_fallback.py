'''Integration tests for Segment.embedding ancestor-fallback wiring.

Exercises the real phraser Phrase>Word>Syllable>Phone tree and the real
Segment.embedding / _ancestor_embedding control flow against a fake echoframe
store. echoframe is not installed in phraser's venv (see
test_segment_embeddings_forwarding.py), and echoframe's real Embedding.
sub_embedding slicing is covered by echoframe's own test suite; here we verify
that phraser tries the segment's own key, walks ancestors nearest-first on a
miss, slices through the first ancestor that has an embedding, and raises or
re-raises correctly.
'''

import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store, UnboundStoreError
from phraser.models import Phone, Phrase, Syllable, Word


class FakeEmbedding:
    '''Stand-in for echoframe.Embedding: records the slice request and returns
    a sentinel so the test can assert which ancestor produced the slice.'''

    def __init__(self, name):
        self.name = name
        self.sliced_for = None

    def sub_embedding(self, phraser_object):
        self.sliced_for = phraser_object
        return ('sliced', self.name, phraser_object)


class FakeEchoframeStore:
    '''Stand-in for echoframe.Store: returns a FakeEmbedding for keys in
    `available`, raises ValueError otherwise (the not-found signal), and
    records every lookup so call order can be asserted.'''

    def __init__(self, available):
        self.available = available
        self.calls = []

    def phraser_key_to_embedding(self, phraser_key, model_name, layer,
        collar=500):
        self.calls.append((phraser_key, model_name, layer, collar))
        if phraser_key not in self.available:
            raise ValueError(f'no metadata found for {phraser_key!r}')
        return self.available[phraser_key]


class TestSegmentEmbeddingFallback(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            cls.store = Store(path=cls._tmpdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def setUp(self):
        # A fresh, unsaved Phrase>Word>Syllable>Phone tree per test. Linking
        # is staging-only, so the parent chain lives in memory and
        # iter_ancestors yields Syllable, Word, Phrase without DB writes.
        store = self.store
        self.phrase = store.create(Phrase, label='ph', start=0, end=1000,
            save=False)
        self.word = store.create(Word, label='w', start=0, end=200, save=False)
        self.syllable = store.create(Syllable, label='s', start=0, end=100,
            save=False)
        self.phone = store.create(Phone, label='p', start=20, end=50,
            save=False)
        self.word.add_parent(self.phrase)
        self.syllable.add_parent(self.word)
        self.phone.add_parent(self.syllable)
        self.addCleanup(self._clear_binding)

    def _clear_binding(self):
        if hasattr(self.store, 'echoframe_store'):
            del self.store.echoframe_store

    def test_returns_own_embedding_without_slicing_when_present(self):
        own = FakeEmbedding('phone-own')
        fake = FakeEchoframeStore({self.phone.key: own})
        result = self.phone.embedding('m', 0, store=fake, fallback=True)
        self.assertIs(result, own)
        self.assertIsNone(own.sliced_for)
        self.assertEqual([call[0] for call in fake.calls], [self.phone.key])

    def test_fallback_walks_to_phrase_and_slices(self):
        phrase_emb = FakeEmbedding('phrase')
        fake = FakeEchoframeStore({self.phrase.key: phrase_emb})
        result = self.phone.embedding('m', 0, store=fake, fallback=True)
        self.assertEqual(result, ('sliced', 'phrase', self.phone))
        self.assertIs(phrase_emb.sliced_for, self.phone)
        self.assertEqual([call[0] for call in fake.calls],
            [self.phone.key, self.syllable.key, self.word.key, self.phrase.key])

    def test_fallback_prefers_nearest_ancestor(self):
        syllable_emb = FakeEmbedding('syllable')
        fake = FakeEchoframeStore({
            self.syllable.key: syllable_emb,
            self.phrase.key: FakeEmbedding('phrase'),
        })
        result = self.phone.embedding('m', 0, store=fake, fallback=True)
        self.assertEqual(result, ('sliced', 'syllable', self.phone))
        # Stops at the syllable; word and phrase are never queried.
        self.assertEqual([call[0] for call in fake.calls],
            [self.phone.key, self.syllable.key])

    def test_no_fallback_reraises_immediately(self):
        fake = FakeEchoframeStore({self.phrase.key: FakeEmbedding('phrase')})
        with self.assertRaises(ValueError):
            self.phone.embedding('m', 0, store=fake, fallback=False)
        # Only the segment's own key is tried; no ancestor walk.
        self.assertEqual([call[0] for call in fake.calls], [self.phone.key])

    def test_fallback_raises_when_no_ancestor_has_embedding(self):
        fake = FakeEchoframeStore({})
        with self.assertRaisesRegex(ValueError,
            'no stored embedding for Phone or its ancestors'):
            self.phone.embedding('m', 0, store=fake, fallback=True)
        self.assertEqual([call[0] for call in fake.calls],
            [self.phone.key, self.syllable.key, self.word.key, self.phrase.key])

    def test_uses_bound_echoframe_store(self):
        phrase_emb = FakeEmbedding('phrase')
        self.store.echoframe_store = FakeEchoframeStore(
            {self.phrase.key: phrase_emb})
        result = self.phone.embedding('m', 0, fallback=True)
        self.assertEqual(result, ('sliced', 'phrase', self.phone))

    def test_no_store_bound_or_passed_raises(self):
        with self.assertRaises(UnboundStoreError):
            self.phone.embedding('m', 0, fallback=True)

    def test_forwards_model_layer_and_collar_to_lookup(self):
        fake = FakeEchoframeStore({self.phrase.key: FakeEmbedding('phrase')})
        self.phone.embedding('spidr', 7, collar=750, store=fake, fallback=True)
        for phraser_key, model_name, layer, collar in fake.calls:
            self.assertEqual((model_name, layer, collar), ('spidr', 7, 750))


if __name__ == '__main__':
    unittest.main()
