import io
import shutil
import tempfile
import types
import unittest
from contextlib import redirect_stdout

from phraser import Store
from phraser.model_helper import EMPTY_ID
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word


class TestSegmentLinking(unittest.TestCase):
    '''Staging-only linking: add_parent / add_children build an in-memory
    tree (bidirectional caches, identity propagation, phrase inheritance)
    and never write to the database; persistence is an explicit save.
    '''

    def setUp(self):
        '''Fresh store per test with one persisted audio and speaker.'''
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=tmpdir)
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='linking.wav',
            duration=10_000)
        self.speaker = self.store.create(Speaker, name='spk',
            dataset='test')

    # ------------------ staged tree navigation ------------------

    def test_staged_children_navigable_and_sorted_from_root(self):
        '''Children reachable from the root, sorted by start time.'''
        tree = self._build_staged_tree()
        self.assertEqual([w.label for w in tree.phrase.children],
            ['hello', 'world'])
        for word, syllable in zip(tree.words, tree.syllables):
            word_children = word.children
            self.assertEqual(len(word_children), 1)
            self.assertIs(word_children[0], syllable)
        self.assertEqual([p.label for p in tree.syllables[0].children],
            ['h', 'e'])
        self.assertEqual([p.label for p in tree.syllables[1].children],
            ['w', 'd'])

    def test_staged_tree_iter_descendants_covers_all_segments(self):
        '''iter_descendants yields every staged segment exactly once.'''
        tree = self._build_staged_tree()
        descendants = list(tree.phrase.iter_descendants())
        expected = tree.words + tree.syllables + tree.phones
        descendant_count = len(descendants)
        expected_count = len(expected)
        self.assertEqual(descendant_count, expected_count)
        for segment in expected:
            found = any(known is segment for known in descendants)
            self.assertTrue(found)

    def test_staged_tree_inherits_phrase_refs(self):
        '''Words, syllables and phones point at the staged phrase.'''
        tree = self._build_staged_tree()
        for segment in tree.words + tree.syllables + tree.phones:
            self.assertEqual(segment.phrase_id, tree.phrase.identifier)
            self.assertEqual(segment.phrase_start, tree.phrase.start)

    def test_staged_tree_propagates_audio_and_speaker(self):
        '''Linking spreads the phrase audio and speaker over the tree.'''
        tree = self._build_staged_tree()
        for segment in tree.all_segments:
            self.assertEqual(segment.audio_id, self.audio.identifier)
            self.assertEqual(segment.speaker_id, self.speaker.identifier)

    def test_linking_writes_nothing_to_db(self):
        '''Linking is staging-only; no segment reaches the database.'''
        tree = self._build_staged_tree()
        for segment in tree.all_segments:
            exists = self.store.DB.key_exists(segment.key)
            self.assertFalse(exists)

    def test_related_stays_consistent_after_linking(self):
        '''related is an empty list after linking, never an error.'''
        tree = self._build_staged_tree()
        for segment in [tree.phrase] + tree.words + tree.syllables:
            self.assertEqual(segment.related, [])

    # ------------------ add_children atomicity ------------------

    def test_add_children_with_invalid_type_links_nothing(self):
        '''One invalid child type aborts the whole add_children call.'''
        phrase = self._create_phrase()
        word = self.store.create(Word, label='valid', start=0, end=500)
        phone = self.store.create(Phone, label='x', start=0, end=100)
        with self.assertRaises(TypeError):
            phrase.add_children([word, phone])
        self.assertEqual(word.parent_id, EMPTY_ID)
        has_parent = hasattr(word, '_parent')
        self.assertFalse(has_parent)
        self.assertEqual(phrase.children, [])

    def test_add_children_with_audio_mismatch_links_nothing(self):
        '''One child on a different audio aborts add_children.'''
        phrase = self._create_phrase()
        word = self.store.create(Word, label='valid', start=0, end=500)
        other = self.store.create(Word, label='other', start=500, end=1000,
            audio_id=b'\x07' * 8)
        with self.assertRaises(ValueError):
            phrase.add_children([word, other])
        self.assertEqual(word.parent_id, EMPTY_ID)
        self.assertEqual(other.parent_id, EMPTY_ID)
        self.assertEqual(phrase.children, [])

    # Review finding 1 regression: children that conflict with each other
    # must be rejected before any link mutates the parent.
    def test_add_children_with_conflicting_children_links_nothing(self):
        '''Children that conflict with each other abort add_children.'''
        word = self.store.create(Word, label='bare', start=0, end=1000)
        first = self.store.create(Syllable, label='one', start=0, end=500,
            audio_id=b'\x01' * 8)
        second = self.store.create(Syllable, label='two', start=500,
            end=1000, audio_id=b'\x02' * 8)
        with self.assertRaises(ValueError):
            word.add_children([first, second])
        self.assertEqual(first.parent_id, EMPTY_ID)
        self.assertEqual(second.parent_id, EMPTY_ID)
        self.assertEqual(word.audio_id, EMPTY_ID)
        self.assertEqual(word.children, [])

    # ------------------ re-parenting and duplicates ------------------

    def test_reparenting_moves_child_between_caches(self):
        '''A new parent removes the child from the old parent cache.'''
        first = self.store.create(Word, label='first', start=0, end=500)
        second = self.store.create(Word, label='second', start=500,
            end=1000)
        syllable = self.store.create(Syllable, label='syl', start=0,
            end=500)
        syllable.add_parent(first)
        syllable.add_parent(second)
        self.assertEqual(first.children, [])
        second_children = second.children
        child_count = len(second_children)
        self.assertEqual(child_count, 1)
        self.assertIs(second_children[0], syllable)
        self.assertIs(syllable.parent, second)
        self.assertEqual(syllable.parent_id, second.identifier)
        self.assertEqual(syllable.parent_start, second.start)

    # Review finding 2 regression: children materialized from the database
    # have no _parent; the old parent must be found via the store cache.
    def test_reparenting_loaded_child_uncaches_old_parent(self):
        '''A DB-loaded child leaves the old parent cache on re-parenting.'''
        audio_id = self.audio.identifier
        speaker_id = self.speaker.identifier
        first = self.store.create(Word, label='first', start=0, end=500,
            audio_id=audio_id, speaker_id=speaker_id)
        second = self.store.create(Word, label='second', start=500,
            end=1000, audio_id=audio_id, speaker_id=speaker_id)
        syllable = self.store.create(Syllable, label='syl', start=0,
            end=400, audio_id=audio_id, speaker_id=speaker_id)
        syllable.add_parent(first)
        self.store.save_many([first, second, syllable])
        self.store._cache.clear()

        loaded_first = self.store.load(first.key)
        loaded_second = self.store.load(second.key)
        child = loaded_first.children[0]
        child.add_parent(loaded_second)
        in_old = any(c is child for c in loaded_first.children)
        in_new = any(c is child for c in loaded_second.children)
        self.assertFalse(in_old)
        self.assertTrue(in_new)

    def test_reparenting_across_phrases_is_rejected(self):
        '''A syllable linked to a phrase cannot move to another phrase.'''
        tree = self._build_staged_tree()
        other_phrase = self._create_phrase(label='other', start=2000,
            end=3000)
        other_word = self.store.create(Word, label='again', start=2000,
            end=3000)
        other_phrase.add_children([other_word])
        syllable = tree.syllables[0]
        with self.assertRaises(ValueError):
            syllable.add_parent(other_word)
        self.assertIs(syllable.parent, tree.words[0])
        self.assertEqual(other_word.children, [])

    def test_duplicate_add_parent_does_not_duplicate_cache(self):
        '''Linking the same child twice keeps one cache entry.'''
        word = self.store.create(Word, label='once', start=0, end=500)
        syllable = self.store.create(Syllable, label='syl', start=0,
            end=500)
        syllable.add_parent(word)
        syllable.add_parent(word)
        word_children = word.children
        self.assertEqual(len(word_children), 1)
        self.assertIs(word_children[0], syllable)

    def test_unbound_segments_can_stage_links(self):
        '''Segments without a store can still be linked in memory.'''
        word = Word(label='solo', start=0, end=100)
        syllable = Syllable(label='so', start=0, end=100)
        syllable.add_parent(word)
        word_children = word.children
        self.assertEqual(len(word_children), 1)
        self.assertIs(word_children[0], syllable)
        self.assertEqual(word.related, [])
        self.assertIs(syllable.parent, word)

    # Review finding 3: _inherit_phrase_from only updates the directly
    # linked segment, so linking bottom-up (phone first, phrase last)
    # leaves syllable and phone without phrase references.
    @unittest.expectedFailure
    def test_bottom_up_construction_inherits_phrase_refs(self):
        '''Linking leaves-first still gives every segment phrase refs.'''
        phrase = self._create_phrase()
        word = self.store.create(Word, label='word', start=0, end=1000)
        syllable = self.store.create(Syllable, label='syl', start=0,
            end=1000)
        phone = self.store.create(Phone, label='p', start=0, end=1000)
        phone.add_parent(syllable)
        syllable.add_parent(word)
        word.add_parent(phrase)
        for segment in (word, syllable, phone):
            self.assertEqual(segment.phrase_id, phrase.identifier)
            self.assertEqual(segment.phrase_start, phrase.start)

    # Review finding 4 regression: propagation at link time walks the
    # child's descendants; children on an unbound segment must return the
    # staged cache instead of raising UnboundStoreError.
    def test_unbound_linking_with_audio_stages_cleanly(self):
        '''An unbound parent carrying audio can still link a child.'''
        word = Word(label='solo', start=0, end=100, audio_id=b'\x01' * 8)
        syllable = Syllable(label='so', start=0, end=100)
        syllable.add_parent(word)
        self.assertEqual(syllable.parent_id, word.identifier)
        self.assertEqual(syllable.audio_id, word.audio_id)
        word_children = word.children
        self.assertEqual(len(word_children), 1)
        self.assertIs(word_children[0], syllable)

    # ------------------ persistence round trip ------------------

    def test_save_many_reload_and_navigate_from_db(self):
        '''A staged tree survives save_many and reloads navigably.'''
        tree = self._build_staged_tree()
        segments = [tree.phrase] + list(tree.phrase.iter_descendants())
        self.store.save_many(segments)
        self.store._cache.clear()

        loaded = self.store.load(tree.phrase.key)
        self.assertEqual([w.label for w in loaded.words],
            ['hello', 'world'])
        syllables = loaded.syllables
        phones = loaded.phones
        self.assertEqual([s.label for s in syllables], ['hel', 'wor'])
        self.assertEqual([p.label for p in phones], ['h', 'e', 'w', 'd'])
        for segment in syllables + phones:
            self.assertEqual(segment.phrase_id, tree.phrase.identifier)
            self.assertEqual(segment.phrase_start, tree.phrase.start)
        for segment in [loaded] + loaded.words + syllables + phones:
            self.assertEqual(segment.audio_id, self.audio.identifier)
            self.assertEqual(segment.speaker_id, self.speaker.identifier)
        leaf = phones[0]
        self.assertEqual(leaf.syllable.label, 'hel')
        self.assertEqual(leaf.word.label, 'hello')

    # ------------------ Phrase.items / save_phrase_trees ------------------

    def test_items_flattens_staged_tree(self):
        '''items yields the phrase first, then every staged descendant.'''
        tree = self._build_staged_tree()
        items = tree.phrase.items
        self.assertIs(items[0], tree.phrase)
        item_count = len(items)
        segment_count = len(tree.all_segments)
        self.assertEqual(item_count, segment_count)
        for segment in tree.all_segments:
            found = any(known is segment for known in items)
            self.assertTrue(found)

    def test_items_of_childless_phrase_is_only_the_phrase(self):
        '''items of a phrase without children is just the phrase.'''
        phrase = self._create_phrase()
        self.assertEqual(phrase.items, (phrase,))

    def test_save_phrase_trees_persists_and_reloads(self):
        '''save_phrase_trees writes whole staged trees in one batch.'''
        tree = self._build_staged_tree()
        other = self._create_phrase(label='second', start=2000, end=3000)
        self.store.save_phrase_trees([tree.phrase, other])
        self.store._cache.clear()

        loaded = self.store.load(tree.phrase.key)
        self.assertEqual([w.label for w in loaded.words],
            ['hello', 'world'])
        phone_count = len(loaded.phones)
        self.assertEqual(phone_count, 4)
        other_exists = self.store.DB.key_exists(other.key)
        self.assertTrue(other_exists)

    def test_save_phrase_trees_requires_speaker(self):
        '''A phrase without an explicit speaker is rejected.'''
        phrase = self.store.create(Phrase, label='no speaker', start=0,
            end=1000, audio_id=self.audio.identifier,
            filename='linking.TextGrid')
        with self.assertRaisesRegex(ValueError, 'without a speaker'):
            self.store.save_phrase_trees([phrase])
        exists = self.store.DB.key_exists(phrase.key)
        self.assertFalse(exists)

    def test_save_phrase_trees_rejects_duplicate_identity(self):
        '''Two phrases with the same identity cannot share a batch.'''
        first = self._create_phrase(label='one')
        second = self._create_phrase(label='two')
        with self.assertRaisesRegex(ValueError, 'duplicate phrase identity'):
            self.store.save_phrase_trees([first, second])
        exists = self.store.DB.key_exists(first.key)
        self.assertFalse(exists)

    def test_save_phrase_trees_rejects_non_phrase(self):
        '''Only Phrase objects are accepted.'''
        word = self.store.create(Word, label='w', start=0, end=100,
            audio_id=self.audio.identifier)
        with self.assertRaises(TypeError):
            self.store.save_phrase_trees([word])

    def test_save_phrase_trees_writes_nothing_on_invalid_tree(self):
        '''A missing audio anywhere in the batch aborts the whole write.'''
        tree = self._build_staged_tree()
        no_audio = self.store.create(Phrase, label='bad', start=2000,
            end=3000, speaker_id=self.speaker.identifier,
            filename='linking.TextGrid')
        message = 'cannot be saved without audio'
        with self.assertRaisesRegex(ValueError, message):
            self.store.save_phrase_trees([tree.phrase, no_audio])
        for segment in tree.all_segments:
            exists = self.store.DB.key_exists(segment.key)
            self.assertFalse(exists)

    # ------------------ helpers ------------------

    def _create_phrase(self, label='hello world', start=0, end=1000):
        return self.store.create(Phrase, label=label, start=start, end=end,
            audio_id=self.audio.identifier,
            speaker_id=self.speaker.identifier,
            filename='linking.TextGrid')

    def _build_staged_tree(self):
        '''Staged Phrase > 2 Words > 1 Syllable each > 2 Phones each.
        Only the phrase carries audio/speaker; linking must propagate.
        Children are added out of order to exercise cache sorting.
        '''
        phrase = self._create_phrase()
        words = [
            self.store.create(Word, label='hello', start=0, end=500),
            self.store.create(Word, label='world', start=500, end=1000),
        ]
        phrase.add_children([words[1], words[0]])
        syllables = [
            self.store.create(Syllable, label='hel', start=0, end=500),
            self.store.create(Syllable, label='wor', start=500, end=1000),
        ]
        words[0].add_children([syllables[0]])
        words[1].add_children([syllables[1]])
        phones = [
            self.store.create(Phone, label='h', start=0, end=250),
            self.store.create(Phone, label='e', start=250, end=500),
            self.store.create(Phone, label='w', start=500, end=750),
            self.store.create(Phone, label='d', start=750, end=1000),
        ]
        syllables[0].add_children([phones[1], phones[0]])
        syllables[1].add_children([phones[3], phones[2]])
        all_segments = [phrase] + words + syllables + phones
        return types.SimpleNamespace(phrase=phrase, words=words,
            syllables=syllables, phones=phones, all_segments=all_segments)


if __name__ == '__main__':
    unittest.main()
