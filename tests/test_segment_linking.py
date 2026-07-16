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
    tree (bidirectional caches, identity validation, phrase inheritance)
    and never write to the database; persistence is an explicit save.
    Segments carry audio/speaker from construction; linking rejects
    mismatches and inherits phrase refs.
    '''

    def setUp(self):
        '''Fresh store per test with one persisted audio and speaker.'''
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=tmpdir)
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='linking.wav',
            duration=10_000, save=True)
        self.speaker = self.store.create(Speaker, name='spk',
            dataset='test', save=True)
        audio_id, speaker_id = self.audio.identifier, self.speaker.identifier
        self.identity = {'audio_id': audio_id, 'speaker_id': speaker_id}

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

    def test_linking_writes_nothing_to_db(self):
        '''Linking is staging-only; no segment reaches the database.'''
        tree = self._build_staged_tree()
        for segment in tree.all_segments:
            exists = self.store.DB.key_exists(segment.key)
            self.assertFalse(exists)

    def test_overlapping_stays_consistent_after_linking(self):
        '''overlapping is an empty list after linking, never an error.'''
        tree = self._build_staged_tree()
        for segment in [tree.phrase] + tree.words + tree.syllables:
            self.assertEqual(segment.overlapping, [])

    # ------------------ add_children atomicity ------------------

    def test_add_children_with_invalid_type_links_nothing(self):
        '''One invalid child type aborts the whole add_children call.'''
        phrase = self._create_phrase()
        word = self.store.create(Word, label='valid', start=0, end=500,
            **self.identity)
        phone = self.store.create(Phone, label='x', start=0, end=100,
            **self.identity)
        with self.assertRaises(TypeError):
            phrase.add_children([word, phone])
        self.assertEqual(word.parent_id, EMPTY_ID)
        has_parent = hasattr(word, '_parent')
        self.assertFalse(has_parent)
        self.assertEqual(phrase.children, [])

    def test_add_children_with_audio_mismatch_links_nothing(self):
        '''One child on a different audio aborts add_children.'''
        phrase = self._create_phrase()
        word = self.store.create(Word, label='valid', start=0, end=500,
            **self.identity)
        other = self.store.create(Word, label='other', start=500, end=1000,
            audio_id=b'\x07' * 8, speaker_id=self.speaker.identifier)
        with self.assertRaises(ValueError):
            phrase.add_children([word, other])
        self.assertEqual(word.parent_id, EMPTY_ID)
        self.assertEqual(other.parent_id, EMPTY_ID)
        self.assertEqual(phrase.children, [])

    # ------------------ re-parenting and duplicates ------------------

    def test_reparenting_moves_child_between_caches(self):
        '''A new parent removes the child from the old parent cache.'''
        first = self.store.create(Word, label='first', start=0, end=500,
            **self.identity)
        second = self.store.create(Word, label='second', start=500,
            end=1000, **self.identity)
        syllable = self.store.create(Syllable, label='syl', start=0,
            end=500, **self.identity)
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
            end=3000, **self.identity)
        other_phrase.add_children([other_word])
        syllable = tree.syllables[0]
        with self.assertRaises(ValueError):
            syllable.add_parent(other_word)
        self.assertIs(syllable.parent, tree.words[0])
        self.assertEqual(other_word.children, [])

    def test_duplicate_add_parent_does_not_duplicate_cache(self):
        '''Linking the same child twice keeps one cache entry.'''
        word = self.store.create(Word, label='once', start=0, end=500,
            **self.identity)
        syllable = self.store.create(Syllable, label='syl', start=0,
            end=500, **self.identity)
        syllable.add_parent(word)
        syllable.add_parent(word)
        word_children = word.children
        self.assertEqual(len(word_children), 1)
        self.assertIs(word_children[0], syllable)

    def test_unbound_segments_can_stage_links(self):
        '''Segments without a store can still be linked in memory.'''
        word = Word(label='solo', start=0, end=100, audio_id=b'\x01' * 8,
            speaker_id=b'\x02' * 8)
        syllable = Syllable(label='so', start=0, end=100,
            audio_id=b'\x01' * 8, speaker_id=b'\x02' * 8)
        syllable.add_parent(word)
        word_children = word.children
        self.assertEqual(len(word_children), 1)
        self.assertIs(word_children[0], syllable)
        self.assertEqual(word.overlapping, [])
        self.assertIs(syllable.parent, word)

    # Regression (review finding 3): phrase refs gained at link time
    # must push down through staged children, or bottom-up built trees
    # (phone first, phrase last) leave descendants without them.
    def test_bottom_up_construction_inherits_phrase_refs(self):
        '''Linking leaves-first still gives every segment phrase refs.'''
        phrase = self._create_phrase()
        word = self.store.create(Word, label='word', start=0, end=1000,
            **self.identity)
        syllable = self.store.create(Syllable, label='syl', start=0,
            end=1000, **self.identity)
        phone = self.store.create(Phone, label='p', start=0, end=1000,
            **self.identity)
        phone.add_parent(syllable)
        syllable.add_parent(word)
        word.add_parent(phrase)
        for segment in (word, syllable, phone):
            self.assertEqual(segment.phrase_id, phrase.identifier)
            self.assertEqual(segment.phrase_start, phrase.start)

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

    # ------------------ ownership filter / replace_children ------------------

    def test_foreign_parent_same_speaker_lands_in_overlapping(self):
        '''A same-speaker segment owned by another parent is not adopted.'''
        mine = self._create_phrase(label='mine', start=0, end=1000)
        my_word = self.store.create(Word, label='my', start=0, end=500,
            **self.identity)
        mine.add_children([my_word])
        other = self._create_phrase(label='other', start=500, end=1500)
        other_word = self.store.create(Word, label='their', start=500,
            end=1000, **self.identity)
        other.add_children([other_word])
        self.store.save_many([mine, my_word, other, other_word])
        self.store._cache.clear()

        loaded = self.store.load(mine.key)
        self.assertEqual([w.label for w in loaded.children], ['my'])
        self.assertEqual([w.label for w in loaded.overlapping], ['their'])

    def test_replace_children_rebuild_excludes_persisted_layer(self):
        '''Rebuilding via replace_children keeps the old persisted word
        layer out of the staged tree; the old rows stay on disk.'''
        tree = self._build_staged_tree()
        self.store.save_phrase_trees([tree.phrase])
        self.store._cache.clear()

        loaded = self.store.load(tree.phrase.key)
        old_word_keys = [w.key for w in loaded.words]
        new_word = self.store.create(Word, label='helloworld', start=0,
            end=1000, **self.identity)
        loaded.replace_children([new_word])
        children = loaded.children
        self.assertEqual(len(children), 1)
        self.assertIs(children[0], new_word)
        items = loaded.items
        item_count = len(items)
        self.assertEqual(item_count, 2)
        self.assertIs(items[0], loaded)
        self.assertIs(items[1], new_word)
        loaded.validate_tree()
        for key in old_word_keys:
            exists = self.store.DB.key_exists(key)
            self.assertTrue(exists)

    def test_replace_children_with_invalid_child_keeps_old_children(self):
        '''A failing replace_children leaves the staged view untouched.'''
        tree = self._build_staged_tree()
        phone = self.store.create(Phone, label='x', start=0, end=100,
            **self.identity)
        with self.assertRaises(TypeError):
            tree.phrase.replace_children([phone])
        self.assertEqual([w.label for w in tree.phrase.children],
            ['hello', 'world'])

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

    def test_validate_tree_accepts_valid_staged_tree(self):
        '''A staged tree with speaker and audio everywhere validates.'''
        tree = self._build_staged_tree()
        tree.phrase.validate_tree()

    def test_validate_tree_rejects_speaker_mismatch(self):
        '''A descendant with a diverging speaker fails tree validation.'''
        tree = self._build_staged_tree()
        tree.phones[0].speaker_id = b'\x09' * 8
        with self.assertRaisesRegex(ValueError, 'does not match'):
            tree.phrase.validate_tree()
        with self.assertRaisesRegex(ValueError, 'does not match'):
            self.store.save_phrase_trees([tree.phrase])
        exists = self.store.DB.key_exists(tree.phrase.key)
        self.assertFalse(exists)

    def test_phrase_requires_speaker_at_construction(self):
        '''Identity is a construction-time requirement.'''
        with self.assertRaises(TypeError):
            self.store.create(Phrase, label='no speaker', start=0,
                end=1000, audio_id=self.audio.identifier,
                filename='linking.TextGrid')
        with self.assertRaisesRegex(ValueError, 'requires a speaker_id'):
            self.store.create(Phrase, label='no speaker', start=0,
                end=1000, audio_id=self.audio.identifier,
                speaker_id=EMPTY_ID, filename='linking.TextGrid')

    def test_save_phrase_trees_rejects_duplicate_identity(self):
        '''Two phrases with the same identity cannot share a batch.'''
        first = self._create_phrase(label='one')
        second = self._create_phrase(label='two')
        with self.assertRaisesRegex(ValueError, 'duplicate phrase identity'):
            self.store.save_phrase_trees([first, second])
        exists = self.store.DB.key_exists(first.key)
        self.assertFalse(exists)

    def test_save_many_rejects_duplicate_keys_in_batch(self):
        '''The same key twice in one batch aborts before writing.'''
        word = self.store.create(Word, label='twice', start=0, end=500,
            **self.identity)
        with self.assertRaisesRegex(ValueError, 'duplicate key in batch'):
            self.store.save_many([word, word])
        exists = self.store.DB.key_exists(word.key)
        self.assertFalse(exists)

    def test_save_phrase_trees_rejects_shared_descendant_key(self):
        '''Two loaded copies of one word cannot be saved in one batch:
        DB.write_many would silently keep only the last copy's row.'''
        tree = self._build_staged_tree()
        self.store.save_phrase_trees([tree.phrase])
        self.store._cache.clear()
        loaded = self.store.load(tree.phrase.key)
        first_copy = loaded.words[0]
        self.store._cache.clear()
        second_copy = self.store.load(first_copy.key)
        other = self._create_phrase(label='other', start=2000, end=3000)
        second_copy.add_parent(other)
        message = 'duplicate key in batch'
        with self.assertRaisesRegex(ValueError, message):
            self.store.save_phrase_trees([loaded, other], overwrite=True)
        exists = self.store.DB.key_exists(other.key)
        self.assertFalse(exists)

    def test_save_phrase_trees_rejects_same_speaker_overlap_in_batch(self):
        '''Two same-speaker overlapping phrases cannot share a batch.'''
        first = self._create_phrase(label='first', start=0, end=1000)
        second = self._create_phrase(label='second', start=500, end=1500)
        message = 'same-speaker overlapping phrases'
        with self.assertRaisesRegex(ValueError, message):
            self.store.save_phrase_trees([first, second])
        exists = self.store.DB.key_exists(first.key)
        self.assertFalse(exists)

    def test_save_phrase_trees_rejects_overlap_with_persisted(self):
        '''A phrase overlapping a same-speaker persisted phrase is
        rejected, also across separate save calls.'''
        first = self._create_phrase(label='first', start=0, end=1000)
        self.store.save_phrase_trees([first])
        second = self._create_phrase(label='second', start=500, end=1500)
        message = 'same-speaker overlapping phrases'
        with self.assertRaisesRegex(ValueError, message):
            self.store.save_phrase_trees([second])
        exists = self.store.DB.key_exists(second.key)
        self.assertFalse(exists)

    def test_save_phrase_trees_rejects_overlap_inside_persisted(self):
        '''A phrase inside an earlier-starting persisted phrase is
        rejected: the scan window must reach earlier starts.'''
        first = self._create_phrase(label='first', start=0, end=2000)
        self.store.save_phrase_trees([first])
        inner = self._create_phrase(label='inner', start=500, end=800)
        message = 'same-speaker overlapping phrases'
        with self.assertRaisesRegex(ValueError, message):
            self.store.save_phrase_trees([inner])

    def test_save_phrase_trees_own_row_is_overlap_exempt(self):
        '''Re-saving a persisted phrase does not overlap itself.'''
        tree = self._build_staged_tree()
        self.store.save_phrase_trees([tree.phrase])
        self.store._cache.clear()
        loaded = self.store.load(tree.phrase.key)
        self.store.save_phrase_trees([loaded], overwrite=True)

    def test_save_phrase_trees_allows_other_speaker_overlap(self):
        '''Overlap between different speakers is legitimate speech.'''
        other_speaker = self.store.create(Speaker, name='spk2',
            dataset='test', save=True)
        first = self._create_phrase(label='first', start=0, end=1000)
        second = self.store.create(Phrase, label='second', start=0,
            end=1000, audio_id=self.audio.identifier,
            speaker_id=other_speaker.identifier,
            filename='linking.TextGrid')
        self.store.save_phrase_trees([first, second])
        for phrase in (first, second):
            exists = self.store.DB.key_exists(phrase.key)
            self.assertTrue(exists)

    # ------------------ save_phrase_trees overwrite replacement ------------------

    def test_overwrite_replaces_persisted_tree(self):
        '''overwrite=True: the persisted tree becomes exactly the staged
        tree; the old descendant rows are deleted, not re-merged.'''
        tree = self._build_staged_tree()
        self.store.save_phrase_trees([tree.phrase])
        old_keys = [segment.key for segment in
            tree.words + tree.syllables + tree.phones]
        self.store._cache.clear()
        loaded = self.store.load(tree.phrase.key)
        new_word = self.store.create(Word, label='helloworld', start=0,
            end=1000, **self.identity)
        loaded.replace_children([new_word])
        self.store.save_phrase_trees([loaded], overwrite=True)
        for key in old_keys:
            exists = self.store.DB.key_exists(key)
            self.assertFalse(exists)
        self.store._cache.clear()
        reloaded = self.store.load(loaded.key)
        self.assertEqual([w.label for w in reloaded.children],
            ['helloworld'])
        self.assertEqual(reloaded.overlapping, [])

    def test_overwrite_reaches_stale_rows_beyond_shrunk_end(self):
        '''Re-saving with a smaller end still deletes stale descendants
        starting beyond the new end: the deletion scan runs to the
        persisted row's end, not the staged one.'''
        phrase = self._create_phrase(label='long', start=0, end=2000)
        late_word = self.store.create(Word, label='late', start=1500,
            end=2000, **self.identity)
        phrase.add_children([late_word])
        self.store.save_phrase_trees([phrase])
        stale_key = late_word.key
        self.store._cache.clear()
        loaded = self.store.load(phrase.key)
        loaded.end = 1000
        new_word = self.store.create(Word, label='early', start=0,
            end=1000, **self.identity)
        loaded.replace_children([new_word])
        self.store.save_phrase_trees([loaded], overwrite=True)
        exists = self.store.DB.key_exists(stale_key)
        self.assertFalse(exists)

    def test_overwrite_cleans_label_index_of_deleted_rows(self):
        '''Deleted rows lose their label-index entries; the staged rows
        gain theirs.'''
        tree = self._build_staged_tree()
        self.store.save_phrase_trees([tree.phrase])
        old_word_key = tree.words[0].key
        self.store._cache.clear()
        loaded = self.store.load(tree.phrase.key)
        new_word = self.store.create(Word, label='helloworld', start=0,
            end=1000, **self.identity)
        loaded.replace_children([new_word])
        self.store.save_phrase_trees([loaded], overwrite=True)
        old_entries = self.store.DB.label_to_segment_keys('hello', 'Word')
        old_entries = list(old_entries)
        self.assertNotIn(old_word_key, old_entries)
        new_entries = self.store.DB.label_to_segment_keys(
            'helloworld', 'Word')
        new_entries = list(new_entries)
        self.assertIn(new_word.key, new_entries)

    def test_overwrite_spares_other_speaker_rows_in_range(self):
        '''Attribution is by identifier: another speaker's overlapping
        tree survives an overwrite re-save.'''
        other_speaker = self.store.create(Speaker, name='spk2',
            dataset='test', save=True)
        audio_id, speaker_id = self.audio.identifier, other_speaker.identifier
        other_identity = {'audio_id': audio_id, 'speaker_id': speaker_id}
        other = self.store.create(Phrase, label='other', start=0,
            end=1000, **other_identity)
        other_word = self.store.create(Word, label='their', start=0,
            end=500, **other_identity)
        other.add_children([other_word])
        tree = self._build_staged_tree()
        self.store.save_phrase_trees([tree.phrase, other])
        self.store._cache.clear()
        loaded = self.store.load(tree.phrase.key)
        new_word = self.store.create(Word, label='helloworld', start=0,
            end=1000, **self.identity)
        loaded.replace_children([new_word])
        self.store.save_phrase_trees([loaded], overwrite=True)
        for segment in (other, other_word):
            exists = self.store.DB.key_exists(segment.key)
            self.assertTrue(exists)

    def test_save_phrase_trees_without_overwrite_rejects_existing_keys(self):
        '''A persisted tree cannot be re-saved without overwrite; the
        DB-level fail-early check writes nothing.'''
        tree = self._build_staged_tree()
        self.store.save_phrase_trees([tree.phrase])
        self.store._cache.clear()
        loaded = self.store.load(tree.phrase.key)
        new_word = self.store.create(Word, label='helloworld', start=0,
            end=1000, **self.identity)
        loaded.replace_children([new_word])
        with self.assertRaises(KeyError):
            self.store.save_phrase_trees([loaded])
        exists = self.store.DB.key_exists(new_word.key)
        self.assertFalse(exists)

    def test_save_phrase_trees_rejects_non_phrase(self):
        '''Only Phrase objects are accepted.'''
        word = self.store.create(Word, label='w', start=0, end=100,
            **self.identity)
        with self.assertRaises(TypeError):
            self.store.save_phrase_trees([word])

    def test_save_phrase_trees_writes_nothing_on_invalid_tree(self):
        '''An invalid tree anywhere in the batch aborts the whole write.'''
        tree = self._build_staged_tree()
        bad = self._create_phrase(label='bad', start=2000, end=3000)
        bad.save()
        bad.audio_id = b'\x08' * 8
        message = 'cannot change after persistence'
        with self.assertRaisesRegex(ValueError, message):
            self.store.save_phrase_trees([tree.phrase, bad])
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
        Every segment carries audio/speaker from construction.
        Children are added out of order to exercise cache sorting.
        '''
        identity = self.identity
        phrase = self._create_phrase()
        words = [
            self.store.create(Word, label='hello', start=0, end=500,
                **identity),
            self.store.create(Word, label='world', start=500, end=1000,
                **identity),
        ]
        phrase.add_children([words[1], words[0]])
        syllables = [
            self.store.create(Syllable, label='hel', start=0, end=500,
                **identity),
            self.store.create(Syllable, label='wor', start=500, end=1000,
                **identity),
        ]
        words[0].add_children([syllables[0]])
        words[1].add_children([syllables[1]])
        phones = [
            self.store.create(Phone, label='h', start=0, end=250,
                **identity),
            self.store.create(Phone, label='e', start=250, end=500,
                **identity),
            self.store.create(Phone, label='w', start=500, end=750,
                **identity),
            self.store.create(Phone, label='d', start=750, end=1000,
                **identity),
        ]
        syllables[0].add_children([phones[1], phones[0]])
        syllables[1].add_children([phones[3], phones[2]])
        all_segments = [phrase] + words + syllables + phones
        return types.SimpleNamespace(phrase=phrase, words=words,
            syllables=syllables, phones=phones, all_segments=all_segments)


if __name__ == '__main__':
    unittest.main()
