'''Regression tests: repeated load_many keys, label index on overwrite
and update, and lookup operators after a relation.'''

import unittest

from test_repairs import RepairCase

from phraser.models import Word
from phraser.query import queryset_from_items


class TestLoadManyRepeatedKeys(RepairCase):
    def setUp(self):
        super().setUp()
        self.phrase, self.word, self.syllable, self.phone = self.tree()

    def test_repeated_uncached_key_fills_every_position(self):
        self.store._cache.clear()
        objs = self.store.load_many([self.word.key, self.word.key])
        self.assertEqual([obj.key for obj in objs], [self.word.key] * 2)
        self.assertIs(objs[0], objs[1])

    def test_repeated_keys_with_bulk_load(self):
        self.store._cache.clear()
        keys = [self.word.key, self.phone.key, self.word.key, self.phone.key]
        objs = self.store.load_many(keys)
        self.assertEqual([obj.key for obj in objs], keys)

    def test_repeated_cached_key_fills_every_position(self):
        objs = self.store.load_many([self.word.key, self.word.key])
        self.assertEqual([obj.key for obj in objs], [self.word.key] * 2)


class TestLabelIndexOnOverwrite(RepairCase):
    def setUp(self):
        super().setUp()
        self.phrase, self.word, self.syllable, self.phone = self.tree()

    def labels(self, label):
        return self.store.label_to_instances(label, 'Word')

    def test_save_overwrite_drops_old_label(self):
        self.word.label = 'renamed'
        self.store.save(self.word, overwrite=True)
        self.assertEqual(self.labels('word'), [])
        self.assertEqual([w.key for w in self.labels('renamed')],
            [self.word.key])

    def test_save_many_overwrite_drops_old_label(self):
        self.word.label = 'renamed'
        self.store.save_many([self.word], overwrite=True)
        self.assertEqual(self.labels('word'), [])
        self.assertEqual(len(self.labels('renamed')), 1)

    def test_overwrite_with_same_label_keeps_entry(self):
        self.store.save(self.word, overwrite=True)
        self.assertEqual(len(self.labels('word')), 1)

    def test_save_many_empty_batch_is_noop(self):
        self.store.save_many([], overwrite=True)
        self.store.save_many([])

    def test_update_with_new_key_moves_row_and_label(self):
        word = self.store.create(Word, label='solo', start=500, end=600,
            audio_id=self.audio.identifier,
            speaker_id=self.speaker.identifier, save=True)
        old_key = word.key
        word.start = 510
        word.label = 'moved'
        self.store.update(old_key, word)
        self.assertNotEqual(word.key, old_key)
        self.assertFalse(self.store.DB.key_exists(old_key))
        self.assertTrue(self.store.DB.key_exists(word.key))
        self.assertEqual(self.labels('solo'), [])
        self.assertEqual([w.key for w in self.labels('moved')], [word.key])


class TestLookupOperatorsAfterRelation(RepairCase):
    def setUp(self):
        super().setUp()
        phrase, *_ = self.tree()
        self.query = queryset_from_items([phrase], self.store)

    def count(self, **lookup):
        return len(self.query.filter(**lookup))

    def test_in_through_relation(self):
        self.assertEqual(self.count(words__label__in=['word']), 1)
        self.assertEqual(self.count(words__label__in=['other']), 0)

    def test_operator_two_relations_deep(self):
        self.assertEqual(self.count(words__syllables__label__in=['a']), 1)
        self.assertEqual(self.count(words__syllables__end__gt=15), 1)
        self.assertEqual(self.count(words__syllables__end__gt=25), 0)

    def test_operator_on_relation_list_itself(self):
        self.assertEqual(self.count(words__len_eq=1), 1)
        self.assertEqual(self.count(words__syllables__len_eq=2), 1)

    def test_plain_equality_unchanged(self):
        self.assertEqual(self.count(words__label='word'), 1)
        self.assertEqual(self.count(label='phrase'), 1)


class TestLabelIndexOnDelete(RepairCase):
    def setUp(self):
        super().setUp()
        self.phrase, self.word, self.syllable, self.phone = self.tree()

    def test_delete_drops_label_entry(self):
        self.store.delete(self.word.key)
        self.assertEqual(self.store.label_to_instances('word', 'Word'), [])

    def test_phrase_delete_drops_tree_entries(self):
        self.phrase.delete()
        pairs = [('phrase', 'Phrase'), ('word', 'Word'), ('a', 'Syllable'),
            ('a', 'Phone')]
        for label, object_type in pairs:
            found = self.store.label_to_instances(label, object_type)
            self.assertEqual(found, [], object_type)

    def test_delete_skips_audio_and_speaker_keys(self):
        self.store.delete_many([self.audio.key, self.speaker.key])
        self.assertIsNone(self.store.DB.load(self.audio.key))
        self.assertEqual(len(self.store.label_to_instances('word', 'Word')),
            1)


if __name__ == '__main__':
    unittest.main()
