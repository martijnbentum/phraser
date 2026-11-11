import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from phraser import Audio, Marker, Phone, Phrase, Speaker, Store, Syllable, Word
from phraser.repair import segment_changes
from phraser.repair.changes import add_change, apply_change_plan
from phraser.repair.changes import create_change_plan
from phraser.repair.changes import rollback_changes, save_change_plan
from phraser.repair.segment_changes import plan_delete, plan_update
from phraser.storage import key_helper, struct_value


class TestSegmentRepairs(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        with redirect_stdout(io.StringIO()):
            self.store = Store(self.root / 'store')
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='test.wav',
            duration=1000, save=True)
        self.speaker = self.store.create(Speaker, name='speaker', save=True)
        self.speaker.add_audio(self.audio)
        self.phrase, self.word, self.syllable, self.phone = self.tree()

    def tree(self, start=10):
        identity = {'audio_id': self.audio.identifier,
            'speaker_id': self.speaker.identifier, 'store': self.store}
        phone = Phone('a', start, start + 10, **identity)
        syllable = Syllable('a', start, start + 10, **identity)
        syllable.add_children([phone])
        word = Word('word', start, start + 10, **identity)
        word.add_children([syllable])
        phrase = Phrase('phrase', start, start + 20, **identity)
        phrase.add_children([word])
        with redirect_stdout(io.StringIO()):
            self.store.save_phrase_trees([phrase])
        return phrase, word, syllable, phone

    def snapshot(self):
        snapshot = {}
        with self.store.DB.env.begin() as txn:
            for name, database in self.store.DB.db.items():
                with txn.cursor(db=database) as cursor:
                    snapshot[name] = dict(cursor)
        return snapshot

    def deletion(self, keys):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'delete trees')
            plan_delete(plan, self.store, txn, keys)
        return plan

    def save(self, plan):
        return save_change_plan(plan, path=self.root / 'repair.json')

    def test_combined_roots_overlap_and_exact_restoration(self):
        second = self.tree(50)
        stale = key_helper.label_to_label_index_key('old', 'Phone',
            self.phone.key)
        self.store.DB.write(stale, b'old-value', db_name='label_segment')
        before = self.snapshot()
        roots = [self.phrase.key, self.word.key, second[0].key]
        reader = segment_changes._audio_rows
        with patch.object(segment_changes, '_audio_rows', wraps=reader) as read:
            plan = self.deletion(roots)
        self.assertEqual(read.call_count, 1)
        self.assertEqual(self.snapshot(), before)
        path = self.save(plan)
        apply_change_plan(self.store, path)
        after = self.snapshot()
        self.assertEqual(len(after['main']), 2)
        self.assertEqual(after['label_segment'], {})
        self.assertEqual(after['speaker_audio'], {})
        rollback_changes(self.store, path)
        self.assertEqual(self.snapshot(), before)

    def test_partial_tree_keeps_parent_and_speaker_link(self):
        before = self.snapshot()
        path = self.save(self.deletion(self.syllable.key))
        apply_change_plan(self.store, path)
        after = self.snapshot()
        self.assertNotIn(self.syllable.key, after['main'])
        self.assertNotIn(self.phone.key, after['main'])
        self.assertEqual(after['main'][self.word.key],
            before['main'][self.word.key])
        self.assertEqual(after['speaker_audio'], before['speaker_audio'])

    def test_second_phrase_preserves_speaker_link(self):
        self.tree(50)
        before = self.snapshot()['speaker_audio']
        path = self.save(self.deletion(self.phrase.key))
        apply_change_plan(self.store, path)
        self.assertEqual(self.snapshot()['speaker_audio'], before)

    def test_marker_blocks_without_partial_additions(self):
        marker = Marker('marker', 10, 20, audio_id=self.audio.identifier,
            speaker_id=self.speaker.identifier, store=self.store,
            phrase=self.phrase)
        self.store.save(marker)
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'blocked')
            with self.assertRaisesRegex(ValueError, 'Retained Marker'):
                plan_delete(plan, self.store, txn, self.phrase.key)
        self.assertEqual(plan.changes, [])

    def test_retained_phrase_reference_blocks(self):
        _, _, _, phone = self.tree(50)
        phone.phrase_id = self.phrase.identifier
        phone.phrase_start = self.phrase.start
        self.store.DB.write(phone.key, struct_value.pack_instance(phone),
            overwrite=True)
        with self.assertRaisesRegex(ValueError, 'Retained Phone'):
            self.deletion(self.phrase.key)

    def test_descendants_outside_parent_time_are_included(self):
        self.word.end = 11
        self.store.DB.write(self.word.key,
            struct_value.pack_instance(self.word), overwrite=True)
        plan = self.deletion(self.word.key)
        keys = {change['key'] for change in plan.changes}
        self.assertIn(self.phone.key.hex(), keys)

    def test_later_root_failure_keeps_plan_unchanged(self):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'blocked')
            plan_update(plan, self.store, txn, self.phone.key, end=25)
            original = list(plan.changes)
            with self.assertRaisesRegex(ValueError, 'already changes'):
                plan_delete(plan, self.store, txn, self.phrase.key)
            self.assertEqual(plan.changes, original)

    def test_label_update_moves_index_and_preserves_other_fields(self):
        original = struct_value.unpack_instance('Phone',
            self.store.DB.load(self.phone.key))
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'new label')
            plan_update(plan, self.store, txn, self.phone.key, label='b')
        self.assertEqual(plan.count_changes('object_type'),
            {'Phone': 1, 'label_segment': 2})
        with redirect_stdout(io.StringIO()) as output: plan.summary(limit=1)
        self.assertIn("label: 'a' -> 'b'", output.getvalue())
        self.assertIn(self.audio.identifier.hex(), output.getvalue())
        path = self.save(plan)
        apply_change_plan(self.store, path)
        updated = struct_value.unpack_instance('Phone',
            self.store.DB.load(self.phone.key))
        original['label'] = 'b'
        self.assertEqual(updated, original)
        old = key_helper.label_to_label_index_key('a', 'Phone', self.phone.key)
        new = key_helper.label_to_label_index_key('b', 'Phone', self.phone.key)
        self.assertIsNone(self.store.DB.load(old, db_name='label_segment'))
        self.assertEqual(self.store.DB.load(new, db_name='label_segment'), b'')

    def test_explicit_bad_end_refused_but_existing_bad_duration_allowed(self):
        self.phone.end = self.phone.start
        self.store.DB.write(self.phone.key,
            struct_value.pack_instance(self.phone), overwrite=True)
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'edit')
            for end in (9, 10):
                with self.assertRaisesRegex(ValueError, 'greater than start'):
                    plan_update(plan, self.store, txn, self.phone.key, end=end)
                self.assertEqual(plan.changes, [])
            plan_update(plan, self.store, txn, self.phone.key, flags=1)
        self.assertEqual(len(plan.changes), 1)

    def test_index_conflict_leaves_update_proposal_unchanged(self):
        old = key_helper.label_to_label_index_key('a', 'Phone', self.phone.key)
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'conflict')
            add_change(plan, 'label_segment', old, b'', b'changed')
            original = list(plan.changes)
            with self.assertRaisesRegex(ValueError, 'already changes'):
                plan_update(plan, self.store, txn, self.phone.key, label='b')
            self.assertEqual(plan.changes, original)

    def test_protected_fields_and_pack_failure_leave_plan_unchanged(self):
        fields = {'start': 5, 'speaker_id': self.speaker.identifier,
            'parent_start': 10, 'phrase_id': self.phrase.identifier,
            'version': 1, 'unknown': 'x'}
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'edit')
            for name, value in fields.items():
                with self.subTest(field=name), self.assertRaises(ValueError):
                    plan_update(plan, self.store, txn, self.phone.key,
                        **{name: value})
                self.assertEqual(plan.changes, [])
            with self.assertRaises(ValueError):
                plan_update(plan, self.store, txn, self.phone.key,
                    label='x' * 256)
            self.assertEqual(plan.changes, [])
