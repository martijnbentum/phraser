import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from phraser import Audio, Phone, Phrase, Speaker, Store, Syllable, Word
from phraser.audit import audit_store, compare_reports
from phraser.audit import load_report
from phraser.repair import add_change, apply_change_plan, check_change_state
from phraser.repair import create_change_plan, load_change_plan, plan_audio_ids
from phraser.repair import plan_delete, rollback_changes, save_change_plan
from phraser.repair import changes
from phraser.storage import key_helper


class RepairCase(unittest.TestCase):
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

    def tree(self, audio=None):
        '''Save a phrase whose word has an empty zero-duration syllable.'''
        if audio is None: audio = self.audio
        identity = {'audio_id': audio.identifier,
            'speaker_id': self.speaker.identifier, 'store': self.store}
        phone = Phone('', 10, 10, **identity)
        syllable = Syllable('', 10, 10, **identity)
        syllable.add_children([phone])
        other = Syllable('a', 10, 20, **identity)
        other.add_children([Phone('a', 10, 20, **identity)])
        word = Word('word', 10, 20, **identity)
        word.add_children([syllable, other])
        phrase = Phrase('phrase', 10, 30, **identity)
        phrase.add_children([word])
        with redirect_stdout(io.StringIO()):
            self.store.save_phrase_trees([phrase])
        return phrase, word, syllable, phone

    def plan(self, metadata=None):
        with self.store.DB.env.begin() as txn:
            return create_change_plan(self.store, txn, 'test',
                metadata=metadata)

    def delete_plan(self, keys):
        '''Save a plan deleting the main rows of keys; return its path.'''
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'delete rows')
            main = self.store.DB.db['main']
            for key in keys:
                add_change(plan, 'main', key, txn.get(key, db=main), None)
        return self.save(plan)

    def save(self, plan):
        return save_change_plan(plan, self.root / 'audit', 'test')

    def snapshot(self):
        result = {}
        with self.store.DB.env.begin() as txn:
            for name, database in self.store.DB.db.items():
                with txn.cursor(db=database) as cursor:
                    result[name] = dict(cursor)
        return result


class TestScopedVerification(RepairCase):
    def setUp(self):
        super().setUp()
        self.other_audio = self.store.create(Audio, filename='other.wav',
            duration=1000, save=True)
        self.speaker.add_audio(self.other_audio)
        self.other_tree = self.tree(self.other_audio)
        *_, self.syllable, self.phone = self.tree()

    def audit(self, audio_ids=None):
        with redirect_stdout(io.StringIO()):
            return audit_store(self.store, save=False, audio_ids=audio_ids)

    def test_compare_full_before_with_scoped_after(self):
        before = self.audit()
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'delete')
            plan_delete(plan, self.store, txn, self.syllable.key)
        self.assertEqual(plan_audio_ids(plan), [self.audio.identifier])
        apply_change_plan(self.store, self.save(plan))
        after = self.audit(plan_audio_ids(plan))
        comparison = compare_reports(before, after)
        self.assertEqual(len(comparison.resolved['start_before_end']), 2)
        self.assertEqual(comparison.n_new, 0)
        self.assertEqual(comparison.n_new_errors, 0)
        with redirect_stdout(io.StringIO()) as output: comparison.summary()
        self.assertIn('start_before_end', output.getvalue())

    def test_compare_reports_new_errors(self):
        before = self.audit()
        plan = self.plan()
        before_bytes = self.store.DB.load(self.syllable.key)
        add_change(plan, 'main', self.syllable.key, before_bytes, None)
        apply_change_plan(self.store, self.save(plan))
        comparison = compare_reports(before, self.audit(plan_audio_ids(plan)))
        self.assertEqual(len(comparison.new['parent_exists']), 1)
        self.assertGreater(comparison.n_new_errors, 0)

    def test_numbered_repairs_do_not_interfere_with_saved_audit(self):
        report = self.audit()
        directory = report.save(self.root)
        plan = self.plan(metadata={'audit': report.info.stamp})
        path = save_change_plan(plan, directory, 'reviewed')
        self.assertTrue(path.name.startswith('change_001_'))
        loaded = load_report(directory)
        self.assertEqual(loaded.n_errors, report.n_errors)
        self.assertEqual(loaded.n_warnings, report.n_warnings)

    def test_plan_audio_ids_reads_every_database(self):
        plan = self.plan()
        link = key_helper.pack_speaker_audio_key(self.speaker.identifier,
            self.other_audio.identifier)
        add_change(plan, 'speaker_audio', link, b'', None)
        add_change(plan, 'main', self.audio.key, b'x', b'y')
        expected = sorted([self.audio.identifier, self.other_audio.identifier])
        self.assertEqual(plan_audio_ids(plan), expected)




class TestChangePlans(RepairCase):
    def index_plan(self):
        plan = self.plan()
        add_change(plan, 'label_segment', b'one', None, b'')
        add_change(plan, 'label_segment', b'two', None, b'value')
        return self.save(plan)

    def test_insert_update_delete_and_empty_values(self):
        plan = self.plan()
        before = self.store.DB.load(self.audio.key)
        add_change(plan, 'main', self.audio.key, before, b'updated')
        add_change(plan, 'label_segment', b'insert', None, b'')
        path = self.save(plan)
        snapshot = self.snapshot()
        apply_change_plan(self.store, path)
        self.assertEqual(self.store.DB.load(self.audio.key), b'updated')
        self.assertEqual(self.store.DB.load(b'insert', 'label_segment'), b'')
        rollback_changes(self.store, path)
        self.assertEqual(self.snapshot(), snapshot)

    def test_preview_apply_and_exact_rollback(self):
        *_, syllable, phone = self.tree()
        before = self.snapshot()
        path = self.delete_plan([syllable.key, phone.key])
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(load_change_plan(path).count_changes(), {'main': 2})
        apply_change_plan(self.store, path)
        self.assertIsNone(self.store.DB.load(syllable.key))
        self.assertEqual(self.store._cache, {})
        rollback_changes(self.store, path)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(load_change_plan(path).status, 'rolled_back')

    def test_metadata_and_captured_preview(self):
        *_, phone = self.tree()
        plan = self.plan(metadata={'audit': 'original', 'checks': ['timing']})
        add_change(plan, 'main', phone.key, self.store.DB.load(phone.key), None)
        path = self.save(plan)
        phone.label = 'later'
        self.store.save(phone, overwrite=True)
        saved = load_change_plan(path)
        self.assertEqual(saved.metadata, plan.metadata)
        self.assertEqual(saved.changes[0]['context']['before']['label'], '')
        self.assertEqual(saved.count_changes('operation'), {'delete': 1})
        self.assertEqual(saved.count_changes('object_type'), {'Phone': 1})
        with redirect_stdout(io.StringIO()) as output: saved.summary(limit=0)
        self.assertIn('complete affected-row list', output.getvalue())
        self.assertNotIn('later', output.getvalue())

    def test_unrelated_write_does_not_invalidate_proposal(self):
        path = self.index_plan()
        self.store.DB.write(b'unrelated', b'value', db_name='label_segment')
        apply_change_plan(self.store, path)
        self.assertEqual(self.store.DB.load(b'one', 'label_segment'), b'')

    def test_stale_bytes_and_second_application_are_refused(self):
        path = self.index_plan()
        apply_change_plan(self.store, path)
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'planned'):
            apply_change_plan(self.store, path)
        self.assertEqual(self.snapshot(), before)
        plan = load_change_plan(path)
        plan.status = 'planned'
        plan.applied_txn_id = None
        path.write_text(json.dumps(plan.to_json()))
        with self.assertRaisesRegex(ValueError, 'conflict'):
            apply_change_plan(self.store, path)
        self.assertEqual(self.snapshot(), before)

    def test_conflicts_and_write_failure_are_atomic(self):
        path = self.index_plan()
        snapshot = self.snapshot()
        original = changes._write_change
        calls = []

        def fail_second(store, txn, change, field):
            calls.append(change)
            if len(calls) == 2: raise RuntimeError('write failed')
            original(store, txn, change, field)

        with patch.object(changes, '_write_change', fail_second):
            with self.assertRaisesRegex(RuntimeError, 'write failed'):
                apply_change_plan(self.store, path)
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual(load_change_plan(path).status, 'planned')
        self.store.DB.write(b'two', b'conflict', db_name='label_segment')
        snapshot = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'conflict'):
            apply_change_plan(self.store, path)
        self.assertEqual(self.snapshot(), snapshot)

    def test_wrong_database_and_explicit_path(self):
        plan = self.plan()
        path = save_change_plan(plan, path=self.root / 'explicit.json')
        self.assertEqual(path.name, 'explicit.json')
        plan.database['inode'] += 1
        path.write_text(json.dumps(plan.to_json()))
        with self.assertRaisesRegex(ValueError, 'different database'):
            apply_change_plan(self.store, path)
        with self.assertRaisesRegex(ValueError, 'outside'):
            save_change_plan(self.plan(), path=self.store.path / 'bad.json')

    def test_later_write_and_undo_do_not_reactivate_older_undo(self):
        first = self.index_plan()
        apply_change_plan(self.store, first)
        plan = self.plan()
        add_change(plan, 'label_segment', b'three', None, b'3')
        second = self.save(plan)
        apply_change_plan(self.store, second)
        with self.assertRaisesRegex(ValueError, 'stale'):
            rollback_changes(self.store, first)
        rollback_changes(self.store, second)
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'stale'):
            rollback_changes(self.store, first)
        self.assertEqual(self.snapshot(), before)

    def test_rollback_after_reopening(self):
        path = self.index_plan()
        before = self.snapshot()
        apply_change_plan(self.store, path)
        self.store.close()
        with redirect_stdout(io.StringIO()): self.store.open()
        rollback_changes(self.store, path)
        self.assertEqual(self.snapshot(), before)

    def test_read_only_state_check(self):
        path = self.index_plan()
        original_receipt = path.read_bytes()
        self.assertEqual(check_change_state(self.store, path)['state'],
            'original')
        self.store.DB.write(b'one', b'', db_name='label_segment')
        self.assertEqual(check_change_state(self.store, path)['state'], 'mixed')
        self.store.DB.write(b'two', b'value', db_name='label_segment')
        self.assertEqual(check_change_state(self.store, path)['state'],
            'proposed')
        self.store.DB.write(b'two', b'other', db_name='label_segment',
            overwrite=True)
        snapshot = self.snapshot()
        self.assertEqual(check_change_state(self.store, path)['state'],
            'conflicting')
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual(path.read_bytes(), original_receipt)

    def test_noop_is_ambiguous_and_does_not_write_database(self):
        path = self.save(self.plan())
        initial = check_change_state(self.store, path)
        self.assertEqual(initial['state'], 'both')
        apply_change_plan(self.store, path)
        rollback_changes(self.store, path)
        state = check_change_state(self.store, path)
        self.assertEqual(state['transaction_id'], initial['transaction_id'])
        self.assertEqual(state['state'], 'both')

    def test_interrupted_receipt_is_inspected_not_retried(self):
        path = self.index_plan()
        original = changes.write_receipt

        def fail_completion(path, content):
            if content['status'] == 'applied': raise OSError('receipt failed')
            original(path, content)

        with patch.object(changes, 'write_receipt', fail_completion):
            with self.assertRaisesRegex(OSError, 'receipt failed'):
                apply_change_plan(self.store, path)
        self.assertEqual(load_change_plan(path).status, 'applying')
        snapshot = self.snapshot()
        receipt = path.read_bytes()
        self.assertEqual(check_change_state(self.store, path)['state'],
            'proposed')
        with self.assertRaisesRegex(ValueError, 'inspect state'):
            apply_change_plan(self.store, path)
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual(path.read_bytes(), receipt)

    def test_explicit_retry_of_interruption_before_commit(self):
        path = self.index_plan()
        with patch.object(changes, '_execute', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                apply_change_plan(self.store, path)
        self.assertEqual(load_change_plan(path).status, 'applying')
        self.assertEqual(check_change_state(self.store, path)['state'],
            'original')
        with self.assertRaisesRegex(ValueError, 'inspect state'):
            apply_change_plan(self.store, path)
        apply_change_plan(self.store, path, retry_original=True)
        self.assertEqual(load_change_plan(path).status, 'applied')
        self.assertEqual(self.store.DB.load(b'two', 'label_segment'), b'value')
        with self.assertRaisesRegex(ValueError, 'inspect state'):
            apply_change_plan(self.store, path, retry_original=True)
        rollback_changes(self.store, path)
        self.assertEqual(check_change_state(self.store, path)['state'],
            'original')

    def test_explicit_retry_refuses_nonoriginal_rows_atomically(self):
        for state in ('mixed', 'proposed', 'conflicting'):
            with self.subTest(state=state):
                path = self.index_plan()
                plan = load_change_plan(path)
                plan.status = 'applying'
                changes.write_receipt(path, plan.to_json())
                self.store.DB.write(b'two', b'value',
                    db_name='label_segment', overwrite=True)
                if state != 'mixed':
                    value = b'' if state == 'proposed' else b'conflict'
                    self.store.DB.write(b'one', value,
                        db_name='label_segment', overwrite=True)
                snapshot = self.snapshot()
                with self.assertRaisesRegex(ValueError, 'conflict'):
                    apply_change_plan(self.store, path, retry_original=True)
                self.assertEqual(self.snapshot(), snapshot)
                self.assertEqual(load_change_plan(path).status, 'applying')
                with self.store.DB.env.begin(write=True) as txn:
                    database = self.store.DB.db['label_segment']
                    txn.delete(b'one', db=database)
                    txn.delete(b'two', db=database)

    def test_interrupted_undo_is_inspected_not_retried(self):
        path = self.index_plan()
        apply_change_plan(self.store, path)
        original = changes.write_receipt

        def fail_completion(path, content):
            if content['status'] == 'rolled_back':
                raise OSError('receipt failed')
            original(path, content)

        with patch.object(changes, 'write_receipt', fail_completion):
            with self.assertRaisesRegex(OSError, 'receipt failed'):
                rollback_changes(self.store, path)
        self.assertEqual(load_change_plan(path).status, 'rolling_back')
        self.assertEqual(check_change_state(self.store, path)['state'],
            'original')
        with self.assertRaisesRegex(ValueError, 'inspect state'):
            rollback_changes(self.store, path)

    def test_malformed_and_bulk_records_refused(self):
        path = self.index_plan()
        content = json.loads(path.read_text())
        variants = []
        for field, value in (('kind', 'bulk'), ('status', 'nonsense'),
            ('metadata', []), ('transaction_id', 'old')):
            variants.append(dict(content, **{field: value}))
        missing = dict(content)
        del missing['database']
        variants.append(missing)
        bad_hex = json.loads(path.read_text())
        bad_hex['changes'][0]['key'] = 'xyz'
        variants.append(bad_hex)
        duplicate = json.loads(path.read_text())
        duplicate['changes'].append(duplicate['changes'][0])
        variants.append(duplicate)
        snapshot = self.snapshot()
        for variant in variants:
            with self.subTest(record=variant):
                path.write_text(json.dumps(variant))
                with self.assertRaises(ValueError):
                    apply_change_plan(self.store, path)
        self.assertEqual(self.snapshot(), snapshot)

    def test_plan_audio_ids_includes_index_and_skips_speaker(self):
        *_, phone = self.tree()
        plan = self.plan()
        key = key_helper.instance_to_label_index_key(phone)
        add_change(plan, 'label_segment', key, b'', None)
        add_change(plan, 'main', self.speaker.key,
            self.store.DB.load(self.speaker.key), None)
        self.assertEqual(plan_audio_ids(plan), [self.audio.identifier])
