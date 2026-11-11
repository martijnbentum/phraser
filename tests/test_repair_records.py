import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from phraser.repair.records import save_record, write_receipt


class TestRepairRecords(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.audit = self.root / 'audit'
        self.record = {'version': 1, 'kind': 'bulk',
            'database': {'path': str(self.root / 'store')},
            'reason': 'Assess flags', 'progress': 0}

    def test_numbered_files_and_directories_share_number_space(self):
        first = save_record(self.record, self.audit, 'flags')
        self.assertEqual(first.name, 'change_001_flags.json')
        (self.audit / 'change_010_previous').mkdir()
        second = save_record(self.record, self.audit, 'labels')
        self.assertEqual(second.name, 'change_011_labels.json')
        self.assertEqual(json.loads(first.read_text()), self.record)

    def test_explicit_path_refuses_overwrite(self):
        path = self.root / 'custom.json'
        self.assertEqual(save_record(self.record, path=path), path)
        before = path.read_bytes()
        with self.assertRaises(FileExistsError):
            save_record(self.record, path=path)
        self.assertEqual(path.read_bytes(), before)

    def test_repeated_receipts_keep_one_record(self):
        path = save_record(self.record, self.audit)
        for progress in (1, 2, 398):
            self.record['progress'] = progress
            write_receipt(path, self.record)
            self.assertEqual(json.loads(path.read_text()), self.record)
        self.assertEqual(list(self.audit.iterdir()), [path])

    def test_failed_replace_leaves_old_receipt_intact(self):
        path = save_record(self.record, self.audit)
        before = path.read_bytes()
        self.record['progress'] = 10
        with patch('phraser.repair.records.os.replace', side_effect=OSError):
            with self.assertRaises(OSError): write_receipt(path, self.record)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(list(self.audit.iterdir()), [path])

    def test_paths_and_names(self):
        with self.assertRaises(ValueError): save_record(self.record)
        with self.assertRaises(ValueError):
            save_record(self.record, self.audit, path=self.root / 'x.json')
        with self.assertRaises(ValueError):
            save_record(self.record, self.audit, '../escape')
        with self.assertRaisesRegex(ValueError, 'outside'):
            save_record(self.record, self.root / 'store' / 'records')
        with self.assertRaises(FileNotFoundError):
            write_receipt(self.root / 'missing.json', self.record)

    def test_symlink_into_database_refused(self):
        store = self.root / 'store'
        store.mkdir()
        alias = self.root / 'alias'
        alias.symlink_to(store, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'outside'):
            save_record(self.record, alias)

    def test_invalid_envelopes_and_payloads_leave_no_files(self):
        bad_records = [None, {}, dict(self.record, kind='unknown'),
            dict(self.record, version=True), dict(self.record, version=2),
            dict(self.record, database={})]
        for content in bad_records:
            with self.subTest(content=content):
                with self.assertRaises(ValueError):
                    save_record(content, self.audit)
        invalid_json = dict(self.record, progress=float('nan'))
        with self.assertRaises(ValueError):
            save_record(invalid_json, self.audit)
        self.assertFalse(self.audit.exists())

    def test_ordinary_envelope_supported(self):
        record = dict(self.record, kind='byte_changes', changes=[])
        path = save_record(record, self.audit)
        self.assertEqual(json.loads(path.read_text()), record)
