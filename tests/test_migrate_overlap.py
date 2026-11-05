from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

import lmdb

from phraser import Audio, Phone, Phrase, Speaker, Store, Word
from phraser import struct_value
from scripts import migrate_overlap as migration


class TestMigrateOverlap(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = self.root / 'db'
        self.report = self.root / 'report.json'
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self.path)
        self.addCleanup(self.store.close)
        self.a = self.store.create(Speaker, name='a', save=True)
        self.b = self.store.create(Speaker, name='b', save=True)
        self.keys = []
        self.make_audio('one.wav')
        self.make_audio('two.wav')
        self.store.close()

    def segment(self, cls, audio, start, end, speaker, code):
        return self.store.create(cls, label='speech', start=start, end=end,
            audio_id=audio.identifier, speaker_id=speaker.identifier,
            overlap_code=code)

    def make_audio(self, filename):
        audio = self.store.create(Audio, filename=filename, duration=6000,
            save=True)
        phrase = self.segment(Phrase, audio, 0, 5000, self.a, 0)
        first = self.segment(Word, audio, 0, 2000, self.a, 9)
        last = self.segment(Word, audio, 3000, 5000, self.a, 1)
        phrase.add_children([first, last])
        other = self.segment(Phrase, audio, 2200, 2800, self.b, 1)
        word = self.segment(Word, audio, 2200, 2800, self.b, 9)
        other.add_children([word])
        orphan = self.segment(Phone, audio, 2300, 2500, self.a, 0)
        self.store.save_phrase_trees([phrase, other])
        self.store.save(orphan)
        self.keys.extend([phrase.key, first.key, last.key, other.key,
            word.key, orphan.key])

    def snapshot(self):
        env = lmdb.open(str(self.path), readonly=True, create=False,
            max_dbs=3)
        try:
            result = {}
            for name in (b'main', b'label_segment', b'speaker_audio'):
                db = env.open_db(name, create=False)
                with env.begin(db=db) as txn:
                    result[name] = dict(txn.cursor())
            return result
        finally:
            env.close()

    def test_dry_run_is_readonly_and_reports_changes(self):
        before = self.snapshot()
        data = (self.path / 'data.mdb').read_bytes()
        self.report = self.root / 'db.overlap-dry-run.json'
        output = io.StringIO()
        with redirect_stdout(output):
            result = migration.migrate_overlap(self.path)
        self.assertIn('2/2:', output.getvalue())
        self.assertEqual(result['report_path'], str(self.report.resolve()))
        self.assertEqual(before, self.snapshot())
        self.assertEqual(data, (self.path / 'data.mdb').read_bytes())
        self.assertEqual(result['mode'], 'dry-run')
        self.assertTrue(result['complete'])
        self.assertFalse(result['verified'])
        self.assertEqual(result['summary']['audios_processed'], 2)
        self.assertEqual(result['summary']['levels']['Phrase']['0->1'], 2)
        self.assertEqual(result['summary']['levels']['Word']['9->0'], 4)
        self.assertEqual(result['summary']['levels']['Word']['1->0'], 2)
        self.assertEqual(result['summary']['levels']['Phone']['0->1'], 2)
        self.assertEqual(result['summary']['phrases_without_overlapping_words'],
            2)
        self.assertEqual(json.loads(self.report.read_text()), result)

    def test_apply_preserves_every_byte_except_overlap_codes(self):
        env, main = migration._open_database(self.path, readonly=False)
        try:
            with env.begin(write=True, db=main) as txn:
                raw = txn.get(self.keys[0])
                txn.put(self.keys[0], raw[:1] + b'\x07' + raw[2:])
        finally:
            env.close()
        before = self.snapshot()
        result = migration.migrate_overlap(self.path, self.report, apply=True)
        after = self.snapshot()
        self.assertTrue(result['verified'])
        self.assertTrue(result['complete'])
        self.assertEqual(before[b'label_segment'], after[b'label_segment'])
        self.assertEqual(before[b'speaker_audio'], after[b'speaker_audio'])
        self.assertEqual(before[b'main'].keys(), after[b'main'].keys())
        for key, raw in before[b'main'].items():
            new = after[b'main'][key]
            if raw == new: continue
            object_type = migration.key_helper.key_to_object_type(key)
            layout = struct_value.LAYOUTS[object_type.lower()]
            offset = layout['fixed_fields'].index('overlap_code')
            self.assertEqual(raw[:offset], new[:offset])
            self.assertEqual(raw[offset + 1:], new[offset + 1:])
        for index, key in enumerate(self.keys):
            object_type = migration.key_helper.key_to_object_type(key)
            fields = struct_value.unpack_instance(object_type,
                after[b'main'][key])
            expected = [1, 0, 0, 0, 0, 1][index % 6]
            self.assertEqual(fields['overlap_code'], expected)

    def test_resume_retains_counts_and_checks_completed_audios(self):
        first = migration.migrate_overlap(self.path, self.report, apply=True,
            limit=1)
        self.assertFalse(first['complete'])
        self.assertTrue(first['verified'])
        self.assertEqual(first['summary']['audios_processed'], 1)
        result = migration.migrate_overlap(self.path, self.report, apply=True,
            resume=True)
        self.assertTrue(result['complete'])
        self.assertEqual(result['summary']['levels']['Phone']['0->1'], 2)
        snapshot = self.snapshot()
        again = migration.migrate_overlap(self.path, self.report, apply=True,
            resume=True)
        self.assertEqual(result, again)
        self.assertEqual(snapshot, self.snapshot())

    def test_resume_rejects_changed_completed_audio(self):
        result = migration.migrate_overlap(self.path, self.report, apply=True,
            limit=1)
        completed = next(iter(result['audios']))
        key = next(key for key in self.keys if key[1:9].hex() == completed)
        env, main = migration._open_database(self.path, readonly=False)
        try:
            with env.begin(write=True, db=main) as txn:
                raw = txn.get(key)
                txn.put(key, raw[:1] + b'\x05' + raw[2:])
        finally:
            env.close()
        with self.assertRaisesRegex(ValueError, 'Completed audio changed'):
            migration.migrate_overlap(self.path, self.report, apply=True,
                resume=True)

    def test_refuses_overwrite_and_reusing_dry_run_as_apply_checkpoint(self):
        migration.migrate_overlap(self.path, self.report)
        with self.assertRaises(FileExistsError):
            migration.migrate_overlap(self.path, self.report, apply=True)
        with self.assertRaisesRegex(ValueError, 'Checkpoint mismatch: mode'):
            migration.migrate_overlap(self.path, self.report, apply=True,
                resume=True)

    def test_verification_rejects_incorrect_stored_codes(self):
        report = migration.migrate_overlap(self.path, self.report, apply=True)
        env, main = migration._open_database(self.path, readonly=False)
        try:
            with env.begin(write=True, db=main) as txn:
                key = self.keys[0]
                raw = txn.get(key)
                txn.put(key, raw[:2] + b'\x00' + raw[3:])
            with env.begin() as txn:
                audio_id = key[1:9]
                rows = migration._load_rows(txn, main, audio_id)
            record = report['audios'][audio_id.hex()]
            record['fingerprint'] = migration._fingerprint(rows)
        finally:
            env.close()
        with self.assertRaisesRegex(ValueError, 'Overlap verification failed'):
            migration._verify(self.path, report)

    def test_invalid_paths_and_options(self):
        with self.assertRaisesRegex(ValueError, 'Existing LMDB'):
            migration.migrate_overlap(self.root / 'missing', self.report)
        with self.assertRaisesRegex(ValueError, 'outside the LMDB'):
            migration.migrate_overlap(self.path, self.path / 'report.json')
        with self.assertRaisesRegex(ValueError, 'Resume requires apply'):
            migration.migrate_overlap(self.path, self.report, resume=True)
        with self.assertRaisesRegex(ValueError, 'Limit must be positive'):
            migration.migrate_overlap(self.path, self.report, limit=0)


if __name__ == '__main__':
    unittest.main()
