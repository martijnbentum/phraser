import importlib
from pathlib import Path
from stat import S_IMODE
import tempfile
import unittest
from unittest.mock import patch

from phraser import Audio, ClosedStoreError, Phrase, Speaker, Store
from phraser import compact, copy_store, make_markers

copy_module = importlib.import_module('phraser.copy')


class CopyStoreTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = Store(path=self.root / 'source')
        self.addCleanup(self.source.close)
        self.audio = self.source.create(Audio, filename='/audio.wav',
            duration=1000, save=True)
        self.speaker = self.source.create(Speaker, name='speaker',
            dataset='test', save=True)
        self.speaker.add_audio(self.audio)
        phrase = self.source.create(Phrase, label='phrase', start=0, end=500,
            audio_id=self.audio.identifier, speaker_id=self.speaker.identifier,
            save=True)
        make_markers([dict(label='sample', start=100, end=200,
            audio=self.audio, phrase=phrase)], self.source, save=True)
        self.destination = self.root / 'copy'

    def database_bytes(self, store):
        result = {}
        for name in store.DB.db_names:
            with store.DB.env.begin(db=store.DB.db[name]) as txn:
                result[name] = list(txn.cursor())
        return result

    def test_whole_copy_preserves_all_databases(self):
        expected = self.database_bytes(self.source)
        for compact_copy in (True, False):
            destination = self.root / str(compact_copy)
            goal = copy_store(self.source, destination, compact=compact_copy)
            try:
                self.assertEqual(self.database_bytes(goal), expected)
                speaker = goal.load(self.speaker.key)
                self.assertEqual([a.key for a in speaker.audios],
                    [self.audio.key])
            finally:
                goal.close()
        self.assertTrue(self.source.is_open())

    def test_invalid_destinations_leave_source_unchanged(self):
        expected = self.database_bytes(self.source)
        for destination in (Path(self.source.path),
            Path(self.source.path) / 'nested'):
            with self.assertRaises(ValueError):
                copy_store(self.source, destination)
        self.destination.mkdir()
        with self.assertRaises(FileExistsError):
            copy_store(self.source, self.destination)
        self.assertEqual(self.database_bytes(self.source), expected)

    def test_whole_copy_preserves_private_permissions(self):
        source = Path(self.source.path)
        source.chmod(0o700)
        (source / 'data.mdb').chmod(0o600)
        for compact_copy in (True, False):
            destination = self.root / str(compact_copy)
            goal = copy_store(self.source, destination, compact=compact_copy)
            try:
                mode = S_IMODE((destination / 'data.mdb').stat().st_mode)
                self.assertEqual(mode, 0o600)
                self.assertEqual(S_IMODE(destination.stat().st_mode), 0o700)
            finally:
                goal.close()

    def test_interrupted_copy_cleans_destination_and_can_retry(self):
        expected = self.database_bytes(self.source)

        def interrupt_verification(source, destination):
            mode = S_IMODE(destination.stat().st_mode)
            self.assertEqual(mode, 0o700)
            raise KeyboardInterrupt()

        with patch.object(copy_module, '_verify_copy', interrupt_verification):
            with self.assertRaises(KeyboardInterrupt):
                copy_store(self.source, self.destination)
        self.assertFalse(self.destination.exists())
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)
        goal = copy_store(self.source, self.destination)
        try:
            self.assertEqual(self.database_bytes(goal), expected)
        finally:
            goal.close()

    def test_copy_permission_failure_cleans_destination(self):
        expected = self.database_bytes(self.source)
        with patch.object(Path, 'chmod', side_effect=OSError('chmod failed')):
            with self.assertRaisesRegex(OSError, 'chmod failed'):
                copy_store(self.source, self.destination)
        self.assertFalse(self.destination.exists())
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)

    def test_compaction_reopens_store_and_reclaims_space(self):
        self.source.DB.write(b'temporary', b'x' * 2_000_000)
        self.source.DB.delete(b'temporary')
        expected = self.database_bytes(self.source)
        self.source.audios.all()
        sidecar = Path(self.source.path) / 'notes.txt'
        sidecar.write_text('preserve this')
        result = compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)

        self.assertEqual([a.key for a in self.source.audios.all()],
            [self.audio.key])
        self.assertEqual(sidecar.read_text(), 'preserve this')
        self.assertLess(result['after']['file_bytes'],
            result['before']['file_bytes'])
        self.assertIsNone(result['backup_path'])
        self.assertEqual(list(self.root.glob('.source-*')), [])

    def test_compaction_aborts_when_source_changes(self):
        check = copy_module._check_transaction
        calls = []

        def write_before_replacement(source, transaction_id):
            calls.append(True)
            if len(calls) == 2: source.DB.write(b'new', b'value')
            check(source, transaction_id)

        with patch.object(copy_module, '_check_transaction',
            write_before_replacement):
            with self.assertRaisesRegex(RuntimeError, 'source changed'):
                compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.source.DB.load(b'new'), b'value')
        self.assertEqual(list(self.root.glob('.source-*')), [])

    def test_recovery_failure_reports_original_location(self):
        with patch.object(self.source, 'open',
            side_effect=RuntimeError('cannot reopen')):
            with self.assertRaisesRegex(RuntimeError, 'recovery failed'):
                compact(self.source)
        self.assertFalse(self.source.is_open())
        self.source.open()
        self.assertIsNotNone(self.source.load(self.audio.key))

    def test_optional_backup_preserves_original(self):
        expected = self.database_bytes(self.source)
        result = compact(self.source, keep_backup=True)
        backup = Store(path=result['backup_path'])
        try:
            self.assertEqual(self.database_bytes(backup), expected)
        finally:
            backup.close()

    def test_verification_failure_does_not_replace_original(self):
        expected = self.database_bytes(self.source)
        inode = (Path(self.source.path) / 'data.mdb').stat().st_ino
        with patch.object(copy_module, '_verify_copy',
            side_effect=ValueError('verification failed')):
            with self.assertRaisesRegex(ValueError, 'verification failed'):
                compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)
        self.assertEqual((Path(self.source.path) / 'data.mdb').stat().st_ino,
            inode)
        self.assertEqual(list(self.root.glob('.source-*')), [])

    def test_concurrent_write_aborts_copy(self):
        verify = copy_module._verify_copy

        def verify_then_write(source, destination):
            verify(source, destination)
            source.DB.write(b'new', b'value')

        with patch.object(copy_module, '_verify_copy', verify_then_write):
            with self.assertRaisesRegex(RuntimeError, 'source changed'):
                copy_store(self.source, self.destination)
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.source.DB.load(b'new'), b'value')

    def test_reopening_failure_restores_original(self):
        expected = self.database_bytes(self.source)
        reopen = self.source.open
        calls = []

        def fail_once():
            calls.append(True)
            if len(calls) == 1: raise RuntimeError('reopen failed')
            reopen()

        with patch.object(self.source, 'open', fail_once):
            with self.assertRaisesRegex(RuntimeError, 'reopen failed'):
                compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)

    def test_replacement_failure_restores_original(self):
        expected = self.database_bytes(self.source)
        replace = Path.replace

        def fail_install(path, target):
            if '-compact-' in path.parent.name:
                raise OSError('install failed')
            return replace(path, target)

        with patch.object(Path, 'replace', fail_install):
            with self.assertRaisesRegex(OSError, 'install failed'):
                compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)
        self.assertEqual(list(self.root.glob('.source-*')), [])

    def test_verification_detects_changed_values(self):
        goal = copy_store(self.source, self.destination)
        goal.DB.write(b'changed', b'value')
        goal.close()
        with self.assertRaisesRegex(ValueError, 'database differs'):
            copy_module._verify_copy(self.source, self.destination)

    def test_compaction_preserves_private_permissions(self):
        data = Path(self.source.path) / 'data.mdb'
        data.chmod(0o600)
        compact(self.source)
        self.assertEqual(S_IMODE(data.stat().st_mode), 0o600)

    def test_permission_failure_leaves_original_untouched(self):
        data = Path(self.source.path) / 'data.mdb'
        inode = data.stat().st_ino
        expected = self.database_bytes(self.source)
        with patch.object(Path, 'chmod', side_effect=OSError('chmod failed')):
            with self.assertRaisesRegex(OSError, 'chmod failed'):
                compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(data.stat().st_ino, inode)
        self.assertEqual(self.database_bytes(self.source), expected)
        self.assertEqual(list(self.root.glob('.source-*')), [])

    def test_interrupted_install_restores_original(self):
        expected = self.database_bytes(self.source)
        replace = Path.replace

        def interrupt_install(path, target):
            if '-compact-' in path.parent.name: raise KeyboardInterrupt()
            return replace(path, target)

        with patch.object(Path, 'replace', interrupt_install):
            with self.assertRaises(KeyboardInterrupt): compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)
        self.assertEqual(list(self.root.glob('.source-*')), [])

    def test_interrupted_reopening_restores_original(self):
        expected = self.database_bytes(self.source)
        reopen = self.source.open
        calls = []

        def interrupt_once():
            calls.append(True)
            if len(calls) == 1: raise KeyboardInterrupt()
            reopen()

        with patch.object(self.source, 'open', interrupt_once):
            with self.assertRaises(KeyboardInterrupt): compact(self.source)
        self.assertTrue(self.source.is_open())
        self.assertEqual(self.database_bytes(self.source), expected)
        self.assertEqual(list(self.root.glob('.source-*')), [])

    def test_closed_source_is_rejected(self):
        self.source.close()
        with self.assertRaises(ClosedStoreError): compact(self.source)


if __name__ == '__main__':
    unittest.main()
