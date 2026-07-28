import tempfile
import unittest
from unittest import mock

from phraser.lmdb_helper import DB


class TestLmdbBatchWrites(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = DB(path=self.directory.name)
        self.addCleanup(self.database.close)

    def test_late_collision_aborts_the_complete_transaction(self):
        self.database.write(b'existing', b'old')
        keys = [b'new', b'existing']
        values = [b'value', b'changed']

        with self.assertRaisesRegex(KeyError, 'At least one key'):
            self.database.write_many(keys, values)

        new_value = self.database.load(b'new')
        existing_value = self.database.load(b'existing')
        self.assertIsNone(new_value)
        self.assertEqual(existing_value, b'old')

    def test_check_any_key_exist_does_not_scan_all_database_keys(self):
        self.database.write(b'existing', b'value')
        with mock.patch.object(self.database, 'all_keys') as all_keys:
            existing = self.database.check_any_key_exist([b'existing'])
            missing = self.database.check_any_key_exist([b'missing'])
        all_keys.assert_not_called()
        self.assertTrue(existing)
        self.assertFalse(missing)


if __name__ == '__main__':
    unittest.main()
