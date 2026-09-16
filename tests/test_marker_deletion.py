import io
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

from phraser import ClosedStoreError, Marker, Phrase, Store, bulk_delete_markers
from phraser import key_helper


class TestMarkerDeletion(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = directory.name
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self.path)
        self.addCleanup(self.store.close)

    def _marker(self, label='event', start=0):
        return Marker(label, start, start + 100, b'audio-id', b'speaker!',
            store=self.store)

    def test_exact_matches_only_with_index_and_cache_cleanup(self):
        target = self._marker()
        other = self._marker('Event')
        phrase = Phrase('event', 0, 100, b'audio-id', b'speaker!')
        self.store.save_many([target, other, phrase])
        self.store.refresh_query_roots()
        self.assertEqual(len(self.store.markers), 2)
        with mock.patch.object(self.store, 'load_many',
            side_effect=AssertionError('Deletion must not load objects')):
            self.assertEqual(bulk_delete_markers('event', self.store), 1)
        self.assertIsNone(self.store.get_cached(target.key))
        self.assertFalse(self.store.DB.key_exists(target.key))
        self.assertEqual(self.store.label_to_instances('event', 'Marker'), [])
        self.assertEqual([item.key for item in self.store.markers], [other.key])
        self.assertTrue(self.store.DB.key_exists(phrase.key))
        self.assertTrue(self.store.DB.key_exists(other.label_index_key,
            db_name='label_segment'))
        self.store.close()
        with redirect_stdout(io.StringIO()):
            reopened = Store(path=self.path)
        try:
            self.assertEqual([item.key for item in reopened.markers],
                [other.key])
            self.assertEqual(reopened.label_to_instances('event', 'Marker'), [])
        finally:
            reopened.close()

    def test_multiple_batches(self):
        markers = [self._marker(start=i) for i in range(10_001)]
        self.store.save_many(markers)
        method = self.store.DB.delete_labelled_keys
        with mock.patch.object(self.store.DB, 'delete_labelled_keys',
            wraps=method) as delete:
            self.assertEqual(bulk_delete_markers('event', self.store), 10_001)
        sizes = [len(call.args[0]) for call in delete.call_args_list]
        self.assertEqual(sizes, [10_000, 1])
        self.assertEqual(list(self.store.markers), [])
        self.assertEqual(self.store.DB.all_label_index_keys(), [])

    def test_partial_failure_keeps_committed_batches_consistent(self):
        markers = [self._marker(start=i) for i in range(10_001)]
        self.store.save_many(markers)
        method = self.store.DB.delete_labelled_keys
        calls = 0

        def fail_second_batch(*args):
            nonlocal calls
            calls += 1
            if calls == 2: raise RuntimeError('injected failure')
            return method(*args)

        with mock.patch.object(self.store.DB, 'delete_labelled_keys',
            side_effect=fail_second_batch):
            with self.assertRaisesRegex(RuntimeError, 'injected failure'):
                bulk_delete_markers('event', self.store)
        self.assertEqual(len(self.store.markers), 1)
        self.assertEqual(len(self.store._cache), 1)
        self.assertEqual(len(self.store.DB.all_label_index_keys()), 1)
        self.assertEqual(bulk_delete_markers('event', self.store), 1)

    def test_stale_index_cannot_delete_a_different_label(self):
        marker = self._marker('keep')
        marker.save()
        stale = key_helper.label_to_label_index_key('event', 'Marker',
            marker.key)
        self.store.DB.write_label_index_link(stale)
        self.assertEqual(bulk_delete_markers('event', self.store), 0)
        self.assertTrue(self.store.DB.key_exists(marker.key))

    def test_missing_indexed_record_is_cleaned_up(self):
        marker = self._marker()
        self.store.DB.write_label_index_link(marker.label_index_key)
        self.assertEqual(bulk_delete_markers('event', self.store), 0)
        self.assertEqual(self.store.DB.all_label_index_keys(), [])

    def test_empty_match_and_closed_store(self):
        self.assertEqual(bulk_delete_markers('missing', self.store), 0)
        self.store.close()
        with self.assertRaises(ClosedStoreError):
            bulk_delete_markers('event', self.store)
