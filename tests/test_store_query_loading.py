'''Optional query loading and repair-specific key membership reuse.'''

import io
from contextlib import redirect_stdout
from unittest.mock import patch

from test_repairs import RepairCase

from phraser import Store
from phraser.models import Phone
from phraser.query import queryset_from_items
from phraser.repair import apply_change_plan, create_change_plan, plan_delete
from phraser.repair import plan_update, rollback_changes
from phraser.storage.lmdb_helper import DB


class TestStoreQueryLoading(RepairCase):
    def setUp(self):
        super().setUp()
        self.phrase, self.word, self.syllable, self.phone = self.tree()

    def reopen(self, **kwargs):
        self.store.close()
        with redirect_stdout(io.StringIO()):
            self.store = Store(self.root / 'store', **kwargs)
        self.addCleanup(self.store.close)

    def update_plan(self, **fields):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'update existing row')
            plan_update(plan, self.store, txn, self.phone.key, **fields)
        return self.save(plan)

    def test_default_still_loads_query_keys_at_open(self):
        self.reopen()
        self.assertTrue(self.store.eager_query_roots)
        with patch.object(self.store.DB, 'rank_to_keys_dict') as scan:
            keys = self.store.phones._data.keys
        scan.assert_not_called()
        self.assertIn(self.phone.key, keys)

    def test_opt_out_defers_scan_until_query_evaluation(self):
        with patch.object(DB, 'rank_to_keys_dict', side_effect=AssertionError):
            self.reopen(eager_query_roots=False)
            loaded = self.store.load(self.phone.key)
            self.assertEqual(loaded.label, '')
            query = self.store.phones.filter(label='a')
            repr(query)
            repr(self.store)
            local = queryset_from_items([loaded], self.store)
            self.assertEqual(len(local), 1)
        scan = self.store.DB.rank_to_keys_dict
        with patch.object(self.store.DB, 'rank_to_keys_dict',
            wraps=scan) as read:
            self.assertEqual(len(query), 1)
            self.assertEqual(len(self.store.words), 1)
            self.assertEqual(len(self.store.query_for_class(Phone)), 2)
        self.assertEqual(read.call_count, 1)

    def test_explicit_refresh_scans_even_when_loading_is_deferred(self):
        self.reopen(eager_query_roots=False)
        scan = self.store.DB.rank_to_keys_dict
        with patch.object(self.store.DB, 'rank_to_keys_dict',
            wraps=scan) as read:
            self.store.refresh_query_roots()
            self.assertEqual(len(self.store.phones), 2)
        self.assertEqual(read.call_count, 1)

    def test_sampling_and_store_display_can_request_keys(self):
        self.reopen(eager_query_roots=False, fraction=1)
        self.assertEqual(self.store.fraction, 1)
        self.assertIsNotNone(self.store.get_cached(self.phone.key))
        self.assertIn('Phone', str(self.store))
        self.assertEqual(len(self.store.phones), 2)

    def test_in_place_repair_reuses_keys_but_reloads_objects(self):
        self.reopen()
        keys = self.store.rank_to_keys_dict()
        old = self.store.load(self.phone.key)
        old_query = self.store.phones
        self.assertEqual(len(old_query), 2)
        self.store.preload_class_instances(Phone)
        path = self.update_plan(flags=1)
        with patch.object(self.store.DB, 'rank_to_keys_dict',
            side_effect=AssertionError):
            apply_change_plan(self.store, path)
            self.assertIs(self.store.rank_to_keys_dict(), keys)
            self.assertIsNot(self.store.phones, old_query)
            self.assertEqual(self.store._classes_loaded, {})
            current = self.store.load(self.phone.key)
            self.assertIsNot(current, old)
            self.assertEqual(current.flags, 1)
            self.assertEqual(old.flags, 0)
            self.assertEqual(len(self.store.phones.filter(flags=1)), 1)
            rollback_changes(self.store, path)
            self.assertIs(self.store.rank_to_keys_dict(), keys)
            self.assertEqual(self.store.load(self.phone.key).flags, 0)
            self.assertEqual(len(self.store.phones.filter(flags=1)), 0)

    def test_lazy_apply_and_rollback_do_not_initialize_keys(self):
        self.reopen(eager_query_roots=False)
        path = self.update_plan(flags=1)
        with patch.object(self.store.DB, 'rank_to_keys_dict',
            side_effect=AssertionError):
            apply_change_plan(self.store, path)
            self.assertEqual(self.store.load(self.phone.key).flags, 1)
            rollback_changes(self.store, path)
            self.assertEqual(self.store.load(self.phone.key).flags, 0)
        self.assertFalse(hasattr(self.store, '_rank_to_keys_dict'))
        self.assertEqual(len(self.store.phones), 2)

    def test_label_index_edits_do_not_invalidate_main_key_membership(self):
        self.reopen()
        path = self.update_plan(label='b')
        with patch.object(self.store.DB, 'rank_to_keys_dict',
            side_effect=AssertionError):
            apply_change_plan(self.store, path)
            self.assertEqual(len(self.store.phones.filter(label='b')), 1)
            rollback_changes(self.store, path)
            self.assertEqual(len(self.store.phones.filter(label='b')), 0)

    def test_deletion_and_rollback_rebuild_membership(self):
        self.reopen(eager_query_roots=False)
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'delete phone')
            plan_delete(plan, self.store, txn, self.phone.key)
        path = self.save(plan)
        scan = self.store.DB.rank_to_keys_dict
        with patch.object(self.store.DB, 'rank_to_keys_dict',
            wraps=scan) as read:
            apply_change_plan(self.store, path)
            self.assertEqual(len(self.store.phones), 1)
            rollback_changes(self.store, path)
            self.assertEqual(len(self.store.phones), 2)
        self.assertEqual(read.call_count, 2)

    def test_repair_rebuilds_keys_stale_from_prior_normal_writes(self):
        # Setup saved rows after the original empty Store was initialized.
        self.assertEqual(self.store.phones._data.keys, [])
        path = self.update_plan(flags=1)
        scan = self.store.DB.rank_to_keys_dict
        with patch.object(self.store.DB, 'rank_to_keys_dict',
            wraps=scan) as read:
            apply_change_plan(self.store, path)
            self.assertEqual(len(self.store.phones), 2)
        self.assertEqual(read.call_count, 1)
