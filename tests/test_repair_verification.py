'''Field-based verification and explicit duration-assessment planning.'''

from pathlib import Path
from unittest.mock import patch

from test_audit import AuditCase

from phraser.repair import add_change, apply_change_plan, audit_change_plan
from phraser.repair import create_change_plan, load_change_plan, plan_delete
from phraser.repair import plan_duration_assessments, plan_update
from phraser.repair import save_change_plan, verification_checks
from phraser.repair import verify_change_plan
from phraser.storage import struct_value


class TestRepairVerification(AuditCase):
    def setUp(self):
        super().setUp()
        self.phrase, self.word, self.syllable, self.phones = self.make_tree()

    def plan(self, **fields):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'test repair')
            plan_update(plan, self.store, txn, self.phones[0].key, **fields)
        return plan

    def apply(self, plan):
        path = Path(self.directory.name) / 'repair.json'
        save_change_plan(plan, path=path)
        apply_change_plan(self.store, path)
        return path

    def test_assessment_flags_need_bytes_without_domain_scan(self):
        plan = self.plan(flags=2)
        self.assertEqual(verification_checks(plan)['checks'], [])
        with patch.object(self.store.DB, 'rank_to_keys_dict') as scan:
            before = audit_change_plan(self.store, plan, progress=False)
        scan.assert_not_called()
        path = self.apply(plan)
        verify_change_plan(self.store, path, before, progress=False)
        record = load_change_plan(path)
        self.assertTrue(record.verified)
        self.assertEqual(record.domain_verification['status'], 'not_run')
        self.assertIn('within_parent',
            record.domain_verification['skipped_checks'])

    def test_sharing_and_label_fields_select_relevant_checks(self):
        sharing = verification_checks(self.plan(flags=8))
        self.assertEqual(sharing['checks'], ['within_parent'])
        label = verification_checks(self.plan(label='b'))
        self.assertIn('label_index', label['checks'])
        self.assertIn('syllable_label_matches_phones', label['checks'])
        self.assertNotIn('outliers_phone', label['checks'])

    def test_end_change_reports_new_errors_without_rolling_back(self):
        plan = self.plan(end=150)
        selected = verification_checks(plan)
        self.assertNotIn('label_index', selected['checks'])
        self.assertIn('within_parent', selected['checks'])
        before = audit_change_plan(self.store, plan, progress=False)
        path = self.apply(plan)
        after = verify_change_plan(self.store, path, before, progress=False)
        record = load_change_plan(path)
        self.assertEqual(record.status, 'applied')
        self.assertTrue(record.verified)
        self.assertEqual(record.domain_verification['status'], 'new_errors')
        self.assertGreater(record.domain_verification['new_errors'], 0)
        self.assertGreater(after.n_errors, before.n_errors)
        self.assertEqual(self.store.load(self.phones[0].key).end, 150)

    def test_deletion_includes_index_and_structural_checks(self):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'delete phone')
            plan_delete(plan, self.store, txn, self.phones[0].key)
        selected = verification_checks(plan)
        self.assertIn('label_index', selected['checks'])
        self.assertIn('childless', selected['checks'])
        self.assertIn('syllable_label_matches_phones', selected['checks'])

    def test_unknown_row_changes_require_full_audit(self):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'audio correction')
            raw = txn.get(self.audio.key, db=self.store.DB.db['main'])
            self.audio.duration += 1
            add_change(plan, 'main', self.audio.key, raw,
                struct_value.pack_instance(self.audio))
        selected = verification_checks(plan)
        self.assertIsNone(selected['audio_ids'])
        self.assertIn('outliers_phone', selected['checks'])

    def test_stale_baseline_is_refused(self):
        plan = self.plan(flags=1)
        before = audit_change_plan(self.store, plan, progress=False)
        raw = self.store.DB.load(self.phones[1].key)
        self.store.DB.write(self.phones[1].key, raw, overwrite=True)
        path = self.apply(plan)
        with self.assertRaisesRegex(ValueError, 'fresh baseline'):
            verify_change_plan(self.store, path, before, progress=False)

    def test_plan_additions_are_atomic_and_records_stay_compatible(self):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'two edits')
            plan_update(plan, self.store, txn, self.phones[0].key, end=49)
            original = plan.to_json()
            with self.assertRaisesRegex(ValueError, 'already changes'):
                plan_update(plan, self.store, txn, self.phones[0].key, end=48)
            self.assertEqual(plan.to_json(), original)
            plan_update(plan, self.store, txn, self.phones[1].key, end=99)
        self.assertNotIn('_changes', plan.to_json())
        self.assertEqual(len(plan.changes), 2)
        path = self.apply(plan)
        self.assertEqual(load_change_plan(path).changes, plan.changes)

    def test_low_level_duplicates_are_rejected_at_addition(self):
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'duplicate')
            raw = txn.get(self.phones[0].key, db=self.store.DB.db['main'])
            add_change(plan, 'main', self.phones[0].key, raw, None)
            with self.assertRaisesRegex(ValueError, 'already changes'):
                add_change(plan, 'main', self.phones[0].key, raw, None)
        self.assertEqual(len(plan.changes), 1)

    def test_duration_operation_handles_all_levels_without_writing(self):
        transaction = self.store.DB.env.info()['last_txnid']
        plan = plan_duration_assessments(self.store, 'assess current durations',
            min_count=1, progress=False)
        self.assertEqual(transaction, self.store.DB.env.info()['last_txnid'])
        self.assertEqual(plan.count_changes('object_type'),
            {'Phone': 2, 'Syllable': 1, 'Word': 1, 'Phrase': 1})
        for change in plan.changes:
            self.assertEqual(change['context']['changed_fields'], ['flags'])
            self.assertEqual(change['context']['after']['flags'], 3)

    def test_uncertain_unjudged_and_existing_assessments_are_preserved(self):
        phone = self.phones[0]
        phone.flags = 8 | 4
        self.store.DB.write(phone.key, struct_value.pack_instance(phone),
            overwrite=True)
        self.phones[1].flags = 2
        self.store.DB.write(self.phones[1].key,
            struct_value.pack_instance(self.phones[1]), overwrite=True)
        plan = plan_duration_assessments(self.store, 'review', levels='phone',
            min_count=1, progress=False)
        self.assertEqual(plan.changes, [])
        plan = plan_duration_assessments(self.store, 'reassess', levels='phone',
            min_count=1, reassess=True, progress=False)
        self.assertEqual(len(plan.changes), 1)
        self.assertEqual(plan.changes[0]['key'], self.phones[1].key.hex())
        plan = plan_duration_assessments(self.store, 'too few', levels='phone',
            min_count=20, reassess=True, progress=False)
        self.assertEqual(plan.changes, [])
        counts = plan.metadata['duration_assessment']['counts']
        self.assertEqual(counts['unjudged'], 2)

    def test_fresh_assessment_preserves_non_duration_flags(self):
        phone = self.phones[0]
        phone.flags = 8 | 32
        self.store.DB.write(phone.key, struct_value.pack_instance(phone),
            overwrite=True)
        plan = plan_duration_assessments(self.store, 'assess', levels='phone',
            min_count=1, progress=False)
        self.apply(plan)
        self.assertEqual(self.store.load(phone.key).flags, 8 | 32 | 3)

    def test_lost_sharing_explanation_counts_as_new_error(self):
        from phraser.models import Word

        self.phrase.end = 200
        self.syllable.end = 120
        self.phones[-1].end = 120
        for row in (self.word, self.syllable, self.phones[-1]):
            row.shared_phone = True
        receiver = Word('next', 100, 200, **self.identity())
        receiver.missing_shared_phone = True
        self.phrase.add_children([self.word, receiver])
        for row in (self.phrase, self.word, self.syllable, self.phones[-1]):
            self.store.DB.write(row.key, struct_value.pack_instance(row),
                overwrite=True)
        self.store.save(receiver)
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'remove sharing flag')
            plan_update(plan, self.store, txn, self.word.key, flags=0)
        before = audit_change_plan(self.store, plan, progress=False)
        self.assertEqual(before.n_errors, 1)
        self.assertEqual(before.n_attention_errors, 0)
        path = self.apply(plan)
        after = verify_change_plan(self.store, path, before, progress=False)
        self.assertEqual(after.n_errors, 1)
        self.assertEqual(after.n_attention_errors, 1)
        receipt = load_change_plan(path).domain_verification
        self.assertEqual(receipt['new_findings'], 0)
        self.assertEqual(receipt['new_errors'], 1)

    def test_changed_population_invalidates_assessment_plan(self):
        plan = plan_duration_assessments(self.store, 'assess', levels='word',
            min_count=1, progress=False)
        # Child edits can change normalization without changing Word bytes.
        phone = self.phones[0]
        self.store.DB.delete(phone.key)
        path = Path(self.directory.name) / 'assessment.json'
        save_change_plan(plan, path=path)
        with self.assertRaisesRegex(ValueError, 'fresh assessment plan'):
            apply_change_plan(self.store, path)
        self.assertEqual(load_change_plan(path).status, 'planned')
        word = self.store.load(self.word.key)
        self.assertEqual(word.duration_outlier, 'unset')

    def test_worsened_existing_errors_fail_verification(self):
        phone = self.phones[0]
        phone.end = 150
        self.audio.duration = 120
        for row in (phone, self.audio):
            self.store.DB.write(row.key, struct_value.pack_instance(row),
                overwrite=True)
        plan = self.plan(end=180)
        before = audit_change_plan(self.store, plan, progress=False)
        path = self.apply(plan)
        after = verify_change_plan(self.store, path, before, progress=False)
        receipt = load_change_plan(path).domain_verification
        self.assertEqual(before.n_errors, after.n_errors)
        self.assertEqual(receipt['new_findings'], 0)
        self.assertEqual(receipt['worsened_errors'], 2)
        self.assertEqual(receipt['new_errors'], 2)
        self.assertEqual(receipt['status'], 'new_errors')

    def test_reduced_existing_error_is_not_a_regression(self):
        phone = self.phones[0]
        phone.end = 180
        self.store.DB.write(phone.key, struct_value.pack_instance(phone),
            overwrite=True)
        plan = self.plan(end=150)
        before = audit_change_plan(self.store, plan, progress=False)
        path = self.apply(plan)
        verify_change_plan(self.store, path, before, progress=False)
        receipt = load_change_plan(path).domain_verification
        self.assertEqual(receipt['worsened_errors'], 0)
        self.assertEqual(receipt['status'], 'passed')

    def test_refused_rollback_keeps_domain_verification_evidence(self):
        from phraser.repair import rollback_changes

        plan = self.plan(end=150)
        before = audit_change_plan(self.store, plan, progress=False)
        path = self.apply(plan)
        verify_change_plan(self.store, path, before, progress=False)
        evidence = load_change_plan(path).domain_verification
        raw = self.store.DB.load(self.phones[1].key)
        self.store.DB.write(self.phones[1].key, raw, overwrite=True)
        with self.assertRaisesRegex(ValueError, 'undo is stale'):
            rollback_changes(self.store, path)
        record = load_change_plan(path)
        self.assertEqual(record.status, 'applied')
        self.assertEqual(record.domain_verification, evidence)

    def test_successful_rollback_keeps_original_verification_evidence(self):
        from phraser.repair import rollback_changes

        plan = self.plan(end=150)
        before = audit_change_plan(self.store, plan, progress=False)
        path = self.apply(plan)
        verify_change_plan(self.store, path, before, progress=False)
        evidence = load_change_plan(path).domain_verification
        rollback_changes(self.store, path)
        record = load_change_plan(path)
        self.assertEqual(record.status, 'rolled_back')
        self.assertEqual(record.domain_verification, evidence)
        self.assertTrue(record.rollback_verified)

    def test_merge_plans_and_reject_direct_mutation(self):
        from phraser.repair import merge_change_plan

        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'combined repair')
            other = create_change_plan(self.store, txn, 'part')
            plan_update(plan, self.store, txn, self.phones[0].key, flags=1)
            plan_update(other, self.store, txn, self.phones[1].key, flags=2)
        original = plan.to_json()
        with self.assertRaisesRegex(TypeError, 'merge_change_plan'):
            plan.changes.extend(other.changes)
        with self.assertRaisesRegex(TypeError, 'read-only'):
            plan.changes.clear()
        with self.assertRaisesRegex(TypeError, 'read-only'):
            plan.changes[0] = other.changes[0]
        self.assertEqual(plan.to_json(), original)
        merge_change_plan(plan, other)
        self.assertEqual(len(plan.changes), 2)
        self.assertEqual(len(other.changes), 1)
        self.assertEqual(plan.reason, 'combined repair')
        self.apply(plan)
        self.assertEqual(self.store.load(self.phones[0].key).flags, 1)
        self.assertEqual(self.store.load(self.phones[1].key).flags, 2)

    def test_merge_conflict_leaves_destination_unchanged(self):
        from phraser.repair import merge_change_plan

        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'combined')
            other = create_change_plan(self.store, txn, 'part')
            plan_update(plan, self.store, txn, self.phones[0].key, flags=1)
            plan_update(other, self.store, txn, self.phones[1].key, flags=2)
            plan_update(other, self.store, txn, self.phones[0].key, flags=2)
        original = plan.to_json()
        with self.assertRaisesRegex(ValueError, 'already changes'):
            merge_change_plan(plan, other)
        self.assertEqual(plan.to_json(), original)

    def test_merge_rejects_other_transactions_stores_and_applied_plans(self):
        from phraser.repair import merge_change_plan

        plan = self.plan(flags=1)
        other = self.plan(flags=2)
        other.transaction_id += 1
        with self.assertRaisesRegex(ValueError, 'different transactions'):
            merge_change_plan(plan, other)
        other.transaction_id = plan.transaction_id
        other.database['inode'] += 1
        with self.assertRaisesRegex(ValueError, 'different stores'):
            merge_change_plan(plan, other)
        other.database = dict(plan.database)
        other.status = 'applied'
        with self.assertRaisesRegex(ValueError, 'Only planned'):
            merge_change_plan(plan, other)

    def test_duration_assessment_plans_cannot_lose_policy_through_merging(self):
        from phraser.repair import merge_change_plan

        assessment = plan_duration_assessments(self.store, 'assess',
            min_count=1, progress=False)
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'combined')
        with self.assertRaisesRegex(ValueError, 'applied separately'):
            merge_change_plan(plan, assessment)
        self.assertEqual(plan.changes, [])

    def test_more_negative_duration_counts_as_worsening(self):
        phone = self.phones[1]
        phone.end = phone.start - 1
        raw = struct_value.pack_instance(phone)
        self.store.DB.write(phone.key, raw, overwrite=True)
        phone.end = phone.start - 10
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'byte repair')
            add_change(plan, 'main', phone.key, raw,
                struct_value.pack_instance(phone))
        before = audit_change_plan(self.store, plan, progress=False)
        path = self.apply(plan)
        verify_change_plan(self.store, path, before, progress=False)
        receipt = load_change_plan(path).domain_verification
        self.assertEqual(receipt['worsened_errors'], 1)
        self.assertEqual(receipt['status'], 'new_errors')

    def test_increased_sibling_overlap_counts_as_worsening(self):
        phone = self.phones[0]
        phone.end = 80
        self.store.DB.write(phone.key, struct_value.pack_instance(phone),
            overwrite=True)
        plan = self.plan(end=90)
        before = audit_change_plan(self.store, plan, progress=False)
        path = self.apply(plan)
        verify_change_plan(self.store, path, before, progress=False)
        receipt = load_change_plan(path).domain_verification
        self.assertEqual(receipt['worsened_errors'], 1)
        self.assertEqual(receipt['status'], 'new_errors')
