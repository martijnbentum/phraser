'''Store-based assessment, selection and simple sharing interpretation.'''

import io
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from test_audit import AuditCase

from phraser.audit import audit_store, load_audit_data
from phraser.audit import load_report
from phraser.models import Word
from phraser.storage import struct_value


class TestAuditStatus(AuditCase):
    def save_row(self, row):
        self.store.DB.write(row.key, struct_value.pack_instance(row),
            overwrite=True)

    def audit(self, **kwargs):
        return audit_store(self.store, save=False, progress=False, **kwargs)

    def duration_population(self):
        phones = []
        for index in range(5):
            start = index * 1000
            end = start + (900 if index == 4 else 50)
            _, _, _, children = self.make_tree(start=start,
                phone_spans=[(start, end)])
            phones.extend(children)
        return phones

    def test_stored_assessments_and_unjudged_are_read_only(self):
        phones = self.duration_population()
        phones[-1].duration_outlier = 'long'
        phones[0].duration_outlier = 'short'
        for phone in (phones[0], phones[-1]): self.save_row(phone)
        transaction = self.store.DB.env.info()['last_txnid']
        report = self.audit(checks=['outliers_phone'], min_count=3)
        result = report.checks.outliers_phone
        self.assertEqual(result.n_findings, 1)
        self.assertEqual(result.findings[0].status, 'acknowledged')
        self.assertEqual(result.findings[0].assessment_status, 'matching')
        assessment = result.assessments[0]
        self.assertEqual(assessment.current_assessment, 'within_range')
        self.assertEqual(result.assessments[0].assessment_status, 'conflicting')
        self.assertEqual(result.assessment_counts,
            {'conflicting': 1, 'unset': 3, 'matching': 1})
        unjudged = self.audit(checks='outliers_phone', min_count=10)
        self.assertEqual(unjudged.checks.outliers_phone.n_findings, 0)
        self.assertEqual(len(unjudged.checks.outliers_phone.assessments), 2)
        for row in unjudged.checks.outliers_phone.assessments:
            self.assertEqual(row.assessment_status, 'unjudged')
        self.assertEqual(transaction, self.store.DB.env.info()['last_txnid'])

    def test_status_report_roundtrip(self):
        phones = self.duration_population()
        phones[0].duration_outlier = 'long'
        self.save_row(phones[0])
        report = self.audit(checks=['outliers_phone'], min_count=3)
        root = Path(self.directory.name) / 'reports'
        loaded = load_report(report.save(root))
        self.assertEqual(vars(loaded.info), vars(report.info))
        old = report.checks.outliers_phone
        new = loaded.checks.outliers_phone
        self.assertEqual(new.assessment_counts, old.assessment_counts)
        self.assertEqual(vars(new.assessments[0]), vars(old.assessments[0]))
        self.assertEqual(vars(new.findings[0]), vars(old.findings[0]))
        with redirect_stdout(io.StringIO()) as output: loaded.summary()
        self.assertIn('needs_attention', output.getvalue())
        self.assertIn('assessments', output.getvalue())

    def test_only_required_rows_and_indexes_loaded(self):
        self.make_tree()
        with patch.object(self.store.DB, 'all_label_index_keys') as labels:
            with patch.object(self.store.DB, 'all_links') as links:
                data = load_audit_data(self.store, checks=['outliers_phone'])
                report = self.audit(data=data, checks=['outliers_phone'])
        labels.assert_not_called()
        links.assert_not_called()
        self.assertEqual(data.words, [])
        self.assertEqual(data.speakers, [])
        self.assertEqual(report.info.selected_checks, ['outliers_phone'])
        self.assertIsNotNone(report.info.transaction_id)
        with self.assertRaisesRegex(ValueError, 'lacks dependencies'):
            self.audit(data=data, checks=['within_parent'])

    def test_bad_names_and_population_scope_are_explicit(self):
        with self.assertRaisesRegex(ValueError, 'Unknown audit checks'):
            self.audit(checks=['typo'])
        report = self.audit(checks=['outliers_phone'],
            audio_ids=[self.audio.identifier])
        self.assertEqual(report.results, [])
        self.assertIn('outliers_phone', report.info.skipped_checks)

    def test_progress_includes_loading_checks_and_completion(self):
        self.make_tree()
        messages = []
        audit_store(self.store, checks=['within_parent'], save=False,
            progress=messages.append)
        self.assertIn('scanning main keys', messages[0])
        self.assertTrue(any('loading Phone' in message for message in messages))
        self.assertIn('checking within_parent', messages[-2])
        self.assertTrue(messages[-1].endswith('complete'))

    def shared_case(self):
        phrase, word, syllable, phones = self.make_tree(
            phone_spans=[(0, 50), (50, 120)])
        phrase.end = 200
        syllable.end = 120
        word.shared_phone = True
        syllable.shared_phone = True
        phones[-1].shared_phone = True
        receiver = Word('next', 100, 200, **self.identity())
        receiver.missing_shared_phone = True
        # add_children establishes the persisted parent and phrase references.
        phrase.add_children([word, receiver])
        for row in (phrase, word, syllable, phones[-1]): self.save_row(row)
        self.store.save(receiver)
        return phrase, word, syllable, phones, receiver

    def test_sharing_keeps_raw_finding_but_explains_it(self):
        self.shared_case()
        report = self.audit(checks=['within_parent'])
        self.assertEqual(report.n_errors, 1)
        self.assertEqual(report.n_attention_errors, 0)
        finding = report.checks.within_parent.findings[0]
        self.assertEqual(finding.status, 'explained')
        self.assertEqual(finding.end_excess_ms, 20)

    def test_unexplained_extra_tail_and_audio_overrun_stay_errors(self):
        _, _, syllable, _, _ = self.shared_case()
        syllable.end = 121
        self.save_row(syllable)
        report = self.audit(checks=['within_parent'])
        self.assertEqual(report.n_attention_errors, 1)
        syllable.end = 120
        self.save_row(syllable)
        self.audio.duration = 110
        self.save_row(self.audio)
        report = self.audit(checks=['within_parent', 'within_audio'])
        self.assertGreater(report.checks.within_audio.n_findings, 0)
        self.assertEqual(report.checks.within_parent.findings[0].status,
            'needs_attention')

    def test_stale_or_missing_sharing_relationships_do_not_explain(self):
        _, _, _, phones, receiver = self.shared_case()
        receiver.missing_shared_phone = False
        self.save_row(receiver)
        report = self.audit(checks=['within_parent'])
        self.assertEqual(report.n_attention_errors, 1)
        receiver.missing_shared_phone = True
        self.save_row(receiver)
        phones[-1].speaker_id = b'bad-link'
        self.save_row(phones[-1])
        report = self.audit(checks=['within_parent'])
        self.assertEqual(report.n_attention_errors, 1)

    def test_each_selected_check_matches_full_audit(self):
        from phraser.audit.report import _result_to_json
        from phraser.audit.selection import DEPENDENCIES

        self.make_tree(phone_spans=[(0, 50), (50, 150)])
        full = self.audit(min_count=1)
        by_name = {result.name: result for result in full.results}
        for name in DEPENDENCIES:
            with self.subTest(check=name):
                report = self.audit(checks=[name], min_count=1)
                if name == 'speaker_tempo':
                    self.assertEqual(report.speakers, full.speakers)
                else:
                    self.assertEqual(_result_to_json(report.results[0]),
                        _result_to_json(by_name[name]))

    def test_legacy_reports_without_status_or_provenance_load(self):
        import json

        self.make_tree(phone_spans=[(0, 150)])
        report = self.audit(checks=['within_parent'])
        root = report.save(Path(self.directory.name) / 'legacy')
        path = root / 'within_parent.json'
        content = json.loads(path.read_text())
        content.pop('assessments')
        content.pop('assessment_counts')
        for finding in content['findings']:
            finding.pop('status')
        path.write_text(json.dumps(content))
        loaded = load_report(root)
        self.assertEqual(loaded.n_errors, report.n_errors)
        self.assertEqual(loaded.n_attention_errors, report.n_errors)
