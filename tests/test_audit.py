import io
import re
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from phraser import Store, audit as run_audit, load_report
from phraser import load_audit_data, update_audio_overlap
from phraser.audit import audit_speakers, check_childless, check_label_index
from phraser.audit import check_children_span_parent, check_orphans
from phraser.audit import check_outliers_phone, check_outliers_phrase
from phraser.audit import check_outliers_word, check_overlap_code_matches
from phraser.audit import check_overlap_computed, check_parent_exists
from phraser.audit import check_phone_positions, check_same_speaker_phrases
from phraser.audit import check_siblings_do_not_overlap, check_start_before_end
from phraser.audit import check_speaker_audio_links, check_within_audio
from phraser.audit import check_syllable_label_matches_phones
from phraser.audit import check_unknown_phone_labels, check_within_parent
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word


def quiet_store(path):
    with redirect_stdout(io.StringIO()):
        return Store(path=path)


def quiet_load(store):
    with redirect_stdout(io.StringIO()):
        return load_audit_data(store)


class AuditCase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'store'
        self.store = quiet_store(self.path)
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='a.wav',
            duration=100_000, save=True)
        self.speaker = self.make_speaker('first')

    def make_speaker(self, name):
        speaker = self.store.create(Speaker, name=name, dataset='test',
            save=True)
        speaker.add_audio(self.audio)
        return speaker

    def identity(self, speaker=None):
        speaker = speaker or self.speaker
        return {'audio_id': self.audio.identifier,
            'speaker_id': speaker.identifier, 'store': self.store}

    def make_tree(self, start=0, phone_spans=None, speaker=None, label='a'):
        '''Build a tree whose phones can be misaligned on purpose.'''
        identity = self.identity(speaker)
        spans = phone_spans or [(start, start + 50), (start + 50, start + 100)]
        phones = []
        for phone_start, phone_end in spans:
            phone = Phone(label, phone_start, phone_end, **identity)
            phone.position = 'nucleus'
            phones.append(phone)
        syllable_label = label * len(phones)
        syllable = Syllable(syllable_label, start, start + 100, **identity)
        word = Word('word', start, start + 100, **identity)
        phrase = Phrase('phrase', start, start + 100, **identity)
        syllable.add_children(phones)
        word.add_children([syllable])
        phrase.add_children([word])
        self.store.save_phrase_trees([phrase])
        return phrase, word, syllable, phones

    def data(self):
        return quiet_load(self.store)


class TestCleanStore(AuditCase):
    def test_clean_tree_has_no_findings(self):
        self.make_tree()
        with redirect_stdout(io.StringIO()):
            update_audio_overlap(self.audio)
            report = run_audit(self.store, save=False, min_count=1)
        for result in report.results:
            with self.subTest(check=result.name):
                self.assertEqual(result.findings, [])
                self.assertGreater(result.n_checked, 0)


class TestWithinParent(AuditCase):
    def test_finds_phones_a_tree_walk_cannot_see(self):
        spans = [(0, 100), (90, 120), (150, 170)]
        phrase, *_ = self.make_tree(phone_spans=spans)
        phrase = self.reload_phrase(phrase)
        self.assertEqual(len(phrase.phones), 2)
        result = check_within_parent(self.data())
        phones = [f for f in result.findings if f.object_type == 'Phone']
        excess = sorted(f.excess_ms for f in phones)
        self.assertEqual(excess, [20, 70])

    def reload_phrase(self, phrase):
        key = phrase.key
        self.store._cache.clear()
        return self.store.load(key)

    def test_groups_violations_by_size(self):
        self.make_tree(phone_spans=[(0, 101)])
        self.make_tree(start=1000, phone_spans=[(1000, 1105)])
        self.make_tree(start=2000, phone_spans=[(2000, 2150)])
        result = check_within_parent(self.data())
        self.assertEqual(result.count_by('size'),
            {'1ms': 1, '<=10ms': 1, '>10ms': 1})

    def test_finding_is_object_that_loads_the_segment(self):
        self.make_tree(phone_spans=[(0, 150)])
        finding = check_within_parent(self.data()).findings[0]
        self.assertEqual(finding.excess_ms, 50)
        self.assertEqual(finding.severity, 'error')
        self.assertEqual(finding.load(self.store).end, 150)


class TestStructureChecks(AuditCase):
    def test_zero_duration(self):
        self.make_tree(phone_spans=[(0, 0)])
        result = check_start_before_end(self.data())
        self.assertEqual(len(result.findings), 1)

    def test_past_audio_end(self):
        self.make_tree(start=100_000)
        result = check_within_audio(self.data())
        self.assertEqual(result.count_by('kind'), {'past_audio_end': 5})

    def test_missing_audio_duration(self):
        self.store.create(Audio, filename='b.wav', duration=0, save=True)
        result = check_within_audio(self.data())
        self.assertEqual(result.count_by('kind'), {'missing_duration': 1})

    def test_missing_parent_and_stale_start(self):
        _, word, syllable, _ = self.make_tree()
        self.store.delete(syllable.key)
        syllable.start = 5
        self.store.save(syllable)
        result = check_parent_exists(self.data())
        self.assertEqual(result.count_by('kind'), {'stale_start': 2})
        self.store.delete(syllable.key)
        result = check_parent_exists(self.data())
        self.assertEqual(result.count_by('kind'), {'missing_row': 2})

    def test_parent_on_other_audio(self):
        phrase, *_ = self.make_tree()
        other = self.store.create(Audio, filename='b.wav', duration=1000,
            save=True)
        word = Word('stray', 0, 50, audio_id=other.identifier,
            speaker_id=self.speaker.identifier, store=self.store,
            parent_id=phrase.identifier, parent_start=phrase.start)
        self.store.save(word)
        result = check_parent_exists(self.data())
        self.assertEqual(result.count_by('kind'), {'other_audio': 1})

    def test_zero_length_sibling_is_not_an_overlap(self):
        self.make_tree(phone_spans=[(0, 100), (50, 50)])
        result = check_siblings_do_not_overlap(self.data())
        self.assertEqual(result.findings, [])

    def test_overlapping_siblings(self):
        self.make_tree(phone_spans=[(0, 60), (50, 100)])
        result = check_siblings_do_not_overlap(self.data())
        self.assertEqual([f.overlap_ms for f in result.findings], [10])

    def test_same_speaker_phrases_only_within_speaker(self):
        self.make_tree()
        other = self.make_speaker('second')
        self.make_tree(start=50, speaker=other)
        self.assertEqual(check_same_speaker_phrases(self.data()).findings, [])
        legacy = Phrase('legacy', 50, 150, **self.identity())
        self.store.save(legacy)
        result = check_same_speaker_phrases(self.data())
        self.assertEqual(result.findings[0].overlap_ms, 50)

    def test_label_index_missing_and_stale_entries(self):
        phrase, *_ = self.make_tree()
        self.store.DB.delete_label_index_link(phrase.label_index_key)
        result = check_label_index(self.data())
        self.assertEqual(result.count_by('kind'), {'missing_entry': 1})
        self.store.DB.write_label_index_link(phrase.label_index_key)
        self.store.delete(phrase.key)
        result = check_label_index(self.data())
        self.assertEqual(result.count_by('kind'), {'stale_entry': 1})

    def test_speaker_audio_links(self):
        self.make_tree()
        self.store.DB.delete_speaker_audio_link(self.speaker, self.audio)
        result = check_speaker_audio_links(self.data())
        self.assertEqual(result.count_by('kind'), {'missing_link': 1})
        self.store.DB.write_speaker_audio_link(self.speaker, self.audio)
        unused = self.make_speaker('unused')
        result = check_speaker_audio_links(self.data())
        self.assertEqual(result.count_by('kind'), {'unused_link': 1})
        self.assertEqual(result.findings[0].speaker_id, unused.identifier)


class TestQualityChecks(AuditCase):
    def test_gaps_in_children(self):
        self.make_tree(phone_spans=[(10, 40), (60, 90)])
        result = check_children_span_parent(self.data())
        self.assertEqual(result.count_by('kind'),
            {'start_gap': 1, 'internal_gap': 1, 'end_gap': 1})

    def test_non_speech_phone_needs_no_position(self):
        phone = Phone('(..)', 500, 600, **self.identity())
        self.store.save(phone)
        self.make_tree()
        result = check_phone_positions(self.data())
        self.assertEqual(result.findings, [])
        self.assertEqual(result.n_checked, 2)


    def test_pause_labels_are_not_speech(self):
        for index, label in enumerate(('<p:>', '(..)', '#', '')):
            start = 500 + index * 100
            phone = Phone(label, start, start + 100, **self.identity())
            self.store.save(phone)
        data = self.data()
        self.assertEqual(len(data.phones), 4)
        self.assertEqual(check_orphans(data).findings, [])
        self.assertEqual(check_unknown_phone_labels(data).findings, [])

    def test_syllable_label_with_or_without_spaces_matches(self):
        _, _, syllable, _ = self.make_tree()
        result = check_syllable_label_matches_phones(self.data())
        self.assertEqual(result.findings, [])
        syllable.label = 'a a'
        self.store.save(syllable, overwrite=True)
        result = check_syllable_label_matches_phones(self.data())
        self.assertEqual(result.findings, [])
        syllable.label = 'b a'
        self.store.save(syllable, overwrite=True)
        result = check_syllable_label_matches_phones(self.data())
        self.assertEqual(result.n_findings, 1)

    def test_unknown_speech_label_is_reported(self):
        self.store.save(Phone('notaphone', 500, 600, **self.identity()))
        result = check_unknown_phone_labels(self.data())
        self.assertEqual([f.label for f in result.findings], ['notaphone'])


class TestOverlapChecks(AuditCase):
    def make_overlap_pair(self):
        '''Build two speakers whose words really intersect.'''
        first, *_ = self.make_tree()
        second = self.make_speaker('second')
        other, *_ = self.make_tree(start=50, speaker=second)
        return first, other

    def test_uncomputed_codes_reported_per_audio_and_class(self):
        self.make_tree()
        result = check_overlap_computed(self.data())
        self.assertEqual(result.n_checked, 4)
        self.assertEqual(result.count_by('object_type'), {'Phrase': 1,
            'Word': 1, 'Syllable': 1, 'Phone': 1})
        phones = [f for f in result.findings if f.object_type == 'Phone']
        self.assertEqual(phones[0].n_uncomputed, 2)
        self.assertEqual(phones[0].share, 1)

    def test_fresh_codes_match_and_are_computed(self):
        self.make_overlap_pair()
        with redirect_stdout(io.StringIO()):
            update_audio_overlap(self.audio)
        data = self.data()
        self.assertEqual(check_overlap_computed(data).findings, [])
        result = check_overlap_code_matches(data)
        self.assertEqual(result.findings, [])
        self.assertEqual(result.n_checked, 10)

    def test_stale_code_is_found_without_changing_data(self):
        first, _ = self.make_overlap_pair()
        with redirect_stdout(io.StringIO()):
            update_audio_overlap(self.audio)
        phrase = self.store.load(first.key)
        self.assertEqual(phrase.overlap_code, 1)
        phrase.overlap_code = 0
        self.store.save(phrase, overwrite=True)
        data = self.data()
        result = check_overlap_code_matches(data)
        self.assertEqual([(f.stored, f.expected) for f in result.findings],
            [(0, 1)])
        self.assertEqual(data.by_key[first.key].overlap_code, 0)

    def test_uncomputed_codes_are_not_mismatches(self):
        self.make_overlap_pair()
        result = check_overlap_code_matches(self.data())
        self.assertEqual(result.findings, [])
        self.assertEqual(result.n_checked, 0)


class TestOutliers(AuditCase):
    def make_phones(self, n, duration, start=0, speaker=None, label='a'):
        '''Save phones in a row, without a tree, and return the next start.'''
        phones = []
        for index in range(n):
            # small variation so the IQR is not only its floor
            end = start + duration + index % 5
            phones.append(Phone(label, start, end, **self.identity(speaker)))
            start = end
        self.store.save_many(phones)
        return start

    def test_flags_extreme_duration_and_reports_every_k(self):
        start = self.make_phones(100, 80)
        self.store.save(Phone('a', start, start + 2000, **self.identity()))
        result = check_outliers_phone(self.data())
        self.assertEqual(result.n_findings, 1)
        finding = result.findings[0]
        self.assertEqual(finding.direction, 'long')
        self.assertEqual(finding.value, 2000)
        group = result.groups[0]
        self.assertEqual(group.n_k_3, 1)
        self.assertGreaterEqual(group.n_k_1_5, group.n_k_3)
        self.assertGreaterEqual(group.n_k_3, group.n_k_5)

    def test_small_group_is_not_judged(self):
        self.make_phones(5, 80)
        result = check_outliers_phone(self.data())
        self.assertEqual(result.n_checked, 0)
        self.assertEqual(result.n_unjudged, 5)
        self.assertFalse(result.groups[0].judged)

    def test_pause_words_do_not_count_as_speech(self):
        word = Word('<p:>', 0, 5000, **self.identity())
        self.store.save(word)
        data = self.data()
        self.assertEqual(check_orphans(data).findings, [])
        self.assertEqual(check_childless(data).findings, [])
        result = check_outliers_word(data, min_count=1)
        self.assertEqual(result.n_unjudged, 0)
        self.assertEqual(result.n_checked, 0)

    def test_min_count_zero_handles_empty_groups(self):
        data = self.data()
        self.assertEqual(check_outliers_word(data, min_count=0).findings, [])
        self.assertEqual(check_outliers_phrase(data, min_count=0).findings, [])

    def test_non_speech_phones_are_excluded(self):
        self.make_phones(40, 2000, label='(..)')
        result = check_outliers_phone(self.data())
        self.assertEqual(result.groups, [])

    def test_slow_speaker_differs_from_badly_aligned_speaker(self):
        slow = self.make_speaker('slow')
        bad = self.make_speaker('bad')
        start = self.make_phones(2000, 80)
        start = self.make_phones(300, 400, start=start, speaker=slow)
        start = self.make_phones(280, 80, start=start, speaker=bad)
        self.make_phones(20, 2000, start=start, speaker=bad)
        rows = {r.name: r for r in audit_speakers(self.data())}
        self.assertGreater(rows['slow'].tempo_vowel, 4)
        self.assertEqual(rows['slow'].n_flagged_without_tempo, 0)
        self.assertAlmostEqual(rows['bad'].tempo_vowel, 1, delta=0.1)
        self.assertEqual(rows['bad'].n_flagged_without_tempo, 20)


class TestAuditRun(AuditCase):
    def audit_quietly(self, **kwargs):
        output = io.StringIO()
        with redirect_stdout(output):
            report = run_audit(self.store, **kwargs)
        return report, output.getvalue()

    def main_rows(self):
        with self.store.DB.env.begin() as txn:
            cursor = txn.cursor(db=self.store.DB.db['main'])
            return list(cursor)

    def test_audit_is_read_only_and_leaves_cache_alone(self):
        self.make_tree(phone_spans=[(0, 150)])
        before = self.main_rows()
        self.store._cache.clear()
        self.audit_quietly(save=False)
        self.assertEqual(self.main_rows(), before)
        self.assertEqual(self.store._cache, {})

    def test_save_false_writes_nothing(self):
        self.make_tree()
        root = Path(self.directory.name) / 'reports'
        self.audit_quietly(save=False, directory=root)
        self.assertFalse(root.exists())

    def test_saves_by_default_into_dated_folder_and_loads_back(self):
        self.make_tree(phone_spans=[(0, 150)])
        root = Path(self.directory.name) / 'reports'
        report, _ = self.audit_quietly(directory=root, min_count=1)
        folders = list(root.iterdir())
        self.assertEqual(len(folders), 1)
        pattern = r'audit_\d{4}(_\d{2}){5}'
        self.assertIsNotNone(re.fullmatch(pattern, folders[0].name))
        loaded = load_report(folders[0])
        self.assertEqual(loaded.n_errors, report.n_errors)
        self.assertEqual(loaded.info.k, 3)
        self.assertEqual(loaded.info.row_counts['phones'], 1)
        finding = loaded.checks.within_parent.findings[0]
        self.assertEqual(finding.excess_ms, 50)
        self.assertEqual(finding.load(self.store).end, 150)
        self.assertEqual(loaded.speakers[0].speaker_id,
            self.speaker.identifier)
        self.assertEqual(len(loaded.outlier_groups),
            len(report.outlier_groups))
        self.assertEqual(len(loaded.checks.outliers_phone.groups),
            len(report.checks.outliers_phone.groups))

    def test_refuses_folder_inside_store_before_loading(self):
        self.make_tree()
        with self.assertRaises(ValueError):
            self.audit_quietly(directory=self.path)
        self.assertEqual(list(self.path.glob('audit_*')), [])

    def test_empty_store_is_called_out(self):
        _, output = self.audit_quietly(save=False)
        self.assertIn('no segments', output)

    def test_passed_data_is_used_instead_of_reading_rows(self):
        self.make_tree()
        data = self.data()
        self.make_tree(start=1000)
        report, _ = self.audit_quietly(save=False, data=data)
        self.assertEqual(report.info.row_counts['phrases'], 1)

    def test_summary_shows_counts_and_sizes(self):
        self.make_tree(phone_spans=[(0, 150)])
        report, _ = self.audit_quietly(save=False)
        output = io.StringIO()
        with redirect_stdout(output):
            report.summary()
        self.assertIn('within_parent', output.getvalue())
        self.assertIn('>10ms', output.getvalue())


if __name__ == '__main__':
    unittest.main()
