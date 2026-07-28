import contextlib
import inspect
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from textgrid import IntervalTier, TextGrid

from phraser import Store, models
from scripts import build_cgn_awd_textgrid_db as builder


def make_tier(name, intervals, maximum=2.0):
    tier = IntervalTier(name=name, minTime=0, maxTime=maximum)
    for start, end, mark in intervals:
        tier.add(start, end, mark)
    return tier


def make_pair(special=False):
    if special:
        words = [
            (0.0, 0.3, '!ggg.'),
            (0.3, 0.4, '_'),
            (0.4, 0.8, 'a'),
        ]
        fon = [
            (0.0, 0.3, '!#'),
            (0.3, 0.4, '_'),
            (0.4, 0.8, 'a'),
        ]
        segments = [
            (0.0, 0.3, '!#'),
            (0.3, 0.45, 't'),
            (0.45, 0.55, '[]'),
            (0.55, 0.65, '#'),
            (0.65, 0.8, 'a'),
        ]
        anchors = [(0.0, 1.0, 'special')]
    else:
        words = [(0.1, 0.6, 'pak'), (0.6, 0.9, 'ta')]
        fon = [(0.1, 0.6, 'pAk'), (0.6, 0.9, 'ta')]
        segments = [
            (0.1, 0.2, 'p'),
            (0.2, 0.5, 'A'),
            (0.5, 0.6, 'k'),
            (0.6, 0.72, 't'),
            (0.72, 0.9, 'a'),
        ]
        anchors = [(0.0, 0.7, 'pak'), (0.7, 1.2, 'ta')]
    ort = TextGrid(minTime=0, maxTime=2.0)
    ort.append(make_tier('N00001', anchors))
    ort.append(make_tier('BACKGROUND', []))
    ort.append(make_tier('COMMENT', []))
    awd = TextGrid(minTime=0, maxTime=2.0)
    awd.append(make_tier('N00001', words))
    awd.append(make_tier('N00001_FON', fon))
    segment_tier = make_tier('N00001_SEG', segments)
    awd.append(segment_tier)
    return ort, awd


def write_test_corpus(root, stems):
    '''Write a small CGN-shaped source tree.'''
    ort_dir = root / 'ort'
    awd_dir = root / 'awd'
    audio_dir = root / 'audio'
    nested_audio = audio_dir / 'nl'
    ort_dir.mkdir()
    awd_dir.mkdir()
    nested_audio.mkdir(parents=True)
    for stem in stems:
        ort, awd = make_pair()
        ort.write(str(ort_dir / f'{stem}.ort'))
        awd.write(str(awd_dir / f'{stem}.awd'))
        (nested_audio / f'{stem}.wav').touch()
    speakers = root / 'speakers.txt'
    speakers.write_text(
        'ID\tsex\tbirthYear\tresRegion\n'
        'N00001\tsex1\t1970\tUtrecht\n', encoding='utf-8')
    return audio_dir, awd_dir, ort_dir, speakers


class TestTierParsing(unittest.TestCase):
    def test_validates_awd_triple_names(self):
        _, awd = make_pair()
        awd.tiers[1].name = 'wrong'
        with self.assertRaisesRegex(ValueError, 'invalid AWD tier triple'):
            builder.awd_speaker_tiers(awd)

    def test_rejects_mismatched_word_and_fon_intervals(self):
        _, awd = make_pair()
        awd.tiers[1].intervals[0].maxTime = 0.5
        with self.assertRaisesRegex(ValueError, 'times differ'):
            builder.awd_speaker_tiers(awd)

    def test_excludes_ort_annotation_tiers(self):
        ort, _ = make_pair()
        tiers = builder.ort_speaker_tiers(ort)
        self.assertEqual(set(tiers), {'N00001'})


class TestMapping(unittest.TestCase):
    def test_maps_compact_cgn_transcription_and_keeps_markers(self):
        mapped = builder.map_cgn_transcription('E+_O~')
        self.assertEqual(mapped, 'ɛi_ɒ̃ː')

    def test_reports_unknown_symbols_without_reinterpreting_them(self):
        report = builder.ImportReport()
        mapped = builder.map_cgn_transcription('J', report)
        self.assertEqual(mapped, 'J')
        self.assertEqual(report.counts['unknown_fon_symbol'], 1)


class TestPhraseTreeConstruction(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(path=Path(self.directory.name) / 'db')
        self.audio = models.Audio(filename='/tmp/fn000001.wav',
            duration=2000, n_channels=1, sample_rate=16000, dataset='cgn',
            language='nld', dialect='nl-NL', store=self.store)
        self.speaker = models.Speaker(name='N00001', dataset='cgn',
            language='nld', dialect='nl-NL', store=self.store)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def build(self, special=False):
        ort, awd = make_pair(special=special)
        report = builder.ImportReport()
        speakers = {'N00001': self.speaker}
        path = Path('/tmp/fn000001.awd')
        phrases = builder.textgrids_to_phrase_trees(ort, awd, self.audio,
            speakers, path, self.store, report)
        return phrases, report

    def test_assigns_boundary_crossing_word_by_midpoint(self):
        phrases, _ = self.build()
        self.assertEqual([word.label for word in phrases[0].words], ['pak'])
        self.assertEqual([word.label for word in phrases[1].words], ['ta'])
        self.assertEqual((phrases[0].start, phrases[0].end), (100, 600))
        self.assertEqual((phrases[1].start, phrases[1].end), (600, 900))

    def test_builds_mapped_word_syllable_phone_tree(self):
        phrases, _ = self.build()
        first = phrases[0].words[0]
        self.assertEqual(first.ipa, 'pɑk')
        self.assertEqual([phone.label for phone in first.phones],
            ['p', 'ɑ', 'k'])
        syllable_count = len(first.syllables)
        self.assertEqual(syllable_count, 1)
        self.assertEqual([phone.position for phone in first.phones],
            ['onset', 'nucleus', 'coda'])

    def test_handles_unreliable_shared_and_skipped_units(self):
        phrases, report = self.build(special=True)
        unreliable, shared, vowel = phrases[0].words
        self.assertEqual(unreliable.label, '!ggg.')
        self.assertEqual(unreliable.phones, [])
        self.assertEqual(shared.label, '_')
        self.assertEqual([phone.label for phone in shared.phones], ['t'])
        self.assertEqual(shared.phones[0].position, 'unknown')
        self.assertEqual([phone.label for phone in vowel.phones], ['aː'])
        self.assertEqual(report.counts['skipped_segment_marker'], 2)
        self.assertEqual(report.counts['fallback_syllable'], 1)

    def test_read_after_write_preserves_hierarchy(self):
        phrases, _ = self.build()
        self.store.save_many([self.audio, self.speaker])
        self.store.DB.write_speaker_audio_link(self.speaker, self.audio)
        self.store.save_phrase_trees(phrases)
        self.store.close()
        self.store.open()
        self.store.refresh_query_roots()
        loaded = list(self.store.phrases.all())
        self.assertEqual(len(loaded), 2)
        self.assertEqual([word.label for word in loaded[1].words], ['ta'])
        self.assertEqual([phone.label for phone in loaded[1].phones],
            ['t', 'aː'])


class TestDatabaseBuild(unittest.TestCase):
    def test_uses_cgn_source_directory_defaults(self):
        signature = inspect.signature(builder.build_cgn_awd_database)
        parameters = signature.parameters
        self.assertEqual(parameters['audio_dir'].default,
            builder.cgn_audio_dir)
        self.assertEqual(parameters['awd_dir'].default, builder.cgn_awd_dir)
        self.assertEqual(parameters['ort_dir'].default, builder.cgn_ort_dir)
        self.assertEqual(parameters['db_path'].default, builder.cgn_db_path)
        self.assertEqual(parameters['speaker_file'].default,
            builder.cgn_speaker_file)
        self.assertEqual(parameters['report_file'].default,
            builder.cgn_report_file)

    def test_collects_audio_and_transcriptions_recursively_by_stem(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            audio_dir = root / 'audio'
            ort_dir = root / 'ort'
            awd_dir = root / 'awd'
            audio_nested = audio_dir / 'third'
            ort_nested = ort_dir / 'one'
            awd_nested = awd_dir / 'another'
            audio_nested.mkdir(parents=True)
            ort_nested.mkdir(parents=True)
            awd_nested.mkdir(parents=True)
            audio = audio_nested / 'fn000001.WAV'
            ort = ort_nested / 'fn000001.ORT'
            awd = awd_nested / 'fn000001.AWD'
            audio.touch()
            ort.touch()
            awd.touch()
            report = builder.ImportReport()
            recordings = builder.collect_cgn_audio_and_transcription_files(
                audio_dir, ort_dir, awd_dir, report=report)
            resolved_audio = audio.resolve()
            resolved_ort = ort.resolve()
            resolved_awd = awd.resolve()
            self.assertEqual(len(recordings), 1)
            self.assertEqual(recordings[0].stem, 'fn000001')
            self.assertEqual(recordings[0].audio_path, resolved_audio)
            self.assertEqual(recordings[0].ort_path, resolved_ort)
            self.assertEqual(recordings[0].awd_path, resolved_awd)
            self.assertEqual(report.counts['paired_recordings'], 1)

    def test_reports_or_rejects_incomplete_source_triples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            audio_dir = root / 'audio'
            ort_dir = root / 'ort'
            awd_dir = root / 'awd'
            audio_dir.mkdir()
            ort_dir.mkdir()
            awd_dir.mkdir()
            (audio_dir / 'fn000001.wav').touch()
            (ort_dir / 'fn000001.ort').touch()
            report = builder.ImportReport()
            recordings = builder.collect_cgn_audio_and_transcription_files(
                audio_dir, ort_dir, awd_dir, report=report)
            self.assertEqual(recordings, [])
            self.assertEqual(report.counts['missing_awd'], 1)
            with self.assertRaisesRegex(ValueError, 'stems do not match'):
                builder.collect_cgn_audio_and_transcription_files(
                    audio_dir, ort_dir, awd_dir, strict=True)

    def test_rejects_missing_annotation_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            audio_dir = root / 'audio'
            awd_dir = root / 'awd'
            audio_dir.mkdir()
            awd_dir.mkdir()
            with self.assertRaisesRegex(
                ValueError, 'ort_dir is not a directory'):
                builder.collect_cgn_audio_and_transcription_files(
                    audio_dir, root / 'missing-ort', awd_dir)

    def test_rejects_duplicate_audio_stems(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            audio_dir = root / 'audio'
            ort_dir = root / 'ort'
            awd_dir = root / 'awd'
            first = audio_dir / 'nl'
            second = audio_dir / 'vl'
            first.mkdir(parents=True)
            second.mkdir()
            ort_dir.mkdir()
            awd_dir.mkdir()
            (first / 'fn000001.wav').touch()
            (second / 'fn000001.wav').touch()
            with self.assertRaisesRegex(ValueError, 'duplicate audio stem'):
                builder.collect_cgn_audio_and_transcription_files(
                    audio_dir, ort_dir, awd_dir)

    def test_refuses_legacy_and_nonempty_targets(self):
        with self.assertRaisesRegex(ValueError, 'legacy CGN DB'):
            builder.validate_target_path(builder.locations.cgn_lmdb)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / 'existing').write_text('data', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'non-empty'):
                builder.validate_target_path(path)
            target = builder.validate_target_path(path, resume=True)
            self.assertEqual(target, path.resolve())

    def test_fresh_state_skips_resume_database_reads(self):
        report = builder.ImportReport()
        audio_patch = mock.patch.object(
            builder, '_get_database_audios_by_filename')
        speaker_patch = mock.patch.object(
            builder, '_get_database_speakers_by_id')
        with audio_patch as get_audios, speaker_patch as get_speakers:
            state = builder.make_cgn_import_state(
                mock.sentinel.store, report, {}, [], resume=False)
        get_audios.assert_not_called()
        get_speakers.assert_not_called()
        self.assertEqual(state.filename_to_audio, {})
        self.assertEqual(state.id_to_speaker, {})
        self.assertFalse(state.audit_stems)
        audio_count = report.counts['database_audio_files']
        audit_count = report.counts['resume_audit_recordings']
        self.assertEqual(audio_count, 0)
        self.assertEqual(audit_count, 0)

    def test_resume_repairs_label_index_and_skips_complete_recording(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = write_test_corpus(root, ['fn000001'])
            audio_dir, awd_dir, ort_dir, speakers = paths
            database = root / 'database'
            audio_info = dict(duration=2000, n_channels=1, sample_rate=16000)
            output = io.StringIO()
            patcher = mock.patch.object(builder.audio_helper, 'audio_info',
                return_value=audio_info)
            redirect = contextlib.redirect_stdout(output)
            recording_progress = mock.patch.object(builder, 'progressbar')
            with patcher, redirect, recording_progress as recording_bar:
                first = builder.build_cgn_awd_database(audio_dir,
                    database, awd_dir=awd_dir, ort_dir=ort_dir,
                    speaker_file=speakers, report_file=None,
                    show_progress=False)
                repair_store = Store(path=database)
                label_keys = repair_store.DB.all_label_index_keys()
                missing_key = label_keys[0]
                repair_store.DB.delete(
                    missing_key, db_name='label_segment')
                repair_store.close()
                second = builder.build_cgn_awd_database(audio_dir,
                    database, awd_dir=awd_dir, ort_dir=ort_dir,
                    speaker_file=speakers, resume=True,
                    report_file=None, show_progress=False)
            recording_bar.assert_not_called()
            self.assertEqual(first.counts['recordings_saved'], 1)
            self.assertEqual(second.counts['recordings_skipped'], 1)
            self.assertEqual(second.counts['recordings_audited'], 1)
            self.assertEqual(second.counts['label_indices_repaired'], 1)
            store = Store(path=database)
            try:
                phrases = list(store.phrases.all())
                self.assertEqual(len(phrases), 2)
                exists = store.DB.key_exists(
                    missing_key, db_name='label_segment')
                self.assertTrue(exists)
            finally:
                store.close()

    def test_resume_audits_only_last_three_database_audios(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stems = [f'fn{index:06d}' for index in range(1, 5)]
            paths = write_test_corpus(root, stems)
            audio_dir, awd_dir, ort_dir, speakers = paths
            database = root / 'database'
            audio_info = dict(duration=2000, n_channels=1, sample_rate=16000)
            output = io.StringIO()
            patcher = mock.patch.object(builder.audio_helper, 'audio_info',
                return_value=audio_info)
            redirect = contextlib.redirect_stdout(output)
            with patcher, redirect:
                builder.build_cgn_awd_database(audio_dir, database,
                    awd_dir=awd_dir, ort_dir=ort_dir,
                    speaker_file=speakers, report_file=None,
                    show_progress=False)
                stage_patcher = mock.patch.object(
                    builder, 'stage_cgn_recording',
                    wraps=builder.stage_cgn_recording)
                with stage_patcher as stage_recording:
                    report = builder.build_cgn_awd_database(
                        audio_dir, database, awd_dir=awd_dir,
                        ort_dir=ort_dir, speaker_file=speakers,
                        resume=True, report_file=None,
                        show_progress=False)
            self.assertEqual(stage_recording.call_count, 3)
            self.assertEqual(report.counts['recordings_fast_skipped'], 1)
            self.assertEqual(report.counts['recordings_audited'], 3)

    def test_resume_completes_tail_audio_without_phrases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = write_test_corpus(root, ['fn000001'])
            audio_dir, awd_dir, ort_dir, speakers = paths
            database = root / 'database'
            filename = audio_dir / 'nl' / 'fn000001.wav'
            store = Store(path=database)
            resolved_filename = str(filename.resolve())
            audio = models.Audio(filename=resolved_filename,
                duration=2000, n_channels=1, sample_rate=16000,
                dataset='cgn', language='nld', dialect='nl-NL',
                store=store)
            store.save(audio)
            store.close()
            output = io.StringIO()
            redirect = contextlib.redirect_stdout(output)
            with redirect:
                report = builder.build_cgn_awd_database(
                    audio_dir, database, awd_dir=awd_dir,
                    ort_dir=ort_dir, speaker_file=speakers,
                    resume=True, report_file=None, show_progress=False)
            self.assertEqual(report.counts['recordings_repaired'], 1)
            store = Store(path=database)
            try:
                phrases = list(store.phrases.all())
                self.assertEqual(len(phrases), 2)
            finally:
                store.close()

    def test_source_error_retries_and_does_not_stop_later_recordings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = write_test_corpus(
                root, ['fn000001', 'fn000002'])
            audio_dir, awd_dir, ort_dir, speakers = paths
            database = root / 'database'
            report_file = root / 'report.json'
            audio_info = dict(duration=2000, n_channels=1, sample_rate=16000)
            original_load = builder.load_textgrid
            failed = []

            def load_with_error(filename):
                if Path(filename).stem == 'fn000001':
                    failed.append(Path(filename).stem)
                    raise ValueError('bad source recording')
                return original_load(filename)

            output = io.StringIO()
            patch_info = mock.patch.object(builder.audio_helper, 'audio_info',
                return_value=audio_info)
            patch_load = mock.patch.object(builder, 'load_textgrid',
                side_effect=load_with_error)
            redirect = contextlib.redirect_stdout(output)
            with patch_info, patch_load, redirect:
                with self.assertRaisesRegex(
                    RuntimeError, 'recording imports failed'):
                    builder.build_cgn_awd_database(
                        audio_dir, database, awd_dir=awd_dir,
                        ort_dir=ort_dir, speaker_file=speakers,
                        report_file=report_file, show_progress=False)
                with self.assertRaisesRegex(
                    RuntimeError, 'recording imports failed'):
                    builder.build_cgn_awd_database(
                        audio_dir, database, awd_dir=awd_dir,
                        ort_dir=ort_dir, speaker_file=speakers,
                        resume=True, report_file=report_file,
                        show_progress=False)
            self.assertEqual(failed, ['fn000001', 'fn000001'])
            report_text = report_file.read_text(encoding='utf-8')
            report = json.loads(report_text)
            self.assertEqual(report['counts']['source_recording_errors'], 1)
            self.assertEqual(report['counts']['recordings_audited'], 1)
            store = Store(path=database)
            try:
                filenames = []
                for audio in store.audios.all():
                    filenames.append(Path(audio.filename).stem)
                self.assertEqual(filenames, ['fn000002'])
            finally:
                store.close()

    def test_database_write_error_aborts_before_next_recording(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = write_test_corpus(
                root, ['fn000001', 'fn000002'])
            audio_dir, awd_dir, ort_dir, speakers = paths
            database = root / 'database'
            report_file = root / 'report.json'
            audio_info = dict(duration=2000, n_channels=1, sample_rate=16000)
            output = io.StringIO()
            patch_info = mock.patch.object(builder.audio_helper, 'audio_info',
                return_value=audio_info)
            write_error = OSError('write failed')
            patch_save = mock.patch.object(
                Store, 'save_phrase_trees', side_effect=write_error)
            redirect = contextlib.redirect_stdout(output)
            with patch_info, patch_save as save_recording, redirect:
                with self.assertRaisesRegex(OSError, 'write failed'):
                    builder.build_cgn_awd_database(
                        audio_dir, database, awd_dir=awd_dir,
                        ort_dir=ort_dir, speaker_file=speakers,
                        report_file=report_file, show_progress=False)
            self.assertEqual(save_recording.call_count, 1)
            report_text = report_file.read_text(encoding='utf-8')
            report = json.loads(report_text)
            self.assertEqual(report['counts']['recording_write_errors'], 1)


if __name__ == '__main__':
    unittest.main()
