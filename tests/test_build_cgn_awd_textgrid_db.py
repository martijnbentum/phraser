import contextlib
import io
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

    def test_resume_skips_complete_recording(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ort_dir = root / 'ort'
            awd_dir = root / 'awd'
            ort_dir.mkdir()
            awd_dir.mkdir()
            ort, awd = make_pair()
            ort.write(str(ort_dir / 'fn000001.ort'))
            awd.write(str(awd_dir / 'fn000001.awd'))
            audio = root / 'fn000001.wav'
            audio.touch()
            audio_list = root / 'audio.txt'
            audio_list.write_text(str(audio) + '\n', encoding='utf-8')
            speakers = root / 'speakers.txt'
            speakers.write_text(
                'ID\tsex\tbirthYear\tresRegion\n'
                'N00001\tsex1\t1970\tUtrecht\n', encoding='utf-8')
            database = root / 'database'
            audio_info = dict(duration=2000, n_channels=1, sample_rate=16000)
            output = io.StringIO()
            patcher = mock.patch.object(builder.audio_helper, 'audio_info',
                return_value=audio_info)
            redirect = contextlib.redirect_stdout(output)
            with patcher, redirect:
                first = builder.build_database(ort_dir, awd_dir, audio_list,
                    speakers, database)
                second = builder.build_database(ort_dir, awd_dir, audio_list,
                    speakers, database, resume=True)
            self.assertEqual(first.counts['recordings_saved'], 1)
            self.assertEqual(second.counts['recordings_skipped'], 1)
            store = Store(path=database)
            try:
                phrases = list(store.phrases.all())
                self.assertEqual(len(phrases), 2)
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
