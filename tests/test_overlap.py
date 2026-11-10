from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from phraser import Audio, Phone, Phrase, Speaker, Store, Syllable, Word
from phraser import overlap
from scripts import load_cgn_webmaus_textgrids_to_db as webmaus


class TestOverlap(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / 'db')
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self.path)
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='overlap.wav',
            duration=6000, save=True)
        self.a = self.store.create(Speaker, name='a', save=True)
        self.b = self.store.create(Speaker, name='b', save=True)

    def segment(self, cls, start, end, speaker=None, label='speech'):
        if speaker is None: speaker = self.a
        return self.store.create(cls, label=label, start=start, end=end,
            audio_id=self.audio.identifier, speaker_id=speaker.identifier)

    def pause_example(self):
        phrase_a = self.segment(Phrase, 0, 5000)
        first = self.segment(Word, 0, 2000)
        last = self.segment(Word, 3000, 5000)
        phrase_a.add_children([first, last])
        phrase_b = self.segment(Phrase, 2200, 2800, self.b)
        word_b = self.segment(Word, 2200, 2800, self.b)
        phrase_b.add_children([word_b])
        self.store.save_phrase_trees([phrase_a, phrase_b])
        return [phrase_a, first, last, phrase_b, word_b]

    def test_pause_example_before_after_and_reopen(self):
        items = self.pause_example()
        expected = [True, False, False, False, False]
        self.assertEqual([item.overlap for item in items], expected)
        self.assertEqual([item.overlap_code for item in items], [9] * 5)
        self.assertEqual(items[0].overlap_items, [items[-1]])
        keys = [item.key for item in items]
        overlap.update_audio_overlap(self.audio)
        self.assertEqual([item.overlap for item in items], expected)
        self.assertEqual([item.overlap_code for item in items],
            [1, 0, 0, 0, 0])
        self.store.close()
        with redirect_stdout(io.StringIO()):
            reopened = Store(path=self.path)
        self.addCleanup(reopened.close)
        loaded = reopened.load_many(keys)
        self.assertEqual([item.overlap for item in loaded], expected)
        for item, result in zip(loaded, expected):
            self.assertEqual(bool(item.overlap_items), result)

    def test_phrase_intersections_do_not_change_word_based_status(self):
        phrase, first, last, other_phrase, word = self.pause_example()
        self.assertEqual(word.overlap_phrases, [phrase])
        self.assertEqual(word.overlap_words, [])
        self.assertEqual(word.overlap_items, word.overlap_words)
        self.assertEqual(word.overlap_all_items(), [phrase])
        self.assertFalse(word.overlap)
        self.assertEqual(word.overlap_code, 9)

    def test_inspect_other_speaker_segments_at_each_level(self):
        target = self.segment(Phone, 100, 200)
        for cls in (Phrase, Word, Syllable, Phone):
            hit = self.segment(cls, 150, 250, self.b)
            earlier = self.segment(cls, 110, 190, self.b)
            own = self.segment(cls, 110, 190)
            left = self.segment(cls, 0, 100, self.b)
            right = self.segment(cls, 200, 300, self.b)
            foreign = self.segment(cls, 110, 190, self.b)
            foreign.audio_id = b'other-id'
            self.store.save_many([hit, earlier, own, left, right, foreign])
            attr = 'overlap_' + cls.__name__.lower() + 's'
            with self.subTest(level=cls.__name__):
                self.assertEqual(getattr(target, attr), [earlier, hit])
        silence = self.segment(Phone, 120, 140, self.b, '<p:>')
        self.store.save(silence)
        self.assertIn(silence, target.overlap_phones)
        self.assertEqual(target.overlap_items, target.overlap_words)

    def test_unknown_overlap_level_is_rejected(self):
        target = self.segment(Word, 100, 200)
        with self.assertRaisesRegex(ValueError, 'Unsupported overlap level'):
            overlap.overlap_at_level(target, 'Speaker')

    def test_overlap_at_every_level_and_orphans(self):
        phrase = self.segment(Phrase, 0, 1000)
        word = self.segment(Word, 0, 1000)
        syllable = self.segment(Syllable, 0, 1000)
        first = self.segment(Phone, 0, 500)
        last = self.segment(Phone, 500, 1000)
        phrase.add_children([word])
        word.add_children([syllable])
        syllable.add_children([first, last])
        other = self.segment(Word, 600, 900, self.b)
        orphan = self.segment(Phone, 650, 700)
        items = list(phrase.items) + [other, orphan]
        self.store.save_many(items)
        before = [item.overlap for item in items]
        with patch.object(self.store, 'save_many',
            wraps=self.store.save_many) as save:
            overlap.update_audio_overlap(self.audio)
        save.assert_called_once()
        self.assertEqual([item.overlap for item in items], before)
        self.assertEqual([item.overlap_code for item in items],
            [1, 1, 1, 0, 1, 1, 1])

    def test_boundaries_speakers_audio_silence_and_zero_duration(self):
        target = self.segment(Phrase, 100, 200)
        same = self.segment(Word, 110, 190)
        left = self.segment(Word, 0, 100, self.b)
        right = self.segment(Word, 200, 300, self.b)
        zero = self.segment(Word, 150, 150, self.b)
        foreign = self.segment(Word, 110, 190, self.b)
        foreign.audio_id = b'other-id'
        words = [same, left, right, zero, foreign]
        for label in overlap.SILENCE_LABELS:
            words.append(self.segment(Word, 110, 190, self.b, label))
        self.assertEqual(overlap.overlap_items(target, words), [])
        valid = self.segment(Word, 150, 250, self.b)
        words.append(valid)
        self.assertEqual(overlap.overlap_items(target, words), [valid])
        target.end = target.start
        self.assertEqual(overlap.overlap_items(target, words), [])

    def test_nested_reference_intervals_and_unsorted_input(self):
        target = self.segment(Phone, 800, 900)
        wide = self.segment(Word, 0, 1000, self.b)
        early = self.segment(Word, 100, 200, self.b)
        self.assertEqual(overlap.overlap_items(target, [early, wide]), [wide])

    def test_staged_calculation_uses_all_speakers(self):
        phrase = self.segment(Phrase, 0, 5000)
        word = self.segment(Word, 2200, 2800, self.b)
        phrase.overlap_code = 0
        word.overlap_code = 1
        overlap.set_overlap_codes([phrase, word])
        self.assertEqual(phrase.overlap_code, 1)
        self.assertEqual(word.overlap_code, 0)
        self.assertFalse(self.store.DB.key_exists(word.key))

    def test_recomputation_ignores_stale_audio_tree_caches(self):
        items = self.pause_example()
        overlap.update_audio_overlap(self.audio)
        self.assertTrue(items[0].overlap)
        self.assertEqual(len(self.audio.phrases), 2)
        self.assertEqual(len(self.audio.words), 3)
        self.store.delete(items[-1].key)
        overlap.update_audio_overlap(self.audio)
        self.assertFalse(items[0].overlap)
        self.assertEqual(items[0].overlap_items, [])

    def test_empty_audio_does_not_save(self):
        with patch.object(self.store, 'save_many') as save:
            overlap.update_audio_overlap(self.audio)
        save.assert_not_called()

    def test_webmaus_import_updates_previously_stored_segments(self):
        items = self.pause_example()
        overlap.update_audio_overlap(self.audio)
        self.assertFalse(items[1].overlap)
        phrase = self.segment(Phrase, 1000, 1500, self.b)
        word = self.segment(Word, 1000, 1500, self.b)
        phrase.add_children([word])
        filename = str(Path(self.directory.name) / 'new.TextGrid')
        Path(filename).touch()
        info = {'output_filename': filename, 'tier_name': 'b',
            'audio_filename': self.audio.filename}
        speakers = {self.b.name: self.b}
        audios = {self.audio.filename: self.audio}
        with patch.object(webmaus,
            'make_cgn_speaker_name_to_db_speaker_dict',
            return_value=speakers), patch.object(webmaus,
            'make_cgn_audio_filename_to_db_audio_dict',
            return_value=audios), patch.object(webmaus,
            'get_cgn_textgrid_filenames_in_db', return_value=[]), \
            patch.object(webmaus, 'ort_info_to_db_items',
            return_value=list(phrase.items)):
            with redirect_stdout(io.StringIO()):
                webmaus.ort_infos_to_db_items([info], refresh_db=False,
                    store=self.store)
        self.assertTrue(items[1].overlap)
        self.assertTrue(word.overlap)


if __name__ == '__main__':
    unittest.main()
