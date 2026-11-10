from pathlib import Path
import tempfile
import unittest

from phraser import Audio, Phone, Phrase, Speaker, Store, Syllable, Word
from phraser import copy_audio_materials, make_markers
from phraser.storage import key_helper


class CopyMaterialsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = Store(path=self.root / 'source')
        self.addCleanup(self.source.close)
        self.speaker = self.source.create(Speaker, name='shared',
            dataset='test', gender='female', age=37, save=True)
        self.other = self.source.create(Speaker, name='other',
            dataset='test', save=True)
        self.audios = []
        self.phrases = []
        for index in range(3):
            speaker = self.other if index == 2 else self.speaker
            audio = self.source.create(Audio, filename=f'/audio/{index}.wav',
                duration=1000, language='Dutch', save=True)
            phrase = self.make_phrase(audio, speaker, 0)
            self.audios.append(audio)
            self.phrases.append(phrase)
        make_markers([dict(label='old sample', start=0, end=100,
            audio=self.audios[0], phrase=self.phrases[0])], self.source,
            save=True)
        self.destination = self.root / 'goal'

    def make_phrase(self, audio, speaker, start):
        identity = dict(audio_id=audio.identifier,
            speaker_id=speaker.identifier)
        phrase = Phrase('phrase', start, start + 200, **identity)
        word = Word('word', start, start + 200, **identity)
        syllable = Syllable('syllable', start, start + 200, **identity)
        phrase.add_child(word)
        word.add_child(syllable)
        for offset, label in ((0, 'a'), (100, 'n')):
            syllable.add_child(Phone(label, start + offset,
                start + offset + 100, **identity))
        self.source.save_many(phrase.items)
        return phrase

    def copy(self, keys=None):
        if keys is None: keys = [audio.key for audio in self.audios[:2]]
        goal = copy_audio_materials(keys, self.destination, self.source)
        self.addCleanup(goal.close)
        return goal

    def test_subset_preserves_keys_metadata_and_excludes_markers(self):
        expected = {self.speaker.key}
        for audio, phrase in zip(self.audios[:2], self.phrases[:2]):
            expected.add(audio.key)
            expected.update(item.key for item in phrase.items)
        goal = self.copy()
        self.assertEqual(set(goal.all_keys()), expected)
        for key in expected:
            self.assertEqual(goal.DB.load(key), self.source.DB.load(key))
        self.assertEqual(list(goal.markers.all()), [])
        self.assertEqual(len(list(goal.speakers.all())), 1)
        self.assertEqual(len(goal.label_to_instances('word', 'Word')), 2)
        self.assertEqual(len(list(goal.audios.all())), 2)

    def test_reopened_hierarchy_works_after_source_is_closed(self):
        source_word = self.phrases[0].words[0]
        source_word.audio
        source_word.speaker
        source_word.phrase
        source_word.phones
        goal = self.copy()
        self.assertIs(source_word.store, self.source)
        self.assertIs(source_word.audio.store, self.source)
        key = source_word.key
        expected_phones = [phone.key for phone in source_word.phones]
        goal.close()
        self.source.close()
        reopened = Store(path=self.destination)
        self.addCleanup(reopened.close)
        word = reopened.load(key)
        self.assertIs(word.store, reopened)
        self.assertIs(word.audio.store, reopened)
        self.assertIs(word.speaker.store, reopened)
        self.assertIs(word.phrase.store, reopened)
        self.assertEqual([phone.key for phone in word.phones], expected_phones)
        self.assertEqual(word.audio.filename, '/audio/0.wav')
        self.assertEqual(word.speaker.name, 'shared')
        self.assertEqual(word.speaker.age, 37)
        self.assertEqual([item.key for item in word.phrase.words], [key])

    def test_copy_does_not_change_source_records_or_cached_objects(self):
        keys = self.source.all_keys()
        values = self.source.DB.load_many(keys)
        source_word = self.source.load(self.phrases[0].words[0].key)
        cache = dict(self.source._cache)
        self.copy()
        self.assertEqual(self.source.all_keys(), keys)
        self.assertEqual(self.source.DB.load_many(keys), values)
        self.assertEqual(self.source._cache, cache)
        self.assertTrue(self.source.is_open())
        self.assertIs(source_word.store, self.source)

    def test_all_speakers_of_selected_recording_are_copied(self):
        phrase = self.make_phrase(self.audios[0], self.other, 400)
        goal = self.copy([self.audios[0].key])
        self.assertEqual({s.key for s in goal.speakers.all()},
            {self.speaker.key, self.other.key})
        self.assertEqual(goal.load(phrase.key).speaker.key, self.other.key)
        self.assertEqual(len(list(goal.audios.all())), 1)

    def test_duplicate_audio_keys_are_copied_once(self):
        goal = self.copy([self.audios[0].key] * 2)
        self.assertEqual(len(list(goal.audios.all())), 1)
        self.assertEqual(len(list(goal.words.all())), 1)

    def test_missing_or_non_audio_keys_do_not_create_destination(self):
        missing = key_helper.pack_audio_key(b'missing!')
        for keys, error in (([missing], KeyError),
            ([self.phrases[0].key], ValueError)):
            with self.subTest(keys=keys), self.assertRaises(error):
                self.copy(keys)
            self.assertFalse(self.destination.exists())

    def test_nonempty_and_source_destinations_are_rejected(self):
        with self.assertRaises(ValueError):
            copy_audio_materials([], self.source.path, self.source)
        self.destination.mkdir()
        unrelated = self.destination / 'keep.txt'
        unrelated.write_text('unchanged')
        with self.assertRaises(FileExistsError): self.copy()
        self.assertEqual(unrelated.read_text(), 'unchanged')


if __name__ == '__main__':
    unittest.main()
