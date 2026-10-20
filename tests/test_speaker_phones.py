import io
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word


class TestSpeakerPhones(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self.directory.name)
        self.audio = self.store.create(Audio, filename='shared.wav',
            duration=1000, save=True)
        self.other_audio = self.store.create(Audio, filename='other.wav',
            duration=1000, save=True)
        self.speaker = self.store.create(Speaker, name='first',
            dataset='test', save=True)
        self.other_speaker = self.store.create(Speaker, name='second',
            dataset='test', save=True)
        self.speaker.add_audio(self.audio)
        self.speaker.add_audio(self.other_audio)
        self.other_speaker.add_audio(self.audio)
        trees = [
            self.make_tree(self.audio, self.speaker, 'late', 400),
            self.make_tree(self.audio, self.other_speaker, 'overlap', 0),
            self.make_tree(self.audio, self.speaker, 'early', 0),
            self.make_tree(self.other_audio, self.speaker, 'other', 0)]
        self.store.save_phrase_trees(trees)
        audio_key, speaker_key = self.audio.key, self.speaker.key
        other_audio_key = self.other_audio.key
        other_speaker_key = self.other_speaker.key
        self.store.close()
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self.directory.name)
        self.addCleanup(self.store.close)
        self.audio = self.store.load(audio_key)
        self.other_audio = self.store.load(other_audio_key)
        self.speaker = self.store.load(speaker_key)
        self.other_speaker = self.store.load(other_speaker_key)

    def make_tree(self, audio, speaker, label, start):
        '''Build one full phrase tree with two phones in reverse order.'''
        identity = {'audio_id': audio.identifier,
            'speaker_id': speaker.identifier, 'store': self.store}
        phrase = Phrase(label, start, start + 200, **identity)
        word = Word(label, start, start + 200, **identity)
        syllable = Syllable(label, start, start + 200, **identity)
        first = Phone(label + '1', start, start + 100, **identity)
        second = Phone(label + '2', start + 100, start + 200, **identity)
        syllable.add_children([second, first])
        word.add_children([syllable])
        phrase.add_children([word])
        return phrase

    def test_speaker_phrase_keys_exclude_other_speakers(self):
        phrases = self.store.load_many(self.speaker.phrase_keys)
        self.assertEqual({phrase.label for phrase in phrases},
            {'early', 'late', 'other'})
        for phrase in phrases:
            self.assertEqual(phrase.speaker_id, self.speaker.identifier)

    def test_speaker_descendants_and_queries_exclude_other_speakers(self):
        for name in ('phrases', 'words', 'syllables', 'phones'):
            with self.subTest(relation=name):
                items = getattr(self.speaker, name)
                expected_count = 6 if name == 'phones' else 3
                self.assertEqual(len(items), expected_count)
                for item in items:
                    self.assertEqual(item.speaker_id, self.speaker.identifier)
                query_items = list(getattr(self.speaker, name + '_query'))
                self.assertEqual({item.key for item in query_items},
                    {item.key for item in items})

    def test_audio_phones_are_scoped_and_chronological(self):
        self.audio._phrases = list(reversed(self.audio.phrases))
        phones = self.audio.phones_for_speaker(self.speaker)
        self.assertEqual([phone.label for phone in phones],
            ['early1', 'early2', 'late1', 'late2'])
        for phone in phones:
            self.assertEqual(phone.audio_id, self.audio.identifier)
            self.assertEqual(phone.speaker_id, self.speaker.identifier)
        other = self.audio.phones_for_speaker(self.other_speaker)
        self.assertEqual([phone.label for phone in other],
            ['overlap1', 'overlap2'])

    def test_audio_without_matching_speaker_returns_empty_list(self):
        phones = self.other_audio.phones_for_speaker(self.other_speaker)
        self.assertEqual(phones, [])

    def test_audio_without_phrases_returns_empty_list(self):
        audio = self.store.create(Audio, filename='empty.wav', duration=0,
            save=True)
        self.assertEqual(audio.phones_for_speaker(self.speaker), [])


if __name__ == '__main__':
    unittest.main()
