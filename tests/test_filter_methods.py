import io
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word


class TestFilterMethods(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self.directory.name)
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='a.wav',
            duration=2000, save=True)
        self.first = self.store.create(Speaker, name='first',
            dataset='test', save=True)
        self.second = self.store.create(Speaker, name='second',
            dataset='test', save=True)
        self.first.add_audio(self.audio)
        self.second.add_audio(self.audio)
        self.de = self.make_tree(self.first, 'de', 0)
        self.het = self.make_tree(self.second, 'het', 1000)
        self.store.save_phrase_trees([self.de, self.het])

    def make_tree(self, speaker, label, start):
        '''Build one phrase > word > syllable > two phones tree.'''
        identity = {'audio_id': self.audio.identifier,
            'speaker_id': speaker.identifier, 'store': self.store}
        phrase = Phrase(label, start, start + 200, **identity)
        word = Word(label, start, start + 200, **identity)
        syllable = Syllable(label, start, start + 200, **identity)
        first = Phone(label[0], start, start + 100, **identity)
        second = Phone(label[1:], start + 100, start + 200, **identity)
        syllable.add_children([first, second])
        word.add_children([syllable])
        phrase.add_children([word])
        return phrase

    def test_audio_filter_speakers(self):
        speakers = self.audio.filter_speakers(name='first')
        self.assertEqual(speakers, [self.first])

    def test_audio_filter_words_with_in_lookup(self):
        words = self.audio.filter_words(label__in=['de', 'het'])
        self.assertEqual(sorted(w.label for w in words), ['de', 'het'])

    def test_audio_filter_words_through_relation(self):
        words = self.audio.filter_words(syllables__phones__label='h')
        self.assertEqual([w.label for w in words], ['het'])

    def test_speaker_filter_phrases(self):
        phrases = self.first.filter_phrases(start__gt=-1)
        self.assertEqual([p.label for p in phrases], ['de'])

    def test_no_match_returns_empty_list(self):
        self.assertEqual(self.audio.filter_phones(label='x'), [])

    def test_empty_items_return_empty_list(self):
        word = self.de.words[0]
        self.assertEqual(word.syllables[0].filter_phones(label='d'),
            [word.syllables[0].phones[0]])
        empty = Syllable('empty', 0, 10, self.audio.identifier,
            self.first.identifier)
        self.assertEqual(empty.filter_phones(label='d'), [])

    def test_staged_tree_filters_without_store(self):
        identity = {'audio_id': self.audio.identifier,
            'speaker_id': self.first.identifier}
        phrase = Phrase('staged', 5000, 5200, **identity)
        word = Word('staged', 5000, 5200, **identity)
        phrase.add_children([word])
        self.assertEqual(phrase.filter_words(label='staged'), [word])


if __name__ == '__main__':
    unittest.main()
