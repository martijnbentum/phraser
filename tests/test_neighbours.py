'''Select words by neighbouring words from the same speaker.'''

from contextlib import redirect_stdout
import io
import tempfile
import unittest

import phraser
from phraser.models import Audio, Phrase, Speaker, Word
from phraser.sampling import neighbours


class TestNeighbours(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        with redirect_stdout(io.StringIO()):
            self.store = phraser.Store(path=self.directory.name)
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='/data/a.wav',
            duration=6000, save=True)
        a = self.store.create(Speaker, name='a', save=True)
        b = self.store.create(Speaker, name='b', save=True)
        a.add_audio(self.audio)
        b.add_audio(self.audio)
        # b0 overlaps a3; b1 sits in the gap between a6 and a7
        phrases = [self.make_phrase(a, [('a0', 0), ('a1', 100),
            ('a2', 200), ('a3', 300)])]
        phrases.append(self.make_phrase(a, [('a4', 800), ('a5', 900)]))
        phrases.append(self.make_phrase(a, [('a6', 5000)]))
        phrases.append(self.make_phrase(a, [('a7', 5400)]))
        phrases.append(self.make_phrase(b, [('b0', 320, 390)]))
        phrases.append(self.make_phrase(b, [('b1', 5200, 5300)]))
        self.store.save_phrase_trees(phrases)
        self.words = {word.label: word for word in self.audio.words}
        self.phrase = self.words['a0'].parent

    def make_phrase(self, speaker, labels_starts):
        '''Build a phrase of 100 ms words; (label, start, end) sets the end.'''
        identity = {'audio_id': self.audio.identifier,
            'speaker_id': speaker.identifier, 'store': self.store}
        words = []
        for label, start, *end in labels_starts:
            end = end[0] if end else start + 100
            words.append(Word(label, start, end, **identity))
        phrase = Phrase('p', words[0].start, words[-1].end, **identity)
        phrase.add_children(words)
        return phrase

    def labels(self, words):
        return [word.label for word in words]

    def select_phrase(self, **kwargs):
        return self.labels(neighbours.select_words_with_neighbours_in_phrase(
            self.phrase, **kwargs))

    def test_phrase_selection(self):
        self.assertEqual(self.select_phrase(), ['a1'])
        self.assertEqual(self.select_phrase(exclude_overlap=False),
            ['a1', 'a2'])
        self.assertEqual(self.select_phrase(n_neighbours=2,
            exclude_overlap=False), [])

    def test_neighbours_across_phrases(self):
        preceding, next_words = neighbours.get_neighbours(self.words['a3'],
            2)
        self.assertEqual(self.labels(preceding), ['a1', 'a2'])
        self.assertEqual(self.labels(next_words), ['a4', 'a5'])
        preceding, next_words = neighbours.get_neighbours(self.words['a3'],
            2, within_phrase=True)
        self.assertEqual(self.labels(next_words), [])

    def test_gap_and_other_speaker_break(self):
        get = neighbours.get_neighbours
        _, next_words = get(self.words['a5'])
        self.assertEqual(next_words, [])
        _, next_words = get(self.words['a5'], max_gap_ms=5000)
        self.assertEqual(self.labels(next_words), ['a6'])
        _, next_words = get(self.words['a6'])
        self.assertEqual(next_words, [])
        _, next_words = get(self.words['a5'], max_gap_ms=None)
        self.assertEqual(self.labels(next_words), ['a6'])
        with self.assertRaises(ValueError):
            get(self.words['a5'], max_gap_ms=-1)

    def test_has_neighbours_and_overlap(self):
        a0 = self.words['a0']
        self.assertFalse(neighbours.has_neighbours(a0))
        self.assertTrue(neighbours.has_neighbours(self.words['a1']))
        self.assertFalse(neighbours.has_neighbours(self.words['a6']))
        self.assertFalse(neighbours.has_overlap([a0, self.words['a1']]))
        self.assertTrue(neighbours.has_overlap([self.words['a3']]))

    def test_unsaved_word_raises(self):
        identity = {'audio_id': self.audio.identifier,
            'speaker_id': self.words['a0'].speaker_id, 'store': self.store}
        word = Word('x', 9000, 9100, **identity)
        with self.assertRaisesRegex(ValueError, 'saved phrase'):
            neighbours.get_neighbours(word)

    def test_audio_selection(self):
        select = neighbours.select_words_with_neighbours_in_audio
        words = select(self.audio, exclude_overlap=False)
        self.assertEqual(self.labels(words), ['a1', 'a2', 'a3', 'a4'])
        words = select(self.audio)
        self.assertEqual(self.labels(words), ['a1'])
        context = select(self.audio, within_phrase=True,
            return_context=True)
        self.assertEqual(len(context), 1)
        self.assertEqual(self.labels(context[0]['preceding']), ['a0'])
        self.assertEqual(context[0]['target'].label, 'a1')
        self.assertEqual(self.labels(context[0]['next']), ['a2'])


if __name__ == '__main__':
    unittest.main()
