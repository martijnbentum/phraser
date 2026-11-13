'''Select words, syllables and phones by same-level neighbours.'''

from contextlib import redirect_stdout
import io
import tempfile
import unittest

import phraser
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word
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


class TestSubwordNeighbours(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        with redirect_stdout(io.StringIO()):
            self.store = phraser.Store(path=self.directory.name)
        self.addCleanup(self.store.close)
        self.audio = self.store.create(Audio, filename='/data/a.wav',
            duration=2000, save=True)
        a = self.store.create(Speaker, name='a', save=True)
        b = self.store.create(Speaker, name='b', save=True)
        a.add_audio(self.audio)
        b.add_audio(self.audio)
        # each a-word has one syllable of two phones; b0 sits in the gap
        # between the first two a-phrases and b1 overlaps phone p9
        phrases = [self.make_phrase(a, [0, 200, 400])]
        phrases.append(self.make_phrase(a, [700], 3))
        phrases.append(self.make_phrase(a, [1000], 4))
        phrases.append(self.make_word_phrase(b, 'b0', 620, 680))
        phrases.append(self.make_word_phrase(b, 'b1', 1150, 1250))
        self.store.save_phrase_trees(phrases)
        self.syllables = {s.label: s for s in self.audio.syllables}
        self.phones = {phone.label: phone for phone in self.audio.phones}
        self.phrase = self.syllables['s0'].phrase

    def identity(self, speaker):
        return {'audio_id': self.audio.identifier,
            'speaker_id': speaker.identifier, 'store': self.store}

    def make_phrase(self, speaker, starts, index=0):
        '''Build a phrase of 200 ms words, one syllable of two phones each.'''
        identity = self.identity(speaker)
        words = []
        for start in starts:
            word = Word(f'w{index}', start, start + 200, **identity)
            syllable = Syllable(f's{index}', start, start + 200, **identity)
            for offset in (0, 100):
                label = f'p{2 * index + offset // 100}'
                syllable.add_child(Phone(label, start + offset,
                    start + offset + 100, **identity))
            word.add_child(syllable)
            words.append(word)
            index += 1
        phrase = Phrase('p', words[0].start, words[-1].end, **identity)
        phrase.add_children(words)
        return phrase

    def make_word_phrase(self, speaker, label, start, end):
        identity = self.identity(speaker)
        phrase = Phrase('p', start, end, **identity)
        phrase.add_child(Word(label, start, end, **identity))
        return phrase

    def labels(self, segments):
        return [segment.label for segment in segments]

    def test_phone_neighbours_cross_words(self):
        preceding, next_phones = neighbours.get_neighbours(self.phones['p1'],
            within_phrase=True)
        self.assertEqual(self.labels(preceding), ['p0'])
        self.assertEqual(self.labels(next_phones), ['p2'])
        phones = neighbours.select_phones_with_neighbours_in_phrase(
            self.phrase)
        self.assertEqual(self.labels(phones), ['p1', 'p2', 'p3', 'p4'])

    def test_other_speaker_word_breaks_phones(self):
        _, next_phones = neighbours.get_neighbours(self.phones['p5'])
        self.assertEqual(next_phones, [])
        _, next_phones = neighbours.get_neighbours(self.phones['p7'])
        self.assertEqual(self.labels(next_phones), ['p8'])

    def test_phone_audio_selection(self):
        select = neighbours.select_phones_with_neighbours_in_audio
        phones = select(self.audio, exclude_overlap=False)
        expected = ['p1', 'p2', 'p3', 'p4', 'p7', 'p8']
        self.assertEqual(self.labels(phones), expected)
        phones = select(self.audio)
        self.assertEqual(self.labels(phones), expected[:-1])

    def test_syllable_selection(self):
        select = neighbours.select_syllables_with_neighbours_in_audio
        self.assertEqual(self.labels(select(self.audio)), ['s1'])
        preceding, next_syllables = neighbours.get_neighbours(
            self.syllables['s3'])
        self.assertEqual(preceding, [])
        self.assertEqual(self.labels(next_syllables), ['s4'])
        _, next_syllables = neighbours.get_neighbours(self.syllables['s3'],
            within_phrase=True)
        self.assertEqual(next_syllables, [])
        syllables = neighbours.select_syllables_with_neighbours_in_phrase(
            self.phrase, return_context=True)
        self.assertEqual(len(syllables), 1)
        self.assertEqual(syllables[0]['target'].label, 's1')

    def test_phrase_is_not_a_level(self):
        with self.assertRaisesRegex(ValueError, 'Word, Syllable or Phone'):
            neighbours.get_neighbours(self.phrase)


if __name__ == '__main__':
    unittest.main()
