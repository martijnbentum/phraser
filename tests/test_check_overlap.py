import importlib.util
import sys
import types
import unittest
from pathlib import Path

MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'check_overlap.py')

_fake_utils = types.ModuleType('phraser.utils')
_fake_utils.overlap_dict = {False: 0, True: 1, None: 9}
_fake_utils.overlap = lambda a, b: a.start < b.end and b.start < a.end

_fake_phraser = types.ModuleType('phraser')
_fake_phraser.utils = _fake_utils
sys.modules.setdefault('phraser', _fake_phraser)
sys.modules['phraser.utils'] = _fake_utils


def load_module():
    spec = importlib.util.spec_from_file_location('phraser.check_overlap',
        MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    module.__package__ = 'phraser'
    sys.modules['phraser.check_overlap'] = module
    spec.loader.exec_module(module)
    return module


NO_OVERLAP = 0
OVERLAP = 1


class FakeCache:
    def __init__(self):
        self.save_many_calls = []

    def save_many(self, objs, overwrite=False):
        self.save_many_calls.append((list(objs), overwrite))


class FakePhrase:
    _cache = None

    def __init__(self, start, end, speaker_id, words=None):
        self.start = start
        self.end = end
        self.speaker_id = speaker_id
        self.words = words or []
        self.overlap_code = 9

    @property
    def all_objects(self):
        objs = [self]
        syllables, phones = [], []
        for word in self.words:
            objs.append(word)
            for syllable in word.syllables:
                syllables.append(syllable)
                phones.extend(syllable.phones)
        return objs + syllables + phones

    @classmethod
    def get_default_cache(cls):
        return cls._cache


def make_phone(start, end):
    return types.SimpleNamespace(start=start, end=end, overlap_code=9)


def make_syllable(start, end, phones=None):
    return types.SimpleNamespace(start=start, end=end, overlap_code=9,
                                 phones=phones or [])


def make_word(start, end, syllables=None):
    return types.SimpleNamespace(start=start, end=end, overlap_code=9,
                                 syllables=syllables or [])


def make_audio(phrases, n_speakers=1):
    speakers = [object() for _ in range(n_speakers)]
    return types.SimpleNamespace(phrases=phrases, speakers=speakers)


m = load_module()


class TestCheckOverlap(unittest.TestCase):
    def setUp(self):
        FakePhrase._cache = FakeCache()

    def test_empty_audio_no_save(self):
        audio = make_audio(phrases=[], n_speakers=1)
        m.check_overlap_audio(audio)
        self.assertEqual(FakePhrase._cache.save_many_calls, [])

    def test_single_speaker_all_no_overlap(self):
        phone = make_phone(0, 100)
        syllable = make_syllable(0, 100, phones=[phone])
        word = make_word(0, 100, syllables=[syllable])
        phrase = FakePhrase(0, 100, speaker_id='spk1', words=[word])
        m.check_overlap_audio(make_audio([phrase], n_speakers=1))
        self.assertEqual(phrase.overlap_code, NO_OVERLAP)
        self.assertEqual(word.overlap_code, NO_OVERLAP)
        self.assertEqual(syllable.overlap_code, NO_OVERLAP)
        self.assertEqual(phone.overlap_code, NO_OVERLAP)

    def test_multi_speaker_no_phrase_overlap_all_no_overlap(self):
        phrase_a = FakePhrase(0, 100, speaker_id='spk1')
        phrase_b = FakePhrase(200, 300, speaker_id='spk2')
        m.check_overlap_audio(make_audio([phrase_a, phrase_b], n_speakers=2))
        self.assertEqual(phrase_a.overlap_code, NO_OVERLAP)
        self.assertEqual(phrase_b.overlap_code, NO_OVERLAP)

    def test_save_many_called_exactly_once(self):
        phrase = FakePhrase(0, 100, speaker_id='spk1')
        m.check_overlap_audio(make_audio([phrase], n_speakers=1))
        self.assertEqual(len(FakePhrase._cache.save_many_calls), 1)

    def test_save_many_receives_all_items(self):
        phone = make_phone(0, 100)
        syllable = make_syllable(0, 100, phones=[phone])
        word = make_word(0, 100, syllables=[syllable])
        phrase = FakePhrase(0, 100, speaker_id='spk1', words=[word])
        m.check_overlap_audio(make_audio([phrase], n_speakers=1))
        saved = FakePhrase._cache.save_many_calls[0][0]
        self.assertIn(phrase, saved)
        self.assertIn(word, saved)
        self.assertIn(syllable, saved)
        self.assertIn(phone, saved)


class TestSetPhraseOverlapCode(unittest.TestCase):
    def test_same_speaker_not_counted(self):
        phrase_a = FakePhrase(0, 100, speaker_id='spk1')
        phrase_b = FakePhrase(50, 150, speaker_id='spk1')
        m._set_phrase_overlap_code(phrase_a, [phrase_a, phrase_b])
        self.assertEqual(phrase_a.overlap_code, NO_OVERLAP)

    def test_other_speaker_no_time_overlap(self):
        phrase_a = FakePhrase(0, 100, speaker_id='spk1')
        phrase_b = FakePhrase(200, 300, speaker_id='spk2')
        m._set_phrase_overlap_code(phrase_a, [phrase_a, phrase_b])
        self.assertEqual(phrase_a.overlap_code, NO_OVERLAP)

    def test_other_speaker_time_overlap(self):
        phrase_a = FakePhrase(0, 100, speaker_id='spk1')
        phrase_b = FakePhrase(50, 150, speaker_id='spk2')
        m._set_phrase_overlap_code(phrase_a, [phrase_a, phrase_b])
        self.assertEqual(phrase_a.overlap_code, OVERLAP)


class TestDescent(unittest.TestCase):
    def test_phrase_overlaps_word_outside_other_phrase(self):
        phone = make_phone(150, 200)
        syllable = make_syllable(150, 200, phones=[phone])
        word = make_word(150, 200, syllables=[syllable])
        phrase_a = FakePhrase(0, 300, speaker_id='spk1', words=[word])
        phrase_b = FakePhrase(0, 100, speaker_id='spk2')
        m._set_phrase_overlap_code(phrase_a, [phrase_a, phrase_b])
        self.assertEqual(phrase_a.overlap_code, OVERLAP)
        self.assertEqual(word.overlap_code, NO_OVERLAP)
        self.assertEqual(syllable.overlap_code, NO_OVERLAP)
        self.assertEqual(phone.overlap_code, NO_OVERLAP)

    def test_full_overlap_chain(self):
        phone = make_phone(50, 80)
        syllable = make_syllable(50, 80, phones=[phone])
        word = make_word(50, 80, syllables=[syllable])
        phrase_a = FakePhrase(0, 100, speaker_id='spk1', words=[word])
        phrase_b = FakePhrase(0, 100, speaker_id='spk2')
        m._set_phrase_overlap_code(phrase_a, [phrase_a, phrase_b])
        self.assertEqual(phrase_a.overlap_code, OVERLAP)
        self.assertEqual(word.overlap_code, OVERLAP)
        self.assertEqual(syllable.overlap_code, OVERLAP)
        self.assertEqual(phone.overlap_code, OVERLAP)


class TestEmptyChildren(unittest.TestCase):
    def test_phrase_no_words(self):
        phrase_a = FakePhrase(0, 100, speaker_id='spk1', words=[])
        phrase_b = FakePhrase(50, 150, speaker_id='spk2')
        m._set_phrase_overlap_code(phrase_a, [phrase_a, phrase_b])
        self.assertEqual(phrase_a.overlap_code, OVERLAP)

    def test_word_no_syllables(self):
        word = make_word(50, 80, syllables=[])
        phrase_b = FakePhrase(0, 100, speaker_id='spk2')
        m._set_word_overlap_code(word, [phrase_b])
        self.assertEqual(word.overlap_code, OVERLAP)

    def test_syllable_no_phones(self):
        syllable = make_syllable(50, 80, phones=[])
        phrase_b = FakePhrase(0, 100, speaker_id='spk2')
        m._set_syllable_overlap_code(syllable, [phrase_b])
        self.assertEqual(syllable.overlap_code, OVERLAP)


if __name__ == '__main__':
    unittest.main()
