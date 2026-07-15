'''Real-store tests for the rebuild family (phraser.syllabify_phones).

These functions ignore the existing structure and build fresh objects from the
phone sequence, in memory only:

  syllabify_word    rebuild one word's syllables (keeps the word)
  syllabify_phrase  rebuild a phrase's words+syllables, connected-speech MOP
  syllabify_phones  rebuild phrases+words+syllables, phrases re-derived from
                    pauses

Fixtures are built the way textgrid_loader wires a transcription (phrase ->
words -> syllables -> phones, with audio/speaker), then persisted so the
in-memory rebuild can read the phones back through the store.
'''
import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store
from phraser import textgrid_loader as tgl
from phraser.models import Audio, Phone, Syllable, Word, Phrase
from phraser.syllabify_phones import (syllabify_word, syllabify_phrase,
    syllabify_phones)


def build_phrase(store, phones, words, syls, filename='x.TextGrid'):
    '''Build and persist one phrase from (label, start, end) specs, mirroring the
    textgrid loader's wiring. Returns (phrase, words, syllables, phones).'''
    audio = Audio(filename='x.wav', store=store, save=False)
    phrase = Phrase(label=' '.join(l for l, _, _ in words), filename=filename,
        start=phones[0][1], end=phones[-1][2], store=store, save=False)
    word_objs = [Word(label=l, start=s, end=e, store=store, save=False)
        for l, s, e in words]
    syl_objs = [Syllable(label='s', start=s, end=e, store=store, save=False)
        for s, e in syls]
    phone_objs = [Phone(label=l, start=s, end=e, store=store, save=False)
        for l, s, e in phones]
    for syl in syl_objs:
        tgl.find_and_add_phones_to_syllable(syl, phone_objs, save_to_db=False)
        syl._set_phrase_refs(phrase.identifier, phrase.start)
    for word in word_objs:
        tgl.find_and_add_syllables_to_word(word, syl_objs, save_to_db=False)
        word.add_parent(phrase)
    for phone in phone_objs:
        phone._set_phrase_refs(phrase.identifier, phrase.start)
    items = [phrase] + word_objs + syl_objs + phone_objs
    for item in items:
        item.add_audio(audio, update_database=False, propagate=False)
    store.save_many(items)
    return phrase, word_objs, syl_objs, phone_objs


def fresh_store():
    tmpdir = tempfile.mkdtemp()
    with redirect_stdout(io.StringIO()):
        store = Store(path=tmpdir)
    return store, tmpdir


def labels_per_syllable(parent):
    return [[p.label for p in s.phones]
        for s in sorted(parent.syllables, key=lambda x: x.start)]


def views_agree(parent):
    '''True if every phone's stored parent pointer matches the time-scan that
    placed it (both membership views agree).'''
    for syllable in parent.syllables:
        for phone in syllable.phones:
            if phone.parent is None: return False
            if phone.parent.identifier != syllable.identifier: return False
            if phone.parent_start != syllable.start: return False
    return True


# --- april, one word, given wrong as ɑp | rɪl -> ɑ | p r ɪ l ------------------
APRIL_PHONES = [('ɑ', 0, 100), ('p', 100, 200), ('r', 200, 300),
    ('ɪ', 300, 400), ('l', 400, 500)]


class TestSyllabifyWord(unittest.TestCase):
    def setUp(self):
        self.store, self._tmpdir = fresh_store()
        with redirect_stdout(io.StringIO()):
            self.phrase, words, _, _ = build_phrase(self.store, APRIL_PHONES,
                [('april', 0, 500)], [(0, 200), (200, 500)])  # ɑp | rɪl
            self.word = words[0]
            self.word_key = self.word.key
            self.new = syllabify_word(self.word)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_rebuilt_count_and_labels(self):
        self.assertEqual(len(self.new), 2)
        self.assertEqual([s.label for s in sorted(self.new, key=lambda x: x.start)],
            ['ɑ', 'p r ɪ l'])

    def test_timescan_membership(self):
        self.assertEqual(labels_per_syllable(self.word),
            [['ɑ'], ['p', 'r', 'ɪ', 'l']])

    def test_word_children_are_new_syllables(self):
        got = [s.identifier for s in sorted(self.word.syllables, key=lambda x: x.start)]
        self.assertEqual(got, [s.identifier for s in sorted(self.new, key=lambda x: x.start)])

    def test_views_agree(self):
        self.assertTrue(views_agree(self.word))

    def test_phrase_and_audio_wired(self):
        for s in self.new:
            self.assertEqual(s.phrase_id, self.phrase.identifier)
            self.assertEqual(s.audio_id, self.word.audio_id)

    def test_nothing_persisted(self):
        with redirect_stdout(io.StringIO()):
            self.store.close(); self.store.open()
            word = self.store.load(self.word_key)
        self.assertEqual(labels_per_syllable(word), [['ɑ', 'p'], ['r', 'ɪ', 'l']])

    def test_unsyllabifiable_returns_none(self):
        with redirect_stdout(io.StringIO()):
            store, tmpdir = fresh_store()
            _, words, _, _ = build_phrase(store, [('s', 0, 100), ('t', 100, 200)],
                [('st', 0, 200)], [(0, 200)])               # no nucleus
            result = syllabify_word(words[0])
            store.close()
        shutil.rmtree(tmpdir, ignore_errors=True)
        self.assertIsNone(result)


# --- het ei: h ɛ t ɛi ; connected speech moves /t/ into 'ei' ------------------
class TestSyllabifyPhrase(unittest.TestCase):
    def setUp(self):
        self.store, self._tmpdir = fresh_store()
        with redirect_stdout(io.StringIO()):
            self.phrase, self.words, _, _ = build_phrase(self.store,
                [('h', 0, 80), ('ɛ', 80, 180), ('t', 180, 260), ('ɛi', 260, 500)],
                [('het', 0, 260), ('ei', 260, 500)], [(0, 260), (260, 500)])
            self.new = syllabify_phrase(self.phrase)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_t_migrated_to_ei(self):
        by_label = {w.label: w for w in self.new}
        self.assertEqual(labels_per_syllable(by_label['het']), [['h', 'ɛ']])
        self.assertEqual(labels_per_syllable(by_label['ei']), [['t', 'ɛi']])

    def test_word_boundaries_follow_content(self):
        by_label = {w.label: w for w in self.new}
        self.assertEqual((by_label['het'].start, by_label['het'].end), (0, 180))
        self.assertEqual((by_label['ei'].start, by_label['ei'].end), (180, 500))

    def test_chain_and_views_consistent(self):
        for word in self.new:
            self.assertEqual(word.parent.identifier, self.phrase.identifier)
            for s in word.syllables:
                self.assertEqual(s.parent.identifier, word.identifier)
                self.assertEqual(s.phrase_id, self.phrase.identifier)
            self.assertTrue(views_agree(word))

    def test_reduced_word_goes_empty_zero_width(self):
        # 'het' fully reduced to /t/, loses it to 'ei' -> empty zero-width word
        with redirect_stdout(io.StringIO()):
            store, tmpdir = fresh_store()
            phrase, _, _, _ = build_phrase(store,
                [('t', 0, 120), ('ɛi', 120, 500)],
                [('het', 0, 120), ('ei', 120, 500)], [(0, 120), (120, 500)])
            new = syllabify_phrase(phrase)
            by_label = {w.label: w for w in new}
            het, ei = by_label['het'], by_label['ei']
            het_empty = len(het.syllables) == 0
            het_span = (het.start, het.end)
            ei_labels = labels_per_syllable(ei)
            store.close()
        shutil.rmtree(tmpdir, ignore_errors=True)
        self.assertTrue(het_empty)
        self.assertEqual(het_span, (0, 0))
        self.assertEqual(ei_labels, [['t', 'ɛi']])


# --- man <700ms pause> zee : one phrase splits into two -----------------------
MANZEE_PHONES = [('m', 0, 100), ('ɑ', 100, 200), ('n', 200, 300),
    ('z', 1000, 1100), ('eː', 1100, 1300)]
MANZEE_WORDS = [('man', 0, 300), ('zee', 1000, 1300)]
MANZEE_SYLS = [(0, 300), (1000, 1300)]


class TestSyllabifyPhones(unittest.TestCase):
    def setUp(self):
        self.store, self._tmpdir = fresh_store()
        with redirect_stdout(io.StringIO()):
            self.phrase, self.words, _, self.phones = build_phrase(self.store,
                MANZEE_PHONES, MANZEE_WORDS, MANZEE_SYLS)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_pause_splits_into_two_phrases(self):
        new = syllabify_phones(self.phones, max_pause=500)
        self.assertEqual(len(new), 2)
        first, second = sorted(new, key=lambda p: p.start)
        self.assertEqual((first.start, first.end), (0, 300))
        self.assertEqual((second.start, second.end), (1000, 1300))
        self.assertEqual(labels_per_syllable(first), [['m', 'ɑ', 'n']])
        self.assertEqual(labels_per_syllable(second), [['z', 'eː']])

    def test_full_chain_consistent(self):
        new = syllabify_phones(self.phones, max_pause=500)
        for phrase in new:
            for word in phrase.words:
                self.assertEqual(word.parent.identifier, phrase.identifier)
                self.assertTrue(views_agree(word))

    def test_large_pause_threshold_keeps_one_phrase(self):
        new = syllabify_phones(self.phones, max_pause=2000)   # 700ms gap < 2000
        self.assertEqual(len(new), 1)
        self.assertEqual(len(new[0].words), 2)

    def test_accepts_segment_with_phones(self):
        new = syllabify_phones(self.phrase, max_pause=500)    # duck-typed input
        self.assertEqual(len(new), 2)

    def test_multiple_speakers_raise(self):
        self.phones[0].speaker_id = b'\x01' * 8
        with self.assertRaises(ValueError):
            syllabify_phones(self.phones)

    def test_multiple_audio_raise(self):
        self.phones[0].audio_id = b'\x02' * 8
        with self.assertRaises(ValueError):
            syllabify_phones(self.phones)

    def test_empty_input_returns_none(self):
        self.assertIsNone(syllabify_phones([]))

    def test_unsyllabifiable_run_returns_none(self):
        with redirect_stdout(io.StringIO()):
            store, tmpdir = fresh_store()
            _, _, _, phones = build_phrase(store,
                [('s', 0, 100), ('t', 100, 200)], [('st', 0, 200)], [(0, 200)])
            result = syllabify_phones(phones)
            store.close()
        shutil.rmtree(tmpdir, ignore_errors=True)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
