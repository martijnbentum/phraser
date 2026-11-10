'''Real-store tests for the derive family (phraser.phonology.syllabify).

The derive functions copy the donor objects and build an alternative
segmentation on the copies, leaving the donor store and its objects alone:

  derive_syllables_for_word              new syllables (whole phrase copied)
  derive_words_and_syllables_for_phrase  new words+syllables, connected speech
  derive_phrases_for_phones              new phrases+words+syllables, phrases
                                         re-derived from pauses

With store=None the copies stay in memory; with store=target they are saved
there. Fixtures are built the way textgrid_loader wires a transcription
(phrase -> words -> syllables -> phones, with audio/speaker) and persisted in
the donor store.
'''
import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store
from phraser import textgrid_loader as tgl
from phraser.errors import UnboundStoreError
from phraser.models import Audio, Phone, Phrase, Speaker, Syllable, Word
from phraser.phonology.syllabify import (derive_phrases_for_phones,
    derive_syllables_for_word, derive_words_and_syllables_for_phrase,
    repair_word_syllables)


def build_phrase(store, phones, words, syls, filename='x.TextGrid'):
    '''Build and persist one phrase from (label, start, end) specs, mirroring the
    textgrid loader's wiring. Returns (phrase, words, syllables, phones).'''
    audio = Audio(filename='x.wav', duration=5000, store=store, save=True)
    speaker = Speaker(name='s1', dataset='test', store=store, save=True)
    speaker.add_audio(audio)
    identity = {'audio_id': audio.identifier, 'speaker_id': speaker.identifier}
    phrase = Phrase(label=' '.join(l for l, _, _ in words), filename=filename,
        start=phones[0][1], end=phones[-1][2], store=store, save=False,
        **identity)
    word_objs = [Word(label=l, start=s, end=e, store=store, save=False,
        **identity) for l, s, e in words]
    syl_objs = [Syllable(label='s', start=s, end=e, store=store, save=False,
        **identity) for s, e in syls]
    phone_objs = [Phone(label=l, start=s, end=e, store=store, save=False,
        **identity) for l, s, e in phones]
    for syl in syl_objs:
        tgl.find_and_add_phones_to_syllable(syl, phone_objs, save_to_db=False)
        syl._set_phrase_refs(phrase.identifier, phrase.start)
    for word in word_objs:
        tgl.find_and_add_syllables_to_word(word, syl_objs, save_to_db=False)
        word.add_parent(phrase)
    for phone in phone_objs:
        phone._set_phrase_refs(phrase.identifier, phrase.start)
    items = [phrase] + word_objs + syl_objs + phone_objs
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
APRIL_SYLS = [(0, 200), (200, 500)]

# --- het ei: h ɛ t ɛi ; connected speech moves /t/ into 'ei' ------------------
HETEI_PHONES = [('h', 0, 80), ('ɛ', 80, 180), ('t', 180, 260),
    ('ɛi', 260, 500)]
HETEI_WORDS = [('het', 0, 260), ('ei', 260, 500)]
HETEI_SYLS = [(0, 260), (260, 500)]

# --- man <700ms pause> zee : one phrase splits into two -----------------------
MANZEE_PHONES = [('m', 0, 100), ('ɑ', 100, 200), ('n', 200, 300),
    ('z', 1000, 1100), ('eː', 1100, 1300)]
MANZEE_WORDS = [('man', 0, 300), ('zee', 1000, 1300)]
MANZEE_SYLS = [(0, 300), (1000, 1300)]


class StoreTestCase(unittest.TestCase):
    '''A donor store per test, plus target stores on request.'''

    def setUp(self):
        self.store, self._tmpdir = fresh_store()
        self._targets = []

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
            for store, _ in self._targets: store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)
        for _, tmpdir in self._targets:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def build(self, phones, words, syls):
        with redirect_stdout(io.StringIO()):
            return build_phrase(self.store, phones, words, syls)

    def target_store(self):
        store, tmpdir = fresh_store()
        self._targets.append((store, tmpdir))
        return store


class TestDeriveSyllablesForWord(StoreTestCase):
    def setUp(self):
        super().setUp()
        self.phrase, words, self.syls, self.phones = self.build(
            APRIL_PHONES, [('april', 0, 500)], APRIL_SYLS)
        self.word = words[0]
        for i, syllable in enumerate(self.syls):
            syllable.stress_code = 10 + i
        self.derived = derive_syllables_for_word(self.word)

    def test_new_syllables(self):
        self.assertEqual(labels_per_syllable(self.derived),
            [['ɑ'], ['p', 'r', 'ɪ', 'l']])
        self.assertTrue(views_agree(self.derived))

    def test_returns_copy_in_copied_phrase(self):
        self.assertIsNot(self.derived, self.word)
        self.assertIsNot(self.derived.phrase, self.phrase)
        self.assertEqual(self.derived.identifier, self.word.identifier)
        self.assertEqual(self.derived.phrase.identifier,
            self.phrase.identifier)

    def test_one_to_one_syllables_keep_ids_and_stress(self):
        syllables = sorted(self.derived.syllables, key=lambda s: s.start)
        self.assertEqual([s.identifier for s in syllables],
            [s.identifier for s in self.syls])
        self.assertEqual([s.stress_code for s in syllables], [10, 11])

    def test_phones_keep_ids(self):
        ids = [p.identifier for p in self.derived.phones]
        self.assertEqual(sorted(ids), sorted(p.identifier for p in self.phones))

    def test_donor_untouched(self):
        self.assertEqual(labels_per_syllable(self.word),
            [['ɑ', 'p'], ['r', 'ɪ', 'l']])
        self.assertEqual([p.parent_id for p in self.phones],
            [self.syls[0].identifier] * 2 + [self.syls[1].identifier] * 3)

    def test_unsyllabifiable_raises_with_reason(self):
        _, words, _, _ = self.build([('s', 0, 100), ('t', 100, 200)],
            [('st', 0, 200)], [(0, 200)])
        with self.assertRaises(ValueError) as caught:
            derive_syllables_for_word(words[0])
        message = str(caught.exception)
        self.assertIn("word 'st' (0-200 ms)", message)
        self.assertIn('no vowel nucleus', message)


class TestDerivedIds(StoreTestCase):
    '''Ids without a one-to-one donor counterpart are hashed from phones, so
    re-runs agree.'''

    def test_split_syllable_gets_new_deterministic_ids(self):
        # one donor syllable ɑprɪl -> ɑ | prɪl: both nuclei come from it
        _, words, syls, _ = self.build(APRIL_PHONES, [('april', 0, 500)],
            [(0, 500)])
        first = derive_syllables_for_word(words[0])
        second = derive_syllables_for_word(words[0])
        ids = [s.identifier for s in first.syllables]
        self.assertEqual(len(set(ids)), 2)
        self.assertNotIn(syls[0].identifier, ids)
        self.assertEqual(ids, [s.identifier for s in second.syllables])

    def test_unchanged_phrase_keeps_id(self):
        phrase, _, _, phones = self.build(MANZEE_PHONES, MANZEE_WORDS,
            MANZEE_SYLS)
        derived = derive_phrases_for_phones(phones, max_pause=2000)
        self.assertEqual(derived[0].identifier, phrase.identifier)

    def test_split_phrase_gets_new_deterministic_ids(self):
        phrase, _, _, phones = self.build(MANZEE_PHONES, MANZEE_WORDS,
            MANZEE_SYLS)
        first = derive_phrases_for_phones(phones, max_pause=500)
        second = derive_phrases_for_phones(phones, max_pause=500)
        ids = [p.identifier for p in first]
        self.assertNotIn(phrase.identifier, ids)
        self.assertEqual(ids, [p.identifier for p in second])

    def test_word_split_by_pause_gets_two_new_ids(self):
        _, words, _, phones = self.build(
            [('m', 0, 100), ('ɑ', 100, 200), ('m', 900, 1000),
            ('ɑ', 1000, 1100)], [('mama', 0, 1100)], [(0, 200), (900, 1100)])
        derived = derive_phrases_for_phones(phones, max_pause=500)
        ids = [p.words[0].identifier for p in derived]
        self.assertEqual(len(set(ids)), 2)
        self.assertNotIn(words[0].identifier, ids)


class TestDeriveWordsAndSyllablesForPhrase(StoreTestCase):
    def setUp(self):
        super().setUp()
        self.phrase, self.words, _, _ = self.build(HETEI_PHONES, HETEI_WORDS,
            HETEI_SYLS)
        self.derived = derive_words_and_syllables_for_phrase(self.phrase)
        self.by_label = {w.label: w for w in self.derived.words}

    def test_t_migrated_to_ei(self):
        self.assertEqual(labels_per_syllable(self.by_label['het']),
            [['h', 'ɛ']])
        self.assertEqual(labels_per_syllable(self.by_label['ei']),
            [['t', 'ɛi']])

    def test_word_boundaries_follow_content(self):
        het, ei = self.by_label['het'], self.by_label['ei']
        self.assertEqual((het.start, het.end), (0, 180))
        self.assertEqual((ei.start, ei.end), (180, 500))

    def test_phrase_and_words_keep_ids(self):
        self.assertEqual(self.derived.identifier, self.phrase.identifier)
        self.assertEqual([w.identifier for w in self.derived.words],
            [w.identifier for w in self.words])

    def test_chain_and_views_consistent(self):
        for word in self.derived.words:
            self.assertIs(word.parent, self.derived)
            for syllable in word.syllables:
                self.assertIs(syllable.parent, word)
                self.assertEqual(syllable.phrase_id, self.derived.identifier)
            self.assertTrue(views_agree(word))

    def test_donor_untouched(self):
        self.assertEqual(labels_per_syllable(self.words[0]),
            [['h', 'ɛ', 't']])
        self.assertEqual((self.words[0].start, self.words[0].end), (0, 260))

    def test_reduced_word_goes_empty_zero_width(self):
        # 'het' fully reduced to /t/, loses it to 'ei' -> empty zero-width word
        phrase, _, _, _ = self.build([('t', 0, 120), ('ɛi', 120, 500)],
            [('het', 0, 120), ('ei', 120, 500)], [(0, 120), (120, 500)])
        derived = derive_words_and_syllables_for_phrase(phrase)
        het, ei = derived.words
        self.assertEqual(het.syllables, [])
        self.assertEqual((het.start, het.end), (0, 0))
        self.assertEqual(labels_per_syllable(ei), [['t', 'ɛi']])


class TestDerivePhrasesForPhones(StoreTestCase):
    def setUp(self):
        super().setUp()
        self.phrase, self.words, _, self.phones = self.build(MANZEE_PHONES,
            MANZEE_WORDS, MANZEE_SYLS)

    def test_pause_splits_into_two_phrases(self):
        first, second = derive_phrases_for_phones(self.phones, max_pause=500)
        self.assertEqual((first.start, first.end), (0, 300))
        self.assertEqual((second.start, second.end), (1000, 1300))
        self.assertEqual(labels_per_syllable(first), [['m', 'ɑ', 'n']])
        self.assertEqual(labels_per_syllable(second), [['z', 'eː']])

    def test_full_chain_consistent(self):
        for phrase in derive_phrases_for_phones(self.phones, max_pause=500):
            for word in phrase.words:
                self.assertIs(word.parent, phrase)
                self.assertTrue(views_agree(word))

    def test_large_pause_threshold_keeps_one_phrase(self):
        # 700ms gap < 2000: one phrase
        derived = derive_phrases_for_phones(self.phones, max_pause=2000)
        self.assertEqual(len(derived), 1)
        self.assertEqual(len(derived[0].words), 2)

    def test_accepts_segment_with_phones(self):
        derived = derive_phrases_for_phones(self.phrase, max_pause=500)
        self.assertEqual(len(derived), 2)

    def test_donor_untouched(self):
        derive_phrases_for_phones(self.phones, max_pause=500)
        self.assertEqual([p.phrase_id for p in self.phones],
            [self.phrase.identifier] * 5)
        self.assertEqual(len(self.phrase.words), 2)

    def test_multiple_speakers_raise(self):
        self.phones[0].speaker_id = b'\x01' * 8
        with self.assertRaises(ValueError):
            derive_phrases_for_phones(self.phones)

    def test_multiple_audio_raise(self):
        self.phones[0].audio_id = b'\x02' * 8
        with self.assertRaises(ValueError):
            derive_phrases_for_phones(self.phones)

    def test_empty_input_returns_no_phrases(self):
        self.assertEqual(derive_phrases_for_phones([]), [])

    def test_unsyllabifiable_run_raises_with_time_range(self):
        phones = self.build([('ɑ', 2000, 2100), ('p', 2100, 2200),
            ('s', 3200, 3300), ('t', 3300, 3400)],
            [('ap', 2000, 2200), ('st', 3200, 3400)],
            [(2000, 2200), (3200, 3400)])[3]
        with self.assertRaises(ValueError) as caught:
            derive_phrases_for_phones(phones, max_pause=500)
        self.assertIn('phones 3200-3400 ms', str(caught.exception))


class TestDeriveToTargetStore(StoreTestCase):
    def setUp(self):
        super().setUp()
        self.phrase, _, _, self.phones = self.build(MANZEE_PHONES,
            MANZEE_WORDS, MANZEE_SYLS)
        self.donor_rows = len(self.store.DB.all_keys())

    def test_donor_store_refused(self):
        with self.assertRaises(ValueError):
            derive_phrases_for_phones(self.phones, store=self.store)
        self.assertEqual(len(self.store.DB.all_keys()), self.donor_rows)

    def test_saved_and_reloaded_from_target(self):
        target = self.target_store()
        with redirect_stdout(io.StringIO()):
            derived = derive_phrases_for_phones(self.phones, max_pause=500,
                store=target)
            keys = [p.key for p in derived]
            target.close()
            target.open()
            loaded = [target.load(key) for key in keys]
        self.assertEqual([labels_per_syllable(p) for p in loaded],
            [[['m', 'ɑ', 'n']], [['z', 'eː']]])
        self.assertTrue(all(views_agree(p) for p in loaded))
        self.assertEqual(loaded[0].audio.filename, 'x.wav')
        self.assertEqual(loaded[0].speaker.name, 's1')
        self.assertEqual(len(target.DB.all_links()), 1)

    def test_rerun_overwrites(self):
        target = self.target_store()
        with redirect_stdout(io.StringIO()):
            derive_phrases_for_phones(self.phones, max_pause=500, store=target)
            rows = len(target.DB.all_keys())
            derive_phrases_for_phones(self.phones, max_pause=500, store=target)
        self.assertEqual(len(target.DB.all_keys()), rows)

    def test_donor_store_unchanged(self):
        target = self.target_store()
        with redirect_stdout(io.StringIO()):
            derive_words_and_syllables_for_phrase(self.phrase, store=target)
        self.assertEqual(len(self.store.DB.all_keys()), self.donor_rows)

    def test_in_memory_result_cannot_be_saved(self):
        derived = derive_words_and_syllables_for_phrase(self.phrase)
        self.assertEqual(derived.audio.filename, 'x.wav')
        with self.assertRaises(UnboundStoreError):
            derived.save()


class TestLanguage(StoreTestCase):
    def test_other_languages_not_implemented(self):
        phrase, words, _, phones = self.build(APRIL_PHONES,
            [('april', 0, 500)], APRIL_SYLS)
        calls = [lambda: derive_syllables_for_word(words[0], language='en'),
            lambda: derive_words_and_syllables_for_phrase(phrase,
                language='en'),
            lambda: derive_phrases_for_phones(phones, language='en'),
            lambda: repair_word_syllables(words[0], language='en')]
        for call in calls:
            with self.assertRaises(NotImplementedError):
                call()


if __name__ == '__main__':
    unittest.main()
