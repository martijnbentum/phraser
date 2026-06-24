'''Real-store tests for resyllabification.

The rewrite builds a fresh-id COPY of each old syllable on its new phone group
and deletes the old rows, rather than retiming syllables in place. So the things
under test are:

#1  apply via update_database=True persists the new boundaries and leaves
    exactly the new syllables on disk -- no orphan row survives (the original
    leftover bug, where a moved syllable's old key was never deleted).
#2  after persisting, the two membership views agree: the time-scan
    (syllable.phones) and the stored parent pointer (phone.parent) put every
    phone -- including the one moved across the boundary -- in the same syllable.
#3  with update_database=False the same two views already agree in memory, with
    nothing written to disk: the navigation caches (_children / _parent) are
    pointed at the new grouping.
#4  each new syllable is relabelled from its phones, so .label stays in sync; on
    a persisted run the label index is swapped (old label gone, new one resolves).
#5  resyllabify_word returns a ResyllabifyOutcome describing what it saw and did
    (applied / ok / count_mismatch) across correct, rewritten, unanalysable and
    count-mismatch words.
#6  apply_new_syllable_boundaries guards its invariants for direct callers: it
    raises on a syllable-count mismatch and on a multi-speaker word.
#7  because new syllables are copies, per-syllable metadata (stress_code/tone)
    carries over unchanged.

The main test word is "april": ɑ p r ɪ l, given (wrong) as  ɑp | rɪl, which the
Maximal Onset Principle re-segments to  ɑ | p r ɪ l (the p moves to the onset of
the second syllable). Both given syllables start with the placeholder label
"syl", so a correct relabel makes them "ɑ" and "p r ɪ l".
'''
import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store
from phraser import textgrid_loader as tgl
from phraser.models import Audio, Phone, Syllable, Word
from phraser.resyllabifier import (apply_new_syllable_boundaries,
    resyllabify_word)


# given segmentation: syllable [start, end] -> phones it should hold by time
PHONE_SPEC = [('ɑ', 0, 100), ('p', 100, 200), ('r', 200, 300),
    ('ɪ', 300, 400), ('l', 400, 500)]
GIVEN_SYLLABLES = [(0, 200), (200, 500)]            # ɑp | rɪl  (wrong)
CORRECT_SYLLABLES = [(0, 100), (100, 500)]          # ɑ | prɪl  (maximal onset)

# a p i l stored as three syllables ɑ | p | ɪl: the middle one has no nucleus,
# so the maximal-onset suggestion has two syllables -> a count mismatch.
MISMATCH_PHONES = [('ɑ', 0, 100), ('p', 100, 200), ('ɪ', 200, 300),
    ('l', 300, 400)]
MISMATCH_SYLLABLES = [(0, 100), (100, 200), (200, 400)]

# an unknown phone symbol makes the word unanalysable (analyse_word raises
# internally and returns an uncheckable Result).
UNKNOWN_PHONES = [('ɑ', 0, 100), ('XYZ', 100, 200)]
UNKNOWN_SYLLABLES = [(0, 200)]


def build_word(store, phone_spec=PHONE_SPEC, syllable_spec=GIVEN_SYLLABLES,
        word_label='april'):
    '''Build and persist a store-backed word with the given syllable
    boundaries; mirrors the textgrid loader's wiring.'''
    end = max(e for _, _, e in phone_spec)
    audio = Audio(filename='april.wav', store=store, save=False)
    word = Word(label=word_label, start=0, end=end, store=store, save=False)
    syllables = [Syllable(label='syl', start=s, end=e, store=store, save=False)
        for s, e in syllable_spec]
    phones = [Phone(label=l, start=s, end=e, store=store, save=False)
        for l, s, e in phone_spec]
    for syllable in syllables:
        tgl.find_and_add_phones_to_syllable(syllable, phones, save_to_db=False)
    tgl.find_and_add_syllables_to_word(word, syllables, save_to_db=False)
    items = [word] + syllables + phones
    for item in items:
        item.add_audio(audio, update_database=False, propagate=False)
    # the Audio row itself is not needed: segments only carry its identifier as
    # audio_id, and parent lookups resolve via syllable keys, not the Audio.
    store.save_many(items)
    return word


class TestResyllabifyPersistence(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self._tmpdir)
            word = build_word(self.store)
            self.word_key = word.key
            # ids of the originals, captured before the rewrite replaces them
            self.old_syllable_ids = {s.identifier for s in word.syllables}
            # given boundaries differ from maximal onset -> rewrite + persist
            self.outcome = resyllabify_word(word, update_database=True)
            # drop every cache and re-read from disk for a true persistence view
            self.store.close()
            self.store.open()
            self.word = self.store.load(self.word_key)
            self.syllables = sorted(self.word.syllables, key=lambda s: s.start)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    # ---- #5: the outcome reports an applied rewrite ----
    def test_reports_rewrite(self):
        self.assertTrue(self.outcome.applied)
        self.assertTrue(self.outcome.ok)

    # ---- #1: persistence and no orphan rows ----
    def test_persisted_boundaries(self):
        # exactly two syllables survive (every old row was deleted), with the
        # maximal-onset boundaries on disk
        self.assertEqual(len(self.syllables), 2)
        self.assertEqual([s.start for s in self.syllables], [0, 100])
        self.assertEqual([s.end for s in self.syllables], [100, 500])

    def test_new_syllables_have_fresh_ids_no_orphans(self):
        # new syllables are fresh-id copies, and none of the old rows survive:
        # the surviving ids are disjoint from the originals' ids
        new_ids = {s.identifier for s in self.syllables}
        self.assertEqual(new_ids & self.old_syllable_ids, set())

    def test_persisted_membership(self):
        labels = [[p.label for p in s.phones] for s in self.syllables]
        self.assertEqual(labels, [['ɑ'], ['p', 'r', 'ɪ', 'l']])

    # ---- #2: time-scan view and parent-pointer view agree ----
    def test_views_agree_for_every_phone(self):
        for syllable in self.syllables:
            for phone in syllable.phones:            # time-scan membership
                self.assertEqual(phone.parent_id, syllable.identifier)
                self.assertEqual(phone.parent_start, syllable.start)
                # stored pointer resolves to the same syllable
                self.assertEqual(phone.parent.identifier, syllable.identifier)

    def test_moved_phone_landed_in_second_syllable(self):
        # the p that crossed the boundary belongs to syllable 2 under both views
        second = self.syllables[1]
        p = next(ph for ph in second.phones if ph.label == 'p')
        self.assertEqual(p.parent.identifier, second.identifier)

    # ---- #4: labels relabelled and label index swapped, persisted ----
    def test_persisted_labels_match_phones(self):
        # the stale 'syl' placeholders are gone; labels follow the new grouping
        self.assertEqual([s.label for s in self.syllables], ['ɑ', 'p r ɪ l'])

    def test_old_label_no_longer_resolves(self):
        # both old 'syl' label-index entries were removed
        self.assertEqual(self.store.label_to_instances('syl', 'Syllable'), [])

    def test_new_labels_resolve_via_index(self):
        for label in ('ɑ', 'p r ɪ l'):
            found = self.store.label_to_instances(label, 'Syllable')
            self.assertEqual([s.label for s in found], [label])


class TestResyllabifyInMemory(unittest.TestCase):
    '''#3: update_database=False mutates only in memory, yet both membership
    views agree on the new grouping, and disk is left untouched.'''

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self._tmpdir)
            self.word = build_word(self.store)
            self.word_key = self.word.key
            # rewrite boundaries in memory only -- no write, no reopen, so the
            # live object graph (and its caches) is what we assert against
            self.outcome = resyllabify_word(self.word, update_database=False)
            self.syllables = sorted(self.word.syllables, key=lambda s: s.start)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_reports_rewrite(self):
        self.assertTrue(self.outcome.applied)

    def test_boundaries_moved_in_memory(self):
        # maximal-onset boundaries are visible on the live objects
        self.assertEqual(len(self.syllables), 2)
        self.assertEqual([s.start for s in self.syllables], [0, 100])
        self.assertEqual([s.end for s in self.syllables], [100, 500])

    def test_timescan_membership(self):
        # downward view: syllable.phones reflects the new grouping
        labels = [[p.label for p in s.phones] for s in self.syllables]
        self.assertEqual(labels, [['ɑ'], ['p', 'r', 'ɪ', 'l']])

    def test_views_agree_for_every_phone(self):
        # upward view (phone.parent) agrees with the time-scan, with no write:
        # the moved p resolves to its new syllable from the refilled cache
        for syllable in self.syllables:
            for phone in syllable.phones:            # time-scan membership
                self.assertEqual(phone.parent_id, syllable.identifier)
                self.assertEqual(phone.parent_start, syllable.start)
                # stored pointer resolves to the same syllable, from cache
                self.assertIsNotNone(phone.parent)
                self.assertEqual(phone.parent.identifier, syllable.identifier)

    def test_moved_phone_landed_in_second_syllable(self):
        # the p that crossed the boundary belongs to syllable 2 under both views
        second = self.syllables[1]
        p = next(ph for ph in second.phones if ph.label == 'p')
        self.assertEqual(p.parent.identifier, second.identifier)

    def test_labels_relabelled_in_memory(self):
        # the live objects carry labels derived from the new grouping
        self.assertEqual([s.label for s in self.syllables], ['ɑ', 'p r ɪ l'])

    def test_nothing_persisted(self):
        # reopen from disk: the given (wrong) boundaries and placeholder labels
        # survive untouched, proving the in-memory rewrite wrote nothing
        with redirect_stdout(io.StringIO()):
            self.store.close()
            self.store.open()
            word = self.store.load(self.word_key)
            syllables = sorted(word.syllables, key=lambda s: s.start)
        self.assertEqual([s.start for s in syllables], [0, 200])
        self.assertEqual([s.end for s in syllables], [200, 500])
        self.assertEqual([s.label for s in syllables], ['syl', 'syl'])
        labels = [[p.label for p in s.phones] for s in syllables]
        self.assertEqual(labels, [['ɑ', 'p'], ['r', 'ɪ', 'l']])


class TestResyllabifyOutcome(unittest.TestCase):
    '''#5: resyllabify_word returns a ResyllabifyOutcome distinguishing the
    correct / unanalysable / count-mismatch cases that the old bool conflated,
    and leaves those words untouched.'''

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self._tmpdir)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _resyllabify(self, **build_kwargs):
        with redirect_stdout(io.StringIO()):
            word = build_word(self.store, **build_kwargs)
            return word, resyllabify_word(word, update_database=True)

    def test_already_correct_not_applied_but_ok(self):
        word, outcome = self._resyllabify(syllable_spec=CORRECT_SYLLABLES)
        self.assertFalse(outcome.applied)
        self.assertTrue(outcome.ok)
        self.assertFalse(outcome.count_mismatch)
        # untouched: still the two given correct syllables
        self.assertEqual(len(word.syllables), 2)

    def test_unanalysable_not_applied_not_ok(self):
        word, outcome = self._resyllabify(phone_spec=UNKNOWN_PHONES,
            syllable_spec=UNKNOWN_SYLLABLES)
        self.assertFalse(outcome.applied)
        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.result.uncheckable)
        self.assertFalse(outcome.count_mismatch)

    def test_count_mismatch_skipped_not_raised(self):
        word, outcome = self._resyllabify(phone_spec=MISMATCH_PHONES,
            syllable_spec=MISMATCH_SYLLABLES)
        self.assertFalse(outcome.applied)
        self.assertTrue(outcome.count_mismatch)
        # left untouched: the three given syllables survive, none rewritten
        self.assertEqual(len(word.syllables), 3)


class TestApplyGuards(unittest.TestCase):
    '''#6: apply_new_syllable_boundaries protects its 1:1 copy invariant for
    direct callers, independently of resyllabify_word's soft skip.'''

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self._tmpdir)
            self.word = build_word(self.store)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_count_mismatch_raises(self):
        # two syllables, one group -> a count mismatch a direct caller must hear
        with self.assertRaises(ValueError):
            apply_new_syllable_boundaries(self.word, [self.word.phones])

    def test_multi_speaker_raises(self):
        # tamper one phone's speaker so the word is no longer single-speaker
        groups = [s.phones for s in self.word.syllables]
        groups[0][0].speaker_id = b'\x01\x02\x03\x04\x05\x06\x07\x08'
        with self.assertRaises(ValueError):
            apply_new_syllable_boundaries(self.word, groups)


class TestMetadataCarryOver(unittest.TestCase):
    '''#7: new syllables are copies, so per-syllable metadata rides along.'''

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self._tmpdir)
            self.word = build_word(self.store)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_stress_and_tone_preserved_per_syllable(self):
        old = sorted(self.word.syllables, key=lambda s: s.start)
        for i, syllable in enumerate(old):
            syllable.stress_code = 10 + i
            syllable.tone = 20 + i
        resyllabify_word(self.word, update_database=False)
        new = sorted(self.word.syllables, key=lambda s: s.start)
        # new[i] is a copy of old[i] (same nucleus), so its metadata is carried
        self.assertEqual([s.stress_code for s in new], [10, 11])
        self.assertEqual([s.tone for s in new], [20, 21])


if __name__ == '__main__':
    unittest.main()
