'''Real-store tests for resyllabification (issues #1 and #2).

#1  apply via update_database=True actually persists, exercising both write
    paths: a syllable whose start is unchanged (plain overwrite save) and one
    whose start moves (delete-old-key + save-new via store.update).
#2  after persisting, the two membership views agree: the time-scan
    (syllable.phones) and the stored parent pointer (phone.parent) put every
    phone -- including the one moved across the boundary -- in the same
    syllable.

The test word is "april": ɑ p r ɪ l, given (wrong) as  ɑp | rɪl, which the
Maximal Onset Principle re-segments to  ɑ | p r ɪ l (the p moves to the onset
of the second syllable).
'''
import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from phraser import Store
from phraser import textgrid_loader as tgl
from phraser.models import Audio, Phone, Syllable, Word
from phraser.resyllabifier import resyllabify_word


# given segmentation: syllable [start, end] -> phones it should hold by time
PHONE_SPEC = [('ɑ', 0, 100), ('p', 100, 200), ('r', 200, 300),
    ('ɪ', 300, 400), ('l', 400, 500)]
GIVEN_SYLLABLES = [(0, 200), (200, 500)]   # ɑp | rɪl


def build_word(store):
    '''Build and persist a store-backed "april" word with the given (wrong)
    syllable boundaries; mirrors the textgrid loader's wiring.'''
    audio = Audio(filename='april.wav', store=store, save=False)
    word = Word(label='april', start=0, end=500, store=store, save=False)
    syllables = [Syllable(label='syl', start=s, end=e, store=store, save=False)
        for s, e in GIVEN_SYLLABLES]
    phones = [Phone(label=l, start=s, end=e, store=store, save=False)
        for l, s, e in PHONE_SPEC]
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
            # given boundaries differ from maximal onset -> rewrite + persist
            self.changed = resyllabify_word(word, update_database=True)
            # drop every cache and re-read from disk for a true persistence view
            self.store.close()
            self.store.open()
            self.word = self.store.load(self.word_key)
            self.syllables = sorted(self.word.syllables, key=lambda s: s.start)

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    # ---- #1: persistence and both write paths ----
    def test_reports_rewrite(self):
        self.assertTrue(self.changed)

    def test_persisted_boundaries(self):
        # exactly two syllables survive (the stale start=200 key was deleted by
        # the store.update path), with the maximal-onset boundaries on disk
        self.assertEqual(len(self.syllables), 2)
        self.assertEqual([s.start for s in self.syllables], [0, 100])
        self.assertEqual([s.end for s in self.syllables], [100, 500])

    def test_persisted_membership(self):
        labels = [[p.label for p in s.phones] for s in self.syllables]
        self.assertEqual(labels, [['ɑ'], ['p', 'r', 'ɪ', 'l']])

    def test_save_path_syllable_unchanged_start(self):
        # syl0 kept start=0 -> written via the plain overwrite 'save' path
        self.assertEqual(self.syllables[0].start, 0)
        self.assertEqual([p.label for p in self.syllables[0].phones], ['ɑ'])

    def test_update_path_syllable_moved_start(self):
        # syl1 moved start 200 -> 100 -> written via the 'update' (re-key) path
        self.assertEqual(self.syllables[1].start, 100)

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


if __name__ == '__main__':
    unittest.main()
