from contextlib import redirect_stdout
import io
import tempfile
from types import SimpleNamespace
import unittest

from phraser import Audio, Marker, Phone, Phrase, Speaker, Store, Syllable, Word
from phraser import segment_flags, struct_value


class TestFlags(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=directory.name)
        self.addCleanup(self.store.close)
        audio = self.store.create(Audio, filename='flags.wav', duration=1000)
        speaker = self.store.create(Speaker, name='speaker', dataset='test')
        identity = {'audio_id': audio.identifier,
            'speaker_id': speaker.identifier}
        phrase = self.store.create(Phrase, label='a', start=0, end=100,
            **identity)
        word = self.store.create(Word, label='a', start=0, end=100,
            **identity)
        syllable = self.store.create(Syllable, label='a', start=0, end=100,
            **identity)
        phone = self.store.create(Phone, label='a', start=0, end=100,
            **identity)
        marker = self.store.create(Marker, label='event', start=0, end=100,
            **identity)
        word.add_parent(phrase)
        syllable.add_parent(word)
        phone.add_parent(syllable)
        self.phrase = phrase
        self.objects = [audio, speaker, phrase, word, syllable, phone, marker]

    def _assert_persisted_flags(self, objects, expected):
        for obj in objects:
            with self.subTest(object_type=obj.object_type):
                raw = self.store.DB.load(obj.key)
                fields = struct_value.unpack_instance(obj.object_type, raw)
                self.assertEqual(fields['flags'], expected)
                self.store._cache.pop(obj.key, None)
                loaded = self.store.load(obj.key)
                self.assertIsNot(loaded, obj)
                self.assertEqual(loaded.flags, expected)
                if obj.object_type in ('Audio', 'Speaker'): continue
                state = segment_flags.DURATION_OUTLIER_STATES[expected & 3]
                self.assertEqual(loaded.duration_outlier, state)
                self.assertEqual(loaded.is_duration_outlier,
                    state in ('short', 'long'))

    def test_duration_states_preserve_other_bits(self):
        for obj in self.objects[2:]:
            obj.flags = 0b10100100
            for code, state in enumerate(segment_flags.DURATION_OUTLIER_STATES):
                with self.subTest(object_type=obj.object_type, state=state):
                    obj.duration_outlier = state
                    self.assertEqual(obj.duration_outlier_code, code)
                    self.assertEqual(obj.flags, 0b10100100 | code)
                    self.assertEqual(obj.is_duration_outlier,
                        state in ('short', 'long'))

    def test_duration_code_setter_and_unset_default(self):
        phone = self.objects[5]
        self.assertEqual(phone.duration_outlier, 'unset')
        self.assertFalse(phone.is_duration_outlier)
        phone.flags = 0b11111100
        phone.duration_outlier_code = 2
        self.assertEqual(phone.duration_outlier, 'long')
        self.assertEqual(phone.flags, 0b11111110)
        phone.duration_outlier_code = 3
        self.assertEqual(phone.duration_outlier, 'within_range')
        self.assertFalse(phone.is_duration_outlier)

    def test_invalid_duration_assessments_leave_flags_unchanged(self):
        phone = self.objects[5]
        phone.flags = 0b10100110
        for code in (-1, 4, 255, True, 1.0, 'long', None):
            with self.subTest(code=code):
                with self.assertRaises(ValueError):
                    phone.duration_outlier_code = code
                self.assertEqual(phone.flags, 0b10100110)
        for state in ('ok', 'unknown', 1, None):
            with self.subTest(state=state):
                with self.assertRaises(ValueError):
                    phone.duration_outlier = state
                self.assertEqual(phone.flags, 0b10100110)

    def test_duration_assessments_are_independent_of_relatives(self):
        phone = self.objects[5]
        phone.duration_outlier = 'long'
        for obj in self.objects[2:5]:
            self.assertEqual(obj.duration_outlier, 'unset')
        self.phrase.duration_outlier = 'within_range'
        self.assertEqual(phone.duration_outlier, 'long')

    def test_duration_property_survives_save_and_reload(self):
        phone = self.objects[5]
        phone.flags = 0b10100100
        phone.duration_outlier = 'short'
        phone.save()
        self._assert_persisted_flags([phone], 0b10100101)

    def test_defaults_and_objects_without_flags_remain_compatible(self):
        for obj in self.objects:
            with self.subTest(object_type=obj.object_type):
                self.assertEqual(obj.flags, 0)
                raw = struct_value.pack_instance(obj)
                fields = struct_value.unpack_instance(obj.object_type, raw)
                fields.pop('flags')
                fields['object_type'] = obj.object_type
                legacy = SimpleNamespace(**fields)
                raw = struct_value.pack_instance(legacy)
                unpacked = struct_value.unpack_instance(obj.object_type, raw)
                self.assertEqual(unpacked['flags'], 0)

    def test_single_save_and_loaded_object_resave_preserve_all_bits(self):
        for obj in self.objects:
            obj.flags = 0b10100110
            self.store.save(obj)
        self._assert_persisted_flags(self.objects, 0b10100110)
        for obj in self.objects:
            loaded = self.store.load(obj.key)
            self.store.save(loaded, overwrite=True)
        self._assert_persisted_flags(self.objects, 0b10100110)

    def test_batch_save_preserves_flags(self):
        for obj in self.objects:
            obj.flags = 255
        self.store.save_many(self.objects)
        self._assert_persisted_flags(self.objects, 255)

    def test_phrase_tree_save_and_replacement_preserve_flags(self):
        for obj in self.objects:
            obj.flags = 0b11000001
        self.store.save_many(self.objects[:2])
        self.store.save_phrase_trees([self.phrase])
        segments = self.objects[2:6]
        self._assert_persisted_flags(segments, 0b11000001)
        self.store.save_phrase_trees([self.phrase], overwrite=True)
        self._assert_persisted_flags(segments, 0b11000001)
