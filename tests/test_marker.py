import io
import struct
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

from phraser import Audio, Marker, SEGMENT_KEY_LENGTH, Speaker, Store
from phraser import key_helper, struct_helper, struct_value
from phraser.marker import make_marker
from phraser.model_helper import EMPTY_ID
from phraser.segment import Phone, Phrase, Syllable, Word


class TestMarkerKeys(unittest.TestCase):
    def setUp(self):
        self.marker = Marker('event', 1250, 1500, b'audio-id', b'speaker!')

    def test_key_round_trip(self):
        marker = self.marker
        expected = struct.pack('>B8sBI8s', 0, marker.audio_id, 6,
            marker.start, marker.identifier)
        self.assertEqual(marker.key, expected)
        self.assertEqual(len(marker.key), SEGMENT_KEY_LENGTH)
        self.assertEqual(key_helper.key_to_object_type(marker.key), 'Marker')
        self.assertEqual(marker.key_info, {'object_type': 'Marker',
            'audio_id': marker.audio_id, 'start': marker.start,
            'identifier': marker.identifier})

    def test_marker_key_format(self):
        self.assertEqual(struct_helper.make_key_fmt_for_class('marker'),
            key_helper.SEGMENT_FMT)

    def test_existing_ranks_remain_compatible(self):
        expected = {'Audio': 0, 'Phrase': 1, 'Word': 2, 'Syllable': 3,
            'Phone': 4, 'Speaker': 5}
        for name, rank in expected.items():
            self.assertEqual(struct_helper.CLASS_RANK_MAP[name], rank)
            self.assertEqual(struct_helper.RANK_CLASS_MAP[rank], name)

    def test_store_registers_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            with redirect_stdout(io.StringIO()):
                store = Store(path=directory)
            try:
                self.assertIs(store.CLASS_MAP['Marker'], Marker)
                self.assertIs(store.query_for_class(Marker), store.markers)
                self.assertEqual(list(store.markers), [])
                marker = store.create(Marker, label='event', start=0, end=100,
                    audio_id=b'audio-id', speaker_id=b'speaker!')
                self.assertIs(marker.store, store)
                self.assertEqual(marker.key_info['object_type'], 'Marker')
            finally:
                store.close()


class TestMarkerSerialization(unittest.TestCase):
    def test_value_round_trip(self):
        marker = Marker('gelach café 🎵', 1250, 1500, b'audio-id', b'speaker!')
        value = struct_value.pack_instance(marker)
        self.assertEqual(marker.to_struct_value(), value)
        self.assertEqual(struct_value.unpack_instance('Marker', value),
            {'version': 1, 'flags': 0, 'end': 1500,
                'speaker_id': b'speaker!', 'label': marker.label,
                'phrase_id': EMPTY_ID, 'phrase_start': 0})

    def test_invalid_value_bytes_are_rejected(self):
        marker = Marker('event', 0, 100, b'audio-id', b'speaker!')
        value = marker.to_struct_value()
        for invalid in (b'', value[:13], value[:-1], value + b'x'):
            with self.subTest(value=invalid):
                with self.assertRaises(ValueError):
                    struct_value.unpack_instance('Marker', invalid)


class TestMarkerPersistence(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = directory.name
        self.store = self._open_store()
        self.audio = self.store.create(Audio, filename='markers.wav',
            duration=5000, save=True)
        self.speaker = self.store.create(Speaker, name='speaker',
            dataset='test', save=True)

    def _open_store(self):
        with redirect_stdout(io.StringIO()):
            store = Store(path=self.path)
        self.addCleanup(store.close)
        return store

    def _marker(self, label='gelach café 🎵', start=100):
        return self.store.create(Marker, label=label, start=start,
            end=start + 200, audio_id=self.audio.identifier,
            speaker_id=self.speaker.identifier)

    def _reopen_store(self):
        self.store.close()
        self.store = self._open_store()

    def test_save_reopen_and_load(self):
        marker = self._marker()
        marker.save()
        self._reopen_store()
        loaded = self.store.load(marker.key)
        self.assertIsInstance(loaded, Marker)
        self.assertIsNot(loaded, marker)
        for field in Marker.DB_FIELDS:
            self.assertEqual(getattr(loaded, field), getattr(marker, field))
        self.assertEqual(loaded.to_struct_value(), marker.to_struct_value())
        self.assertIs(loaded.store, self.store)
        self.assertEqual(loaded.audio.filename, self.audio.filename)
        self.assertEqual(loaded.speaker.identifier, self.speaker.identifier)
        self.assertIsNone(loaded.parent)
        self.assertIsNone(loaded.parent_key)
        self.assertEqual(loaded.children, [])
        self.assertEqual(loaded.descendants, [])
        self.assertEqual(list(loaded.iter_ancestors()), [])

    def test_bulk_save_load_and_label_lookup(self):
        markers = [self._marker(start=100), self._marker(start=500)]
        self.store.save_many(markers)
        self._reopen_store()
        keys = [marker.key for marker in markers]
        loaded = self.store.load_many(keys)
        self.assertEqual([marker.key for marker in loaded], keys)
        for original, restored in zip(markers, loaded):
            self.assertIsNot(original, restored)
            self.assertEqual(restored.label, original.label)
        found = self.store.label_to_instances(markers[0].label, 'Marker')
        self.assertEqual({marker.key for marker in found}, set(keys))
        self.assertEqual({marker.key for marker in self.store.markers},
            set(keys))

    def test_loaded_marker_can_be_overwritten(self):
        marker = self._marker()
        marker.save()
        self._reopen_store()
        loaded = self.store.load(marker.key)
        loaded.end = 450
        loaded.save(overwrite=True)
        self._reopen_store()
        self.assertEqual(self.store.load(marker.key).end, 450)

    def test_loaded_marker_rejects_changed_links(self):
        marker = self._marker()
        marker.save()
        self._reopen_store()
        loaded = self.store.load(marker.key)
        for field in ('audio_id', 'speaker_id'):
            original = getattr(loaded, field)
            setattr(loaded, field, b'changed!')
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    loaded.save(overwrite=True)
            setattr(loaded, field, original)

    def test_phrase_link_survives_reopen(self):
        marker = self._marker()
        phrase, word, syllable, phone = make_phrase_tree(
            marker.audio_id, marker.speaker_id)
        marker.attach_phrase(phrase)
        self.store.save_phrase_trees([phrase])
        marker.save()
        self._reopen_store()
        loaded = self.store.load(marker.key)
        self.assertEqual(loaded.phrase.key, phrase.key)
        self.assertEqual(loaded.phrase_start, phrase.start)
        self.assertIsNone(loaded.parent)
        self.assertEqual([item.key for item in loaded.phrase.children],
            [word.key])
        self.assertEqual([item.key for item in loaded.overlap_items],
            [phrase.key, word.key, syllable.key, phone.key])

    def test_make_marker_finds_phrase_from_audio(self):
        phrase, _, _, _ = make_phrase_tree(
            self.audio.identifier, self.speaker.identifier)
        self.store.save_phrase_trees([phrase])
        marker = make_marker('event', 200, 300, self.audio, self.store)
        self.assertIs(marker.phrase, phrase)
        self.assertEqual(marker.speaker_id, self.speaker.identifier)
        self.assertEqual(marker.audio_id, self.audio.identifier)
        self.assertIs(marker.store, self.store)
        self.assertEqual((marker.start, marker.end), (200, 300))
        self.assertFalse(self.store.DB.key_exists(marker.key))
        marker.save()
        self._reopen_store()
        self.assertEqual(self.store.load(marker.key).phrase.key, phrase.key)

    def test_make_marker_with_phrase_skips_audio_search(self):
        phrase, _, _, _ = make_phrase_tree(
            self.audio.identifier, self.speaker.identifier)
        with mock.patch.object(Audio, 'phrases', new_callable=mock.PropertyMock,
            side_effect=AssertionError('Unexpected phrase search')):
            marker = make_marker('event', 200, 300, self.audio, self.store,
                phrase=phrase)
        self.assertIs(marker.phrase, phrase)

    def test_make_marker_rejects_phrase_from_other_audio(self):
        phrase, _, _, _ = make_phrase_tree(speaker_id=self.speaker.identifier)
        with self.assertRaises(ValueError):
            make_marker('event', 200, 300, self.audio, self.store, phrase=phrase)

    def test_make_marker_uses_first_overlapping_phrase(self):
        first = Phrase('first', 100, 400, self.audio.identifier,
            self.speaker.identifier)
        second = Phrase('second', 200, 500, self.audio.identifier, b'other-sp')
        before = Phrase('before', 0, 100, self.audio.identifier,
            self.speaker.identifier)
        self.audio._phrases = [before, first, second]
        marker = make_marker('event', 250, 300, self.audio, self.store)
        self.assertIs(marker.phrase, first)
        self.assertEqual(marker.speaker_id, first.speaker_id)

    def test_make_marker_requires_overlap_during_lookup(self):
        phrase = Phrase('phrase', 100, 400, self.audio.identifier,
            self.speaker.identifier)
        self.audio._phrases = [phrase]
        for start, end in ((0, 100), (400, 500)):
            with self.subTest(start=start, end=end):
                with self.assertRaisesRegex(ValueError, 'No overlapping'):
                    make_marker('event', start, end, self.audio, self.store)
        self.audio._phrases = []
        with self.assertRaisesRegex(ValueError, 'No overlapping'):
            make_marker('event', 200, 300, self.audio, self.store)


def make_phrase_tree(audio_id=b'audio-id', speaker_id=b'speaker!'):
    '''Build a staged phrase with one word, syllable, and phone.'''
    identity = {'audio_id': audio_id, 'speaker_id': speaker_id}
    phrase = Phrase('phrase', 50, 1000, **identity)
    word = Word('word', 100, 900, **identity)
    syllable = Syllable('syllable', 100, 900, **identity)
    phone = Phone('phone', 200, 400, **identity)
    phrase.add_child(word)
    word.add_child(syllable)
    syllable.add_child(phone)
    return phrase, word, syllable, phone


class TestMarkerPhraseLink(unittest.TestCase):
    def test_attach_relink_and_detach_without_parent(self):
        marker = Marker('event', 200, 300, b'audio-id', b'speaker!')
        first, word, _, _ = make_phrase_tree()
        second = Phrase('other', 1000, 2000, b'audio-id', b'speaker!')
        marker.attach_phrase(first)
        self.assertIs(marker.phrase, first)
        self.assertEqual(marker.phrase_key, first.key)
        self.assertTrue(marker.overlap)
        self.assertEqual(first.children, [word])
        self.assertIsNone(marker.parent)
        self.assertEqual(list(marker.iter_ancestors()), [])
        marker.attach_phrase(second)
        self.assertIs(marker.phrase, second)
        self.assertTrue(marker.overlap)
        self.assertEqual(second.children, [])
        marker.attach_phrase(None)
        self.assertIsNone(marker.phrase)
        self.assertIsNone(marker.phrase_key)
        self.assertFalse(marker.overlap)

    def test_constructor_accepts_phrase(self):
        phrase, _, _, _ = make_phrase_tree()
        marker = Marker('event', 200, 300, b'audio-id', b'speaker!',
            phrase=phrase)
        self.assertIs(marker.phrase, phrase)

    def test_mismatched_link_preserves_existing_phrase(self):
        first, _, _, _ = make_phrase_tree()
        marker = Marker('event', 200, 300, b'audio-id', b'speaker!',
            phrase=first)
        for field in ('audio_id', 'speaker_id'):
            other, _, _, _ = make_phrase_tree()
            setattr(other, field, b'changed!')
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    marker.attach_phrase(other)
                self.assertIs(marker.phrase, first)

    def test_phrase_does_not_need_to_exist_in_store(self):
        phrase, _, _, _ = make_phrase_tree()
        marker = Marker('event', 200, 300, b'audio-id', b'speaker!')
        marker.attach_phrase(phrase)
        fields = struct_value.unpack_marker(marker.to_struct_value())
        self.assertEqual(fields['phrase_id'], phrase.identifier)

    def test_overlapping_objects_at_each_level(self):
        phrase, word, syllable, phone = make_phrase_tree()
        identity = {'audio_id': phrase.audio_id,
            'speaker_id': phrase.speaker_id}
        following = Phone('following', 400, 600, **identity)
        syllable.add_child(following)
        marker = Marker('event', 300, 500, phrase=phrase, **identity)
        self.assertEqual(marker.overlap_word, [word])
        self.assertEqual(marker.overlap_syllable, [syllable])
        self.assertEqual(marker.overlap_phone, [phone, following])
        self.assertEqual(marker.overlap_items,
            [phrase, word, syllable, phone, following])

    def test_boundaries_and_changed_interval(self):
        phrase, word, syllable, phone = make_phrase_tree()
        marker = Marker('event', 400, 500, b'audio-id', b'speaker!',
            phrase=phrase)
        self.assertEqual(marker.overlap_phone, [])
        self.assertEqual(marker.overlap_items, [phrase, word, syllable])
        marker.start = 300
        self.assertEqual(marker.overlap_phone, [phone])
        marker.start, marker.end = 1000, 1100
        self.assertTrue(marker.overlap)
        self.assertEqual(marker.overlap_items, [])
        self.assertEqual(marker.overlap_word, [])

    def test_unlinked_marker_has_no_overlaps(self):
        marker = Marker('event', 200, 300, b'audio-id', b'speaker!')
        self.assertFalse(marker.overlap)
        self.assertEqual(marker.overlap_word, [])
        self.assertEqual(marker.overlap_syllable, [])
        self.assertEqual(marker.overlap_phone, [])
        self.assertEqual(marker.overlap_items, [])

    def test_only_descend_through_overlapping_items(self):
        phrase, word, _, _ = make_phrase_tree()
        identity = {'audio_id': phrase.audio_id,
            'speaker_id': phrase.speaker_id}
        outside = Word('outside', 900, 1000, **identity)
        phrase.add_child(outside)
        marker = Marker('event', 200, 300, phrase=phrase, **identity)
        def children(item):
            if item is outside:
                raise AssertionError('Traversed a non-overlapping word')
            return item._children

        with mock.patch.object(Word, 'children', new=property(children)):
            self.assertEqual(marker.overlap_word, [word])
            self.assertEqual(len(marker.overlap_phone), 1)
