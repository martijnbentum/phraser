import importlib.util
import types
import unittest
from pathlib import Path

import numpy as np
from echoframe import Embeddings

FAKE_MODEL = object()


MODULE_PATH = (Path(__file__).resolve().parents[1]
               / 'phraser' / 'segment_embeddings.py')


def load_module():
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── Fakes ────────────────────────────────────────────────────────────────────

def make_segment(key=b'\x01\x02', start=1100, end=1400,
                 filename='audio.wav', duration=2000):
    audio = types.SimpleNamespace(filename=filename, duration=duration)
    return types.SimpleNamespace(key=key, start=start, end=end, audio=audio)


class FakeSegment:
    '''Hashable segment fake for use in batch-result dicts.'''
    def __init__(self, key=b'\x01\x02', start=1100, end=1400,
                 filename='audio.wav', duration=2000):
        self.key = key
        self.start = start
        self.end = end
        self.audio = types.SimpleNamespace(filename=filename,
                                           duration=duration)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return isinstance(other, FakeSegment) and self.key == other.key


class FakeFrame:
    def __init__(self, index):
        self.index = index


class FakeFrames:
    '''Fake Frames that returns three frames at indices 0,1,2.'''
    def select_frames(self, start_time, end_time, percentage_overlap=None):
        return [FakeFrame(0), FakeFrame(1), FakeFrame(2)]


def make_fake_frame_module(frames=None):
    fake_frames = frames or FakeFrames()
    return types.SimpleNamespace(
        make_frames_from_outputs=lambda outputs, **kwargs: fake_frames)


class FakeEmptyFrames:
    '''Fake Frames that returns no frames — triggers the no-frames error.'''
    def select_frames(self, start_time, end_time, percentage_overlap=None):
        return []


class FakeStore:
    '''Tracks put/load/exists calls; acts as a simple in-memory store.'''
    def __init__(self, stored=None):
        # stored: dict mapping (phraser_key, collar, model, output_type, layer)
        #         → numpy array
        self._stored = dict(stored or {})
        self.put_calls = []
        self.load_calls = []
        self.exists_calls = []

    def exists(self, phraser_key, collar, model_name, output_type, layer,
               match='exact'):
        self.exists_calls.append(
            (phraser_key, collar, model_name, output_type, layer))
        return (phraser_key, collar, model_name, output_type, layer) \
            in self._stored

    def put(self, phraser_key, collar, model_name, output_type, layer,
            data, tags=None):
        self.put_calls.append(
            (phraser_key, collar, model_name, output_type, layer))
        self._stored[(phraser_key, collar, model_name, output_type, layer)] \
            = data

    def load(self, phraser_key, collar, model_name, output_type, layer,
             match='exact'):
        self.load_calls.append(
            (phraser_key, collar, model_name, output_type, layer))
        key = (phraser_key, collar, model_name, output_type, layer)
        if key not in self._stored:
            raise ValueError('not found in fake store')
        return self._stored[key]


def make_fake_to_vector(n_layers=13, n_frames=50, hidden_dim=768,
                        captured=None):
    '''Return a fake to_vector module that records filename_to_vector calls.'''
    def filename_to_vector(filename, start, end, model, gpu,
                           numpify_output):
        if captured is not None:
            captured['filename'] = filename
            captured['start'] = start
            captured['end'] = end
            captured['model'] = model
            captured['gpu'] = gpu
        hidden_states = [
            np.zeros((1, n_frames, hidden_dim)) for _ in range(n_layers)
        ]
        return types.SimpleNamespace(hidden_states=hidden_states)

    return types.SimpleNamespace(filename_to_vector=filename_to_vector)


class ModuleFixture(unittest.TestCase):
    '''Base class: loads module fresh and manages module patches.'''

    def setUp(self):
        self.module = load_module()
        self._old = {}

    def _patch(self, name, fake):
        self._old[name] = getattr(self.module, name)
        setattr(self.module, name, fake)

    def tearDown(self):
        for name, old in self._old.items():
            setattr(self.module, name, old)


# ── F2: frame selection before storage ───────────────────────────────────────

class TestFrameSelection(ModuleFixture):

    def _run(self, segment, collar, frames=None, n_layers=13, captured=None):
        store = FakeStore()
        fake_tv = make_fake_to_vector(n_layers=n_layers, captured=captured)
        self._patch('to_vector', fake_tv)
        self._patch('frame', make_fake_frame_module(frames))
        self.module.get_embeddings(segment, layers=6, collar=collar,
                                   store=store, model=FAKE_MODEL)
        return store

    def test_model_called_with_collared_window(self):
        captured = {}
        self._run(make_segment(start=1100, end=1400), collar=500,
                  captured=captured)
        self.assertAlmostEqual(captured['start'], 0.6)
        self.assertAlmostEqual(captured['end'], 1.9)

    def test_stored_frames_use_selected_indices(self):
        # FakeFrames returns indices [0,1,2]; stored data must have 3 rows
        store = self._run(make_segment(start=1100, end=1400), collar=500)
        self.assertEqual(len(store.put_calls), 1)
        stored = store._stored[store.put_calls[0]]
        self.assertEqual(stored.shape[0], 3)

    def test_collar_clamped_at_audio_start(self):
        captured = {}
        self._run(make_segment(start=100, end=400, duration=2000),
                  collar=500, captured=captured)
        self.assertAlmostEqual(captured['start'], 0.0)

    def test_no_frames_raises(self):
        store = FakeStore()
        self._patch('to_vector', make_fake_to_vector())
        self._patch('frame', make_fake_frame_module(FakeEmptyFrames()))
        with self.assertRaises(ValueError):
            self.module.get_embeddings(make_segment(), layers=6,
                                       store=store, model=FAKE_MODEL)


# ── F3: multi-layer support ───────────────────────────────────────────────────

class TestMultiLayer(ModuleFixture):

    def setUp(self):
        super().setUp()
        self._patch('to_vector', make_fake_to_vector(n_layers=13))
        self._patch('frame', make_fake_frame_module())

    def test_single_int_layer_produces_no_layers_dim(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, store=store, model=FAKE_MODEL)
        self.assertEqual(result.dims, ('frames', 'embed_dim'))
        self.assertIsNone(result.layers)

    def test_list_of_layers_stores_each_separately(self):
        store = FakeStore()
        self.module.get_embeddings(
            make_segment(), layers=[4, 6], store=store, model=FAKE_MODEL)
        stored_layers = {call[4] for call in store.put_calls}
        self.assertEqual(stored_layers, {4, 6})

    def test_list_of_layers_result_has_layers_dim(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=[4, 6], store=store, model=FAKE_MODEL)
        self.assertEqual(result.dims, ('layers', 'frames', 'embed_dim'))
        self.assertEqual(result.layers, (4, 6))
        self.assertEqual(result.data.shape[0], 2)

    def test_one_model_call_for_all_missing_layers(self):
        call_count = {'n': 0}
        original_tv = make_fake_to_vector(n_layers=13)

        def counting_ftv(*args, **kwargs):
            call_count['n'] += 1
            return original_tv.filename_to_vector(*args, **kwargs)

        self._patch('to_vector',
                    types.SimpleNamespace(filename_to_vector=counting_ftv))
        store = FakeStore()
        self.module.get_embeddings(
            make_segment(), layers=[4, 6, 8], store=store, model=FAKE_MODEL)
        self.assertEqual(call_count['n'], 1)

    def test_out_of_range_layer_raises(self):
        store = FakeStore()
        with self.assertRaises(ValueError):
            self.module.get_embeddings(
                make_segment(), layers=99, store=store, model=FAKE_MODEL)


# ── F4: aggregation on retrieval ─────────────────────────────────────────────

class TestAggregation(ModuleFixture):

    def setUp(self):
        super().setUp()
        self._patch('to_vector', make_fake_to_vector(n_layers=13))
        self._patch('frame', make_fake_frame_module())

    def test_no_aggregation_preserves_frames_dim(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation=None,
            store=store, model=FAKE_MODEL)
        self.assertEqual(result.dims, ('frames', 'embed_dim'))
        self.assertEqual(result.data.ndim, 2)

    def test_mean_aggregation_removes_frames_dim(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='mean',
            store=store, model=FAKE_MODEL)
        self.assertEqual(result.dims, ('embed_dim',))
        self.assertEqual(result.data.ndim, 1)

    def test_centroid_aggregation_removes_frames_dim(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='centroid',
            store=store, model=FAKE_MODEL)
        self.assertEqual(result.dims, ('embed_dim',))
        self.assertEqual(result.data.ndim, 1)

    def test_mean_multi_layer_dims(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=[4, 6], frame_aggregation='mean',
            store=store, model=FAKE_MODEL)
        self.assertEqual(result.dims, ('layers', 'embed_dim'))
        self.assertEqual(result.data.shape[0], 2)

    def test_unknown_aggregation_raises(self):
        store = FakeStore()
        with self.assertRaises(ValueError):
            self.module.get_embeddings(
                make_segment(), layers=4, frame_aggregation='unknown',
                store=store, model=FAKE_MODEL)

    def test_mean_values_are_correct(self):
        data = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        key = ('0102', 500, 'wav2vec2', 'hidden_state', 4)
        store = FakeStore(stored={key: data})
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='mean',
            store=store, model=FAKE_MODEL, collar=500)
        np.testing.assert_array_almost_equal(
            result.data, np.mean(data, axis=0))


# ── F5: batch function ────────────────────────────────────────────────────────

class TestBatch(ModuleFixture):

    def setUp(self):
        super().setUp()
        self._patch('frame', make_fake_frame_module())

    def _make_counting_tv(self, counter):
        original = make_fake_to_vector(n_layers=13)

        def counting_ftv(*args, **kwargs):
            counter['n'] += 1
            return original.filename_to_vector(*args, **kwargs)

        return types.SimpleNamespace(filename_to_vector=counting_ftv)

    def test_all_hits_no_model_call(self):
        seg = FakeSegment()
        key = ('0102', 500, 'wav2vec2', 'hidden_state', 6)
        store = FakeStore(stored={key: np.zeros((3, 768))})
        counter = {'n': 0}
        self._patch('to_vector', self._make_counting_tv(counter))
        self.module.get_embeddings_batch(
            [seg], layers=6, store=store, model=FAKE_MODEL)
        self.assertEqual(counter['n'], 0)

    def test_miss_triggers_one_model_call_per_segment(self):
        seg1 = FakeSegment(key=b'\x01', start=100, end=200)
        seg2 = FakeSegment(key=b'\x02', start=500, end=600)
        store = FakeStore()
        counter = {'n': 0}
        self._patch('to_vector', self._make_counting_tv(counter))
        self.module.get_embeddings_batch(
            [seg1, seg2], layers=6, store=store, model=FAKE_MODEL)
        self.assertEqual(counter['n'], 2)

    def test_batch_returns_result_for_every_segment(self):
        segs = [FakeSegment(key=bytes([i]), start=i*100, end=i*100+50)
                for i in range(1, 4)]
        store = FakeStore()
        self._patch('to_vector', make_fake_to_vector(n_layers=13))
        results = self.module.get_embeddings_batch(
            segs, layers=6, store=store, model=FAKE_MODEL)
        self.assertEqual(results.token_count, len(segs))

    def test_partial_hit_computes_only_missing_layer(self):
        seg = FakeSegment()
        key4 = ('0102', 500, 'wav2vec2', 'hidden_state', 4)
        store = FakeStore(stored={key4: np.zeros((3, 768))})
        counter = {'n': 0}
        self._patch('to_vector', self._make_counting_tv(counter))
        self.module.get_embeddings_batch(
            [seg], layers=[4, 6], store=store, model=FAKE_MODEL)
        self.assertEqual(counter['n'], 1)
        self.assertEqual({call[4] for call in store.put_calls}, {6})

    def test_restartable_skips_stored_entries(self):
        seg = FakeSegment()
        key6 = ('0102', 500, 'wav2vec2', 'hidden_state', 6)
        store = FakeStore(stored={key6: np.zeros((3, 768))})
        counter = {'n': 0}
        self._patch('to_vector', self._make_counting_tv(counter))
        self.module.get_embeddings_batch(
            [seg], layers=6, store=store, model=None)
        self.module.get_embeddings_batch(
            [seg], layers=6, store=store, model=None)
        self.assertEqual(counter['n'], 0)

    def test_cache_miss_without_model_raises(self):
        store = FakeStore()
        self._patch('to_vector', make_fake_to_vector(n_layers=13))
        with self.assertRaisesRegex(ValueError, 'loaded model object'):
            self.module.get_embeddings(
                make_segment(), layers=4, store=store, model=None)

    def test_cache_miss_string_model_raises(self):
        store = FakeStore()
        self._patch('to_vector', make_fake_to_vector(n_layers=13))
        with self.assertRaisesRegex(TypeError, 'loaded model object'):
            self.module.get_embeddings(
                make_segment(), layers=4, store=store, model='dummy')


# ── Real Embeddings integration ───────────────────────────────────────────────

class TestRealEmbeddings(ModuleFixture):
    '''Verify the module returns real echoframe.Embeddings instances and
    that their methods behave correctly end-to-end.'''

    def setUp(self):
        super().setUp()
        self._patch('to_vector', make_fake_to_vector(n_layers=13,
                                                     n_frames=5,
                                                     hidden_dim=4))
        self._patch('frame', make_fake_frame_module())

    def test_result_is_embeddings_instance(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, store=store, model=FAKE_MODEL)
        self.assertIsInstance(result, Embeddings)

    def test_layer_method_extracts_correct_layer(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=[4, 6], store=store, model=FAKE_MODEL)
        extracted = result.layer(6)
        self.assertIsInstance(extracted, Embeddings)
        self.assertEqual(extracted.dims, ('frames', 'embed_dim'))
        self.assertIsNone(extracted.layers)

    def test_layer_method_wrong_index_raises(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=[4, 6], store=store, model=FAKE_MODEL)
        with self.assertRaises(ValueError):
            result.layer(99)

    def test_embeddings_shape_matches_selected_frames(self):
        # FakeFrames always returns 3 frames; hidden_dim=4
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, store=store, model=FAKE_MODEL)
        self.assertEqual(result.shape, (3, 4))

    def test_mean_aggregation_result_is_embeddings(self):
        store = FakeStore()
        result = self.module.get_embeddings(
            make_segment(), layers=4, frame_aggregation='mean',
            store=store, model=FAKE_MODEL)
        self.assertIsInstance(result, Embeddings)
        self.assertEqual(result.dims, ('embed_dim',))


# ── segment_to_echoframe_key ─────────────────────────────────────────────────

class TestSegmentKey(ModuleFixture):

    def test_bytes_key_returns_hex(self):
        seg = types.SimpleNamespace(key=b'\xaa\xbb')
        self.assertEqual(self.module.segment_to_echoframe_key(seg), 'aabb')

    def test_str_key_returned_as_is(self):
        seg = types.SimpleNamespace(key='my-key')
        self.assertEqual(self.module.segment_to_echoframe_key(seg), 'my-key')

    def test_missing_key_raises(self):
        seg = types.SimpleNamespace()
        with self.assertRaises(ValueError):
            self.module.segment_to_echoframe_key(seg)


if __name__ == '__main__':
    unittest.main()
