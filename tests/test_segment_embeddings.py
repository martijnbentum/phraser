import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / 'phraser'
MODULE_PATH = MODULE_PATH / 'segment_embeddings.py'


def load_module():
    spec = importlib.util.spec_from_file_location(
        'segment_embeddings_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeMetadata:
    def __init__(self, payload):
        self._payload = payload

    def load_payload(self):
        return self._payload


class FakeStore:
    def __init__(self, metadata=None, created=False):
        self.metadata = metadata
        self.created = created
        self.calls = []

    def find_or_compute(self, **kwargs):
        self.calls.append(kwargs)
        if self.metadata is not None:
            return self.metadata, False
        payload = kwargs['compute']()
        return FakeMetadata(payload), self.created


class HiddenStateTests(unittest.TestCase):
    def test_hit_returns_stored_payload_without_computing(self):
        module = load_module()
        payload = [[1.0, 2.0]]
        store = FakeStore(metadata=FakeMetadata(payload))
        segment = types.SimpleNamespace(
            key=b'\x01\x02',
            start=100,
            end=200,
            audio=types.SimpleNamespace(filename='audio.wav', duration=500),
        )

        result = module.get_or_compute_hidden_state(
            segment, layer=7, collar=50, model_name='wav2vec2', store=store)

        self.assertEqual(result.payload, payload)
        self.assertFalse(result.created)
        self.assertEqual(result.phraser_key, '0102')
        self.assertEqual(len(store.calls), 1)

    def test_miss_computes_and_returns_selected_layer(self):
        module = load_module()
        fake_to_vector = types.SimpleNamespace()
        captured = {}

        def filename_to_vector(filename, start, end, model, gpu,
            numpify_output):
            captured['filename'] = filename
            captured['start'] = start
            captured['end'] = end
            captured['model'] = model
            captured['gpu'] = gpu
            captured['numpify_output'] = numpify_output
            return types.SimpleNamespace(hidden_states=['layer-0', 'layer-1'])

        fake_to_vector.filename_to_vector = filename_to_vector
        old = sys.modules.get('to_vector')
        sys.modules['to_vector'] = fake_to_vector
        try:
            store = FakeStore(created=True)
            segment = types.SimpleNamespace(
                key=b'\xaa\xbb',
                start=100,
                end=200,
                audio=types.SimpleNamespace(
                    filename='audio.wav',
                    duration=230,
                ),
            )

            result = module.get_or_compute_hidden_state(
                segment,
                layer=1,
                collar=150,
                model_name='wav2vec2',
                store=store,
                gpu=True,
            )
        finally:
            if old is None:
                del sys.modules['to_vector']
            else:
                sys.modules['to_vector'] = old

        self.assertTrue(result.created)
        self.assertEqual(result.payload, 'layer-1')
        self.assertEqual(result.start_ms, 0)
        self.assertEqual(result.end_ms, 230)
        self.assertEqual(captured['start'], 0.0)
        self.assertEqual(captured['end'], 0.23)
        self.assertEqual(captured['model'], 'facebook/wav2vec2-base')
        self.assertTrue(captured['gpu'])

    def test_layer_range_is_validated_against_outputs(self):
        module = load_module()
        fake_to_vector = types.SimpleNamespace(
            filename_to_vector=lambda *args, **kwargs:
                types.SimpleNamespace(hidden_states=['only-layer'])
        )
        old = sys.modules.get('to_vector')
        sys.modules['to_vector'] = fake_to_vector
        try:
            store = FakeStore(created=True)
            segment = types.SimpleNamespace(
                key='segment-1',
                start=0,
                end=100,
                audio=types.SimpleNamespace(filename='audio.wav', duration=200),
            )
            with self.assertRaisesRegex(ValueError, 'out of range'):
                module.get_or_compute_hidden_state(
                    segment, layer=2, collar=0, store=store)
        finally:
            if old is None:
                del sys.modules['to_vector']
            else:
                sys.modules['to_vector'] = old


if __name__ == '__main__':
    unittest.main()
