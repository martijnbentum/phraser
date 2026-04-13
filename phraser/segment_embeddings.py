'''Lazy hidden-state retrieval for phraser segments.

This module intentionally stays isolated from the rest of the package. It
bridges `phraser` segment objects with `echoframe` storage and `to-vector`
feature extraction without changing the current public API.
'''

from dataclasses import dataclass
from pathlib import Path


_MODEL_NAME_ALIASES = {
    'hubert': 'facebook/hubert-base-ls960',
    'wav2vec2': 'facebook/wav2vec2-base',
    'wavlm': 'microsoft/wavlm-base-plus',
}


@dataclass
class HiddenStateResult:
    '''Resolved hidden-state request for one segment.'''
    payload: object
    metadata: object
    created: bool
    phraser_key: str
    audio_filename: str
    start_ms: int
    end_ms: int
    collar: int
    layer: int
    model_name: str


def get_or_compute_hidden_state(segment, layer, collar=500,
    model_name='wav2vec2', model=None, store=None, store_root='echoframe',
    gpu=False, match='exact', tags=None, add_tags_on_hit=False):
    '''Return one hidden-state payload for a segment.

    segment:             phraser segment-like object
    layer:               hidden-state layer index
    collar:              extra context in milliseconds on both sides
    model_name:          stable storage label used by echoframe
    model:               optional to-vector model instance or model path
    store:               optional echoframe.Store instance
    store_root:          store root used when `store` is not provided
    gpu:                 request CUDA in to-vector
    match:               echoframe collar matching mode
    tags:                optional echoframe tags
    add_tags_on_hit:     update tags on an existing matching record
    '''
    _validate_request(segment, layer, collar, model_name)
    audio_filename, start_ms, end_ms = _segment_window(segment, collar)
    phraser_key = segment_to_echoframe_key(segment)
    compute_model = _resolve_compute_model(model_name, model)
    store = _resolve_store(store, store_root)
    payload_box = {}

    def compute():
        payload = _compute_hidden_state(audio_filename, start_ms, end_ms,
            layer, compute_model, gpu)
        payload_box['payload'] = payload
        return payload

    metadata, created = store.find_or_compute(
        phraser_key=phraser_key,
        collar=collar,
        model_name=model_name,
        output_type='hidden_state',
        layer=layer,
        compute=compute,
        match=match,
        tags=tags,
        add_tags_on_hit=add_tags_on_hit,
    )
    if created:
        payload = payload_box['payload']
    else:
        payload = metadata.load_payload()
    return HiddenStateResult(
        payload=payload,
        metadata=metadata,
        created=created,
        phraser_key=phraser_key,
        audio_filename=audio_filename,
        start_ms=start_ms,
        end_ms=end_ms,
        collar=collar,
        layer=layer,
        model_name=model_name,
    )


def segment_to_echoframe_key(segment):
    '''Convert a phraser segment key into a stable text key for echoframe.'''
    key = getattr(segment, 'key', None)
    if key is None:
        raise ValueError('segment must expose a non-empty key')
    if isinstance(key, bytes):
        return key.hex()
    if isinstance(key, str):
        return key
    raise TypeError('segment.key must be bytes or str')


def _resolve_store(store, store_root):
    if store is not None:
        return store
    echoframe = _import_echoframe()
    return echoframe.Store(store_root)


def _resolve_compute_model(model_name, model):
    if model is not None:
        return model
    return _MODEL_NAME_ALIASES.get(model_name, model_name)


def _compute_hidden_state(audio_filename, start_ms, end_ms, layer, model,
    gpu):
    to_vector = _import_to_vector()
    outputs = to_vector.filename_to_vector(
        audio_filename,
        start=_ms_to_seconds(start_ms),
        end=_ms_to_seconds(end_ms),
        model=model,
        gpu=gpu,
        numpify_output=True,
    )
    hidden_states = getattr(outputs, 'hidden_states', None)
    if hidden_states is None:
        raise ValueError('to-vector outputs did not contain hidden_states')
    if layer >= len(hidden_states):
        m = f'layer {layer} is out of range for {len(hidden_states)} '
        m += 'available hidden states'
        raise ValueError(m)
    return hidden_states[layer]


def _segment_window(segment, collar):
    audio = getattr(segment, 'audio', None)
    if audio is None:
        raise ValueError('segment must be linked to an audio object')
    audio_filename = getattr(audio, 'filename', None)
    if not audio_filename:
        raise ValueError('segment.audio must expose a filename')
    start_ms = getattr(segment, 'start', None)
    end_ms = getattr(segment, 'end', None)
    if start_ms is None or end_ms is None:
        raise ValueError('segment must expose start and end in milliseconds')
    start_ms = max(0, int(start_ms) - collar)
    end_ms = int(end_ms) + collar
    duration = getattr(audio, 'duration', None)
    if duration is not None:
        end_ms = min(end_ms, int(duration))
    if end_ms < start_ms:
        raise ValueError('resolved segment window is invalid')
    return str(Path(audio_filename).resolve()), start_ms, end_ms


def _validate_request(segment, layer, collar, model_name):
    if segment is None:
        raise ValueError('segment must not be None')
    if not isinstance(layer, int) or layer < 0:
        raise ValueError('layer must be a non-negative integer')
    if not isinstance(collar, int) or collar < 0:
        raise ValueError('collar must be a non-negative integer')
    if not isinstance(model_name, str) or not model_name.strip():
        raise ValueError('model_name must be a non-empty string')


def _ms_to_seconds(value):
    return int(value) / 1000.0


def _import_echoframe():
    try:
        import echoframe
    except ImportError as exc:
        raise ImportError(
            'echoframe is required to store or load hidden states'
        ) from exc
    return echoframe


def _import_to_vector():
    try:
        import to_vector
    except ImportError as exc:
        raise ImportError(
            'to-vector is required to compute hidden states'
        ) from exc
    return to_vector
