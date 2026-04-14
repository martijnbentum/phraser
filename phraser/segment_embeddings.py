'''Hidden-state retrieval for phraser segments.

This module intentionally stays isolated from the rest of the package.
It bridges phraser segment objects with echoframe storage and to-vector
feature extraction without changing the current public API.
'''

from pathlib import Path
import sys

import echoframe
import frame
import numpy as np
import to_vector
from echoframe import Embeddings, TokenEmbeddings
from echoframe.metadata import EchoframeMetadata


_MODEL_NAME_ALIASES = {
    'hubert': 'facebook/hubert-base-ls960',
    'wav2vec2': 'facebook/wav2vec2-base',
    'wavlm': 'microsoft/wavlm-base-plus',
}

_VALID_AGGREGATIONS = (None, 'mean', 'centroid')


def get_embeddings(
    segment,
    layers,
    collar=500,
    model_name='wav2vec2',
    frame_aggregation=None,
    model=None,
    store=None,
    store_root='echoframe',
    gpu=False,
    tags=None,
):
    '''Return embeddings for a single segment.

    segment: phraser segment-like object with .key, .start, .end,
        and .audio
    layers: int or list of int hidden-state layer indices
    collar: extra context in milliseconds added on both sides
    model_name: stable storage label; resolved via built-in aliases
    frame_aggregation: None (all frames), 'mean', or 'centroid'
    model: optional pre-loaded model or path (overrides alias)
    store: optional echoframe.Store instance
    store_root: store root used when store is not provided
    gpu: pass True to request CUDA in to-vector
    tags: optional list of echoframe tags
    '''
    layers_list, single_layer = _normalise_layers(layers)
    _validate_aggregation(frame_aggregation)
    store = _resolve_store(store, store_root)
    compute_model = _resolve_compute_model(model_name, model)
    phraser_key = segment_to_echoframe_key(segment)

    (
        audio_filename,
        col_start_ms,
        col_end_ms,
        orig_start_ms,
        orig_end_ms,
    ) = _segment_window(segment, collar)

    missing = [
        layer
        for layer in layers_list
        if not store.exists(
            phraser_key,
            collar,
            model_name,
            'hidden_state',
            layer,
        )
    ]
    if missing:
        _compute_and_store(
            audio_filename,
            col_start_ms,
            col_end_ms,
            orig_start_ms,
            orig_end_ms,
            collar,
            missing,
            model_name,
            compute_model,
            phraser_key,
            store,
            gpu,
            tags,
        )

    arrays = [
        store.load(
            phraser_key,
            collar,
            model_name,
            'hidden_state',
            layer,
        )
        for layer in layers_list
    ]
    if single_layer:
        echoframe_keys = (
            _make_echoframe_key(
                phraser_key,
                collar,
                model_name,
                layers_list[0],
            ),
        )
    else:
        echoframe_keys = tuple(
            _make_echoframe_key(phraser_key, collar, model_name, layer)
            for layer in layers_list
        )
    return _build_embeddings(
        arrays,
        layers_list,
        single_layer,
        frame_aggregation=frame_aggregation,
        echoframe_keys=echoframe_keys,
    )


def get_embeddings_batch(
    segments,
    layers,
    collar=500,
    model_name='wav2vec2',
    frame_aggregation=None,
    model=None,
    store=None,
    store_root='echoframe',
    gpu=False,
    tags=None,
):
    '''Return embeddings for a list of segments.

    Checks echoframe for each (segment, layer) pair first; only missing
    entries are computed. One model forward pass per segment with missing
    layers — all needed layers extracted at once. Already-stored entries
    are skipped, making repeated calls restartable.

    Returns a dict mapping each segment to an Embeddings instance.
    '''
    layers_list, single_layer = _normalise_layers(layers)
    _validate_aggregation(frame_aggregation)
    store = _resolve_store(store, store_root)
    compute_model = _resolve_compute_model(model_name, model)

    token_list = []
    for segment in segments:
        phraser_key = segment_to_echoframe_key(segment)
        (
            audio_filename,
            col_start_ms,
            col_end_ms,
            orig_start_ms,
            orig_end_ms,
        ) = _segment_window(segment, collar)

        missing = [
            layer
            for layer in layers_list
            if not store.exists(
                phraser_key,
                collar,
                model_name,
                'hidden_state',
                layer,
            )
        ]
        if missing:
            _compute_and_store(
                audio_filename,
                col_start_ms,
                col_end_ms,
                orig_start_ms,
                orig_end_ms,
                collar,
                missing,
                model_name,
                compute_model,
                phraser_key,
                store,
                gpu,
                tags,
            )

        arrays = [
            store.load(
                phraser_key,
                collar,
                model_name,
                'hidden_state',
                layer,
            )
            for layer in layers_list
        ]
        if single_layer:
            echoframe_keys = (
                _make_echoframe_key(
                    phraser_key,
                    collar,
                    model_name,
                    layers_list[0],
                ),
            )
        else:
            echoframe_keys = tuple(
                _make_echoframe_key(phraser_key, collar, model_name, layer)
                for layer in layers_list
            )
        token_list.append(
            _build_embeddings(
                arrays,
                layers_list,
                single_layer,
                frame_aggregation=frame_aggregation,
                echoframe_keys=echoframe_keys,
            )
        )

    return TokenEmbeddings(tokens=token_list)


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


def _normalise_layers(layers):
    '''Return (list_of_layer_ints, single_int_flag).'''
    if isinstance(layers, int):
        return [layers], True
    layers = list(layers)
    if not layers:
        raise ValueError('layers must not be empty')
    return layers, False


def _validate_aggregation(frame_aggregation):
    if frame_aggregation not in _VALID_AGGREGATIONS:
        raise ValueError(
            f'frame_aggregation must be one of {_VALID_AGGREGATIONS}, '
            f'got {frame_aggregation!r}'
        )


def _segment_window(segment, collar):
    '''Resolve audio filename and time boundaries for a segment.

    Returns (
        audio_filename,
        col_start_ms,
        col_end_ms,
        orig_start_ms,
        orig_end_ms,
    ).
    col_* are the collared boundaries used as model input.
    orig_* are the original segment boundaries used for frame selection.
    '''
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
    orig_start_ms = int(start_ms)
    orig_end_ms = int(end_ms)
    col_start_ms = max(0, orig_start_ms - collar)
    col_end_ms = orig_end_ms + collar
    duration = getattr(audio, 'duration', None)
    if duration is not None:
        col_end_ms = min(col_end_ms, int(duration))
    if col_end_ms <= col_start_ms:
        raise ValueError('resolved segment window is invalid')
    return (
        str(Path(audio_filename).resolve()),
        col_start_ms,
        col_end_ms,
        orig_start_ms,
        orig_end_ms,
    )


def _compute_and_store(
    audio_filename,
    col_start_ms,
    col_end_ms,
    orig_start_ms,
    orig_end_ms,
    collar,
    layers,
    model_name,
    compute_model,
    phraser_key,
    store,
    gpu,
    tags,
):
    '''Run one forward pass and store selected frames for each layer.

    The model runs on the full collared window. Only frames that are
    fully within [orig_start_ms, orig_end_ms] (100% overlap) are stored.
    All layers are extracted from the single forward pass.
    '''
    to_vector_module = _resolve_runtime_module('to_vector', to_vector)
    frame_module = _resolve_runtime_module('frame', frame)

    outputs = to_vector_module.filename_to_vector(
        audio_filename,
        start=_ms_to_s(col_start_ms),
        end=_ms_to_s(col_end_ms),
        model=compute_model,
        gpu=gpu,
        numpify_output=True,
    )

    hidden_states = getattr(outputs, 'hidden_states', None)
    if hidden_states is None:
        raise ValueError('to-vector outputs did not contain hidden_states')

    frames = frame_module.make_frames_from_outputs(
        outputs,
        start_time=_ms_to_s(col_start_ms),
    )

    selected = frames.select_frames(
        _ms_to_s(orig_start_ms),
        _ms_to_s(orig_end_ms),
        percentage_overlap=100,
    )

    if not selected:
        raise ValueError(
            f'no frames fully within [{orig_start_ms}, {orig_end_ms}] ms')

    indices = [f.index for f in selected]

    for layer in layers:
        if layer >= len(hidden_states):
            raise ValueError(
                f'layer {layer} out of range '
                f'(model has {len(hidden_states)} layers)')
        hs = hidden_states[layer]
        data = hs[0, indices, :] if hs.ndim == 3 else hs[indices, :]
        store.put(
            phraser_key,
            collar,
            model_name,
            'hidden_state',
            layer,
            data,
            tags=tags,
        )


def _build_embeddings(
    arrays,
    layers_list,
    single_layer,
    frame_aggregation,
    echoframe_keys,
):
    '''Assemble an Embeddings instance from per-layer arrays.'''
    processed = [_apply_aggregation(arr, frame_aggregation) for arr in arrays]

    if single_layer:
        data = processed[0]
        dims = ('embed_dim',) if frame_aggregation else ('frames', 'embed_dim')
        return Embeddings(
            data=data,
            dims=dims,
            layers=None,
            echoframe_keys=echoframe_keys,
            frame_aggregation=frame_aggregation,
        )

    data = np.stack(processed, axis=0)
    dims = (
        ('layers', 'embed_dim')
        if frame_aggregation
        else ('layers', 'frames', 'embed_dim')
    )
    return Embeddings(
        data=data,
        dims=dims,
        layers=tuple(layers_list),
        echoframe_keys=echoframe_keys,
        frame_aggregation=frame_aggregation,
    )


def _apply_aggregation(data, frame_aggregation):
    '''Aggregate frame data of shape (n_frames, embed_dim).'''
    if frame_aggregation is None:
        return data
    if frame_aggregation == 'mean':
        return np.mean(data, axis=0)
    if frame_aggregation == 'centroid':
        return data[len(data) // 2]
    raise ValueError(f'unknown aggregation: {frame_aggregation!r}')


def _make_echoframe_key(phraser_key, collar, model_name, layer):
    md = EchoframeMetadata(
        phraser_key=phraser_key,
        collar=collar,
        model_name=model_name,
        output_type='hidden_state',
        layer=layer,
    )
    return md.echoframe_key


def _ms_to_s(value):
    return int(value) / 1000.0


def _resolve_store(store, store_root):
    if store is not None:
        return store
    return echoframe.Store(store_root)


def _resolve_compute_model(model_name, model):
    if model is not None:
        return model
    return _MODEL_NAME_ALIASES.get(model_name, model_name)


def _resolve_runtime_module(module_name, default_module):
    return sys.modules.get(module_name, default_module)
