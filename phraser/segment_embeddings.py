'''Hidden-state retrieval for phraser segments.

This module intentionally stays isolated from the rest of the package.
It bridges phraser segment objects with echoframe storage and to-vector
feature extraction without changing the current public API.
'''

from pathlib import Path

import echoframe
import frame
import numpy as np
import to_vector
from echoframe import (
    Codebook,
    Embeddings,
    TokenCodebooks,
    TokenEmbeddings,
)
from echoframe.metadata import EchoframeMetadata
import to_vector.model_registry as to_vector_model_registry


_VALID_AGGREGATIONS = (None, 'mean', 'centroid')


def get_embeddings(segment, layers, collar=500, model_name='wav2vec2',
    frame_aggregation=None, model=None, store=None,
    store_root='echoframe', gpu=False, tags=None):
    '''Return embeddings for a single segment.

    segment:           phraser segment-like object with .key, .start,
                       .end, and .audio
    layers:            int or list of int hidden-state layer indices
    collar:            extra context in milliseconds added on both sides
    model_name:        stable storage label used in echoframe
    frame_aggregation: None (all frames), 'mean', or 'centroid'
    model:             optional loaded model object; only required when
                       embeddings must be computed
    store:             optional echoframe.Store instance
    store_root:        store root used when store is not provided
    gpu:               pass True to request CUDA in to-vector
    tags:              optional list of echoframe tags
    '''
    layers_list, single_layer = _normalise_layers(layers)
    _validate_aggregation(frame_aggregation)
    if store is None:
        store = echoframe.Store(store_root)
    phraser_key = segment_to_echoframe_key(segment)

    audio_filename, col_start_ms, col_end_ms, orig_start_ms, orig_end_ms = (
        _segment_window(segment, collar))

    missing = [layer for layer in layers_list if not store.exists(
        phraser_key, collar, model_name, 'hidden_state', layer)]
    if missing:
        compute_model = _require_loaded_model(model, 'embeddings')
        _compute_and_store(audio_filename, col_start_ms, col_end_ms,
            orig_start_ms, orig_end_ms, collar, missing, model_name,
            compute_model, phraser_key, store, gpu, tags)

    arrays = [store.load(phraser_key, collar, model_name, 'hidden_state',
        layer) for layer in layers_list]
    if single_layer:
        echoframe_keys = (_make_echoframe_key(phraser_key, collar,
            model_name, layers_list[0]),)
    else:
        echoframe_keys = tuple(_make_echoframe_key(phraser_key, collar,
            model_name, layer) for layer in layers_list)
    return _build_embeddings(arrays, layers_list, single_layer,
        frame_aggregation=frame_aggregation,
        echoframe_keys=echoframe_keys)


def get_embeddings_batch(segments, layers, collar=500,
    model_name='wav2vec2', frame_aggregation=None, model=None,
    store=None, store_root='echoframe', gpu=False, tags=None):
    '''Return embeddings for a list of segments.

    segments:           segment-like objects with .key, .start, .end,
                        and .audio
    layers:             int or list of int hidden-state layer indices
    collar:             extra context in milliseconds added on both sides
    model_name:         stable storage label used in echoframe
    frame_aggregation:  None (all frames), 'mean', or 'centroid'
    model:              optional loaded model object; only required when
                        embeddings must be computed
    store:              optional echoframe.Store instance
    store_root:         store root used when store is not provided
    gpu:                pass True to request CUDA in to-vector
    tags:               optional list of echoframe tags
    '''
    layers_list, single_layer = _normalise_layers(layers)
    _validate_aggregation(frame_aggregation)
    if store is None:
        store = echoframe.Store(store_root)

    token_list = []
    for segment in segments:
        phraser_key = segment_to_echoframe_key(segment)
        audio_filename, col_start_ms, col_end_ms, orig_start_ms, orig_end_ms = (
            _segment_window(segment, collar))

        missing = [layer for layer in layers_list if not store.exists(
            phraser_key, collar, model_name, 'hidden_state', layer)]
        if missing:
            compute_model = _require_loaded_model(model, 'embeddings')
            _compute_and_store(audio_filename, col_start_ms, col_end_ms,
                orig_start_ms, orig_end_ms, collar, missing, model_name,
                compute_model, phraser_key, store, gpu, tags)

        arrays = [store.load(phraser_key, collar, model_name, 'hidden_state',
            layer) for layer in layers_list]
        if single_layer:
            echoframe_keys = (_make_echoframe_key(phraser_key, collar,
                model_name, layers_list[0]),)
        else:
            echoframe_keys = tuple(_make_echoframe_key(phraser_key, collar,
                model_name, layer) for layer in layers_list)
        token_list.append(_build_embeddings(arrays, layers_list,
            single_layer, frame_aggregation=frame_aggregation,
            echoframe_keys=echoframe_keys))

    return TokenEmbeddings(tokens=token_list)


def get_codebook_indices(segment, collar=500, model_name='wav2vec2',
    model=None, store=None, store_root='echoframe', gpu=False, tags=None):
    '''Return codebook indices for a single segment.

    segment:      phraser segment-like object with .key, .start, .end, .audio
    collar:       extra context in milliseconds added on both sides
    model_name:   stable storage label used in echoframe
    model:        optional loaded model object; only required when
                  codebook indices must be computed
    store:        optional echoframe.Store instance
    store_root:   store root used when store is not provided
    gpu:          pass True to request CUDA in to-vector
    tags:         optional list of echoframe tags
    '''
    if store is None:
        store = echoframe.Store(store_root)
    phraser_key = segment_to_echoframe_key(segment)
    audio_filename, col_start_ms, col_end_ms, orig_start_ms, orig_end_ms = (
        _segment_window(segment, collar))

    if _codebook_artifacts_missing(store, phraser_key, collar, model_name):
        compute_model = _require_loaded_model(model, 'codebook indices')
        model_architecture = _resolve_codebook_model_architecture(
            compute_model)
        _compute_and_store_codebook_indices(audio_filename, col_start_ms,
            col_end_ms, orig_start_ms, orig_end_ms, collar, model_name,
            compute_model, model_architecture, phraser_key, store, gpu, tags)
        return _load_codebook_indices_object(store, phraser_key, collar,
            model_name, model_architecture)
    return _load_codebook_indices_object(store, phraser_key, collar,
        model_name, model_architecture=None)


def get_codebook_indices_batch(segments, collar=500,
    model_name='wav2vec2', model=None, store=None,
    store_root='echoframe', gpu=False, tags=None):
    '''Return codebook indices for a list of segments.'''
    if store is None:
        store = echoframe.Store(store_root)
    token_list = []
    for segment in segments:
        phraser_key = segment_to_echoframe_key(segment)
        audio_filename, col_start_ms, col_end_ms, orig_start_ms, orig_end_ms = (
            _segment_window(segment, collar))
        if _codebook_artifacts_missing(store, phraser_key, collar,
            model_name):
            compute_model = _require_loaded_model(model, 'codebook indices')
            model_architecture = _resolve_codebook_model_architecture(
                compute_model)
            _compute_and_store_codebook_indices(audio_filename, col_start_ms,
                col_end_ms, orig_start_ms, orig_end_ms, collar, model_name,
                compute_model, model_architecture, phraser_key, store, gpu,
                tags)
        token_list.append(_load_codebook_indices_object(store, phraser_key,
            collar, model_name,
            model_architecture=_resolve_codebook_model_architecture(model)
            if model is not None and not isinstance(model, (str, Path))
            else None))
    return TokenCodebooks(tokens=token_list)


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
        message = 'frame_aggregation must be one of '
        message += f'{_VALID_AGGREGATIONS}, got {frame_aggregation!r}'
        raise ValueError(message)


def _segment_window(segment, collar):
    '''Resolve audio filename and time boundaries for a segment.

    Returns (audio_filename, col_start_ms, col_end_ms, orig_start_ms,
    orig_end_ms).
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
    return (str(Path(audio_filename).resolve()), col_start_ms, col_end_ms,
        orig_start_ms, orig_end_ms)


def _compute_and_store(audio_filename, col_start_ms, col_end_ms,
    orig_start_ms, orig_end_ms, collar, layers, model_name,
    compute_model, phraser_key, store, gpu, tags):
    '''Run one forward pass and store selected frames for each layer.

    The model runs on the full collared window. Only frames that are
    fully within [orig_start_ms, orig_end_ms] (100% overlap) are stored.
    All layers are extracted from the single forward pass.
    '''
    outputs = to_vector.filename_to_vector(audio_filename,
        start=_ms_to_s(col_start_ms), end=_ms_to_s(col_end_ms),
        model=compute_model, gpu=gpu, numpify_output=True)

    hidden_states = getattr(outputs, 'hidden_states', None)
    if hidden_states is None:
        raise ValueError('to-vector outputs did not contain hidden_states')

    frames = frame.make_frames_from_outputs(outputs,
        start_time=_ms_to_s(col_start_ms))

    selected = frames.select_frames(_ms_to_s(orig_start_ms),
        _ms_to_s(orig_end_ms), percentage_overlap=100)

    if not selected:
        message = 'no frames fully within '
        message += f'[{orig_start_ms}, {orig_end_ms}] ms'
        raise ValueError(message)

    indices = [f.index for f in selected]

    for layer in layers:
        if layer >= len(hidden_states):
            message = f'layer {layer} out of range '
            message += f'(model has {len(hidden_states)} layers)'
            raise ValueError(message)
        hs = hidden_states[layer]
        data = hs[0, indices, :] if hs.ndim == 3 else hs[indices, :]
        store.put(phraser_key, collar, model_name, 'hidden_state', layer,
            data, tags=tags)


def _build_embeddings(arrays, layers_list, single_layer,
    frame_aggregation, echoframe_keys):
    '''Assemble an Embeddings instance from per-layer arrays.'''
    processed = [_apply_aggregation(arr, frame_aggregation) for arr in arrays]

    if single_layer:
        data = processed[0]
        dims = ('embed_dim',) if frame_aggregation else ('frames', 'embed_dim')
        return Embeddings(data=data, dims=dims, layers=None,
            echoframe_keys=echoframe_keys,
            frame_aggregation=frame_aggregation)

    data = np.stack(processed, axis=0)
    dims = (('layers', 'embed_dim') if frame_aggregation else
        ('layers', 'frames', 'embed_dim'))
    return Embeddings(data=data, dims=dims, layers=tuple(layers_list),
        echoframe_keys=echoframe_keys,
        frame_aggregation=frame_aggregation)


def _apply_aggregation(data, frame_aggregation):
    '''Aggregate frame data of shape (n_frames, embed_dim).'''
    if frame_aggregation is None:
        return data
    if frame_aggregation == 'mean':
        return np.mean(data, axis=0)
    if frame_aggregation == 'centroid':
        return data[len(data) // 2]
    raise ValueError(f'unknown aggregation: {frame_aggregation!r}')


def _codebook_artifacts_missing(store, phraser_key, collar, model_name):
    return (not store.exists(phraser_key, collar, model_name,
        'codebook_indices', 0) or not store.exists(phraser_key, collar,
        model_name, 'codebook_matrix', 0))


def _compute_and_store_codebook_indices(audio_filename, col_start_ms,
    col_end_ms, orig_start_ms, orig_end_ms, collar, model_name,
    compute_model, model_architecture, phraser_key, store, gpu, tags):
    outputs = to_vector.filename_to_vector(audio_filename,
        start=_ms_to_s(col_start_ms), end=_ms_to_s(col_end_ms),
        model=compute_model, gpu=gpu, numpify_output=True)
    frames = frame.make_frames_from_outputs(outputs,
        start_time=_ms_to_s(col_start_ms))
    selected = frames.select_frames(_ms_to_s(orig_start_ms),
        _ms_to_s(orig_end_ms), percentage_overlap=100)
    if not selected:
        message = 'no frames fully within '
        message += f'[{orig_start_ms}, {orig_end_ms}] ms'
        raise ValueError(message)
    frame_indices = [item.index for item in selected]
    artifacts = to_vector.filename_to_codebook_artifacts(
        audio_filename, start=_ms_to_s(col_start_ms), end=_ms_to_s(col_end_ms),
        model=compute_model, gpu=gpu)
    if artifacts.model_architecture != model_architecture:
        raise ValueError('codebook helper returned unexpected architecture')
    selected_indices = np.asarray(artifacts.indices)[frame_indices]
    store.put(phraser_key, collar, model_name, 'codebook_indices', 0,
        selected_indices, tags=tags)
    store.put(phraser_key, collar, model_name, 'codebook_matrix', 0,
        np.asarray(artifacts.codebook_matrix), tags=tags)


def _load_codebook_indices_object(store, phraser_key, collar, model_name,
    model_architecture):
    indices_metadata = store.find_one(phraser_key, collar, model_name,
        'codebook_indices', 0)
    matrix_metadata = store.find_one(phraser_key, collar, model_name,
        'codebook_matrix', 0)
    if indices_metadata is None or matrix_metadata is None:
        raise ValueError('stored codebook artifacts were not found')
    data = store.load(phraser_key, collar, model_name, 'codebook_indices', 0)
    if model_architecture is None:
        model_architecture = _infer_architecture_from_stored_matrix(
            store.load(phraser_key, collar, model_name, 'codebook_matrix', 0))
    return Codebook(echoframe_keys=(indices_metadata.entry_id,),
        data=data, model_architecture=model_architecture,
        codebook_matrix_echoframe_keys=(matrix_metadata.entry_id,)
        ).bind_store(store)


def _resolve_codebook_model_architecture(model):
    model_type = to_vector_model_registry.model_to_type(model)
    if model_type == 'spidr':
        return 'spidr'
    if model_type in {'wav2vec2', 'wav2vec2-pretraining'}:
        return 'wav2vec2'
    raise ValueError('codebook indices currently support wav2vec2 and spidr')


def _infer_architecture_from_stored_matrix(codebook_matrix):
    matrix = np.asarray(codebook_matrix)
    if matrix.ndim == 3:
        return 'spidr'
    return 'wav2vec2'


def _make_echoframe_key(phraser_key, collar, model_name, layer):
    md = EchoframeMetadata(phraser_key=phraser_key, collar=collar,
        model_name=model_name, output_type='hidden_state', layer=layer)
    return md.echoframe_key


def _ms_to_s(value):
    return int(value) / 1000.0


def _require_loaded_model(model, output_label):
    if model is None:
        raise ValueError(
            f'model is required as a loaded model object when '
            f'{output_label} must be computed')
    if isinstance(model, (str, Path)):
        raise TypeError(
            'model must be a loaded model object; string and path values '
            'are not accepted for compute paths')
    return model
