'''Thin forwarding wrappers for echoframe segment feature retrieval.'''

from echoframe import segment_features


def get_embeddings(segment, layers, collar=500, model_name='wav2vec2',
    frame_aggregation=None, model=None, store=None,
    store_root='echoframe', gpu=False, tags=None):
    '''Forward to ``echoframe.segment_features.get_embeddings``.'''
    return segment_features.get_embeddings(segment, layers, collar=collar,
        model_name=model_name, frame_aggregation=frame_aggregation,
        model=model, store=store, store_root=store_root, gpu=gpu,
        tags=tags)


def get_embeddings_batch(segments, layers, collar=500,
    model_name='wav2vec2', frame_aggregation=None, model=None,
    store=None, store_root='echoframe', gpu=False, tags=None):
    '''Forward to ``echoframe.segment_features.get_embeddings_batch``.'''
    return segment_features.get_embeddings_batch(segments, layers,
        collar=collar, model_name=model_name,
        frame_aggregation=frame_aggregation, model=model, store=store,
        store_root=store_root, gpu=gpu, tags=tags)


def get_codebook_indices(segment, collar=500, model_name='wav2vec2',
    model=None, store=None, store_root='echoframe', gpu=False, tags=None):
    '''Forward to ``echoframe.segment_features.get_codebook_indices``.'''
    return segment_features.get_codebook_indices(segment, collar=collar,
        model_name=model_name, model=model, store=store,
        store_root=store_root, gpu=gpu, tags=tags)


def get_codebook_indices_batch(segments, collar=500,
    model_name='wav2vec2', model=None, store=None,
    store_root='echoframe', gpu=False, tags=None):
    '''Forward to ``echoframe.segment_features.get_codebook_indices_batch``.'''
    return segment_features.get_codebook_indices_batch(segments,
        collar=collar, model_name=model_name, model=model, store=store,
        store_root=store_root, gpu=gpu, tags=tags)
