'''Echoframe segment feature retrieval: stored-embedding lookup and thin
forwarding wrappers.'''

try: from echoframe import segment_features
except ImportError:
    raise ImportError('echoframe is required for segment embedding retrieval. '
        'uv pip install git+https://git@github.com/martijnbentum/echoframe.git')

from .errors import UnboundStoreError


def get_stored_embedding(segment, model_name, layer, collar=500, store=None,
    fallback=False):
    '''Load the stored hidden-state Embedding for a segment.

    Uses the echoframe store bound to the segment's phraser store
    (via echoframe_store.attach_phraser_store), or an explicit
    store=... override. Returns an echoframe Embedding object.

    segment:     phraser segment (Phrase, Word, Syllable or Phone)
    model_name:  name of the model the embedding was stored for
    layer:       model layer index
    collar:      collar in milliseconds used when the embedding was stored
    store:       echoframe store; defaults to the bound echoframe store
    fallback:    if nothing is stored for this segment, walk ancestors
                 (e.g. phone -> syllable -> word -> phrase) and return the
                 first ancestor embedding sliced to this segment as a
                 SlicedEmbedding
    '''
    store = store or getattr(segment.store, 'echoframe_store', None)
    if store is None:
        m = 'no echoframe store bound; call '
        m += 'echoframe_store.attach_phraser_store(source_id, '
        m += 'phraser_store) or pass store=...'
        raise UnboundStoreError(m)
    try:
        return store.phraser_key_to_embedding(segment.key, model_name, layer,
            collar=collar)
    except ValueError as e:
        if not fallback:
            m = f'no stored embedding for {segment.object_type} '
            m += f'(model_name={model_name}, layer={layer}, '
            m += f'collar={collar}); try fallback=True to use the '
            m += 'nearest ancestor embedding'
            raise ValueError(m) from e
    return _ancestor_embedding(segment, model_name, layer, collar, store)


def _ancestor_embedding(segment, model_name, layer, collar, store):
    '''Return the nearest ancestor's stored embedding sliced to segment,
    or raise if no ancestor has one.'''
    for ancestor in segment.iter_ancestors():
        try:
            parent = store.phraser_key_to_embedding(ancestor.key, model_name,
                layer, collar=collar)
        except ValueError:
            continue
        return parent.sub_embedding(segment)
    m = f'no stored embedding for {segment.object_type} or its ancestors '
    m += f'(model_name={model_name}, layer={layer}, collar={collar})'
    raise ValueError(m)



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
