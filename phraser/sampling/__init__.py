'''Explicit sampling entry points for Phraser recordings and annotations.'''

from . import cgn
from .lexicon import Entry, Lexicon
from .lexicon import build_phone_lexicon, build_phrase_lexicon
from .lexicon import build_syllable_lexicon, build_word_lexicon
from .neighbours import get_neighbours, has_neighbours, has_overlap
from .neighbours import select_words_with_neighbours_in_audio
from .neighbours import select_words_with_neighbours_in_phrase
from .sample_audios import sample_audios
from .sample_segments import sample_phone_tokens, sample_phrase_tokens
from .sample_segments import sample_syllable_tokens, sample_word_tokens
from .sample_speakers import sample_speakers
from .selection_report import InsufficientSampleError
from .selection_report import SelectionReport, SelectionStage

__all__ = ['Entry', 'InsufficientSampleError', 'Lexicon', 'SelectionReport',
    'SelectionStage', 'build_phone_lexicon', 'build_phrase_lexicon',
    'build_syllable_lexicon', 'build_word_lexicon', 'cgn', 'get_neighbours',
    'has_neighbours', 'has_overlap', 'sample_audios', 'sample_phone_tokens',
    'sample_phrase_tokens', 'sample_speakers', 'sample_syllable_tokens',
    'sample_word_tokens', 'select_words_with_neighbours_in_audio',
    'select_words_with_neighbours_in_phrase']
