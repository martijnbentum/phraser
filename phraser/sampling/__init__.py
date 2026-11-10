'''Explicit sampling entry points for Phraser recordings and annotations.'''

from . import cgn
from .sample_audios import sample_audios
from .sample_segments import sample_phone_tokens, sample_phrase_tokens
from .sample_segments import sample_syllable_tokens, sample_word_tokens
from .sample_speakers import sample_speakers

__all__ = ['cgn', 'sample_audios', 'sample_phone_tokens',
    'sample_phrase_tokens', 'sample_speakers', 'sample_syllable_tokens',
    'sample_word_tokens']
