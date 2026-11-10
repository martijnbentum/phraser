'''Domain objects: recordings, speakers, and time-aligned segments.'''

from .audio import Audio
from .marker import Marker
from .segment import Segment
from .segment_hierarchy import Phone, Phrase, Syllable, Word
from .speaker import Speaker

__all__ = ['Audio', 'Marker', 'Phone', 'Phrase', 'Segment', 'Speaker',
    'Syllable', 'Word']
