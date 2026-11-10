'''The speech segment levels: Phrase > Word > Syllable > Phone.'''

from ..phonology import phone_features
from .. import query
from .. import utils
from ..storage.key_helper import EMPTY_ID
from .segment import Segment


class Phrase(Segment):
    IDENTITY_FIELDS = {'audio_id', 'speaker_id', 'start'}
    METADATA_FIELDS = {'overlap_code', 'filename'}
    filename = ''

    @property
    def all_keys(self):
        return [item.key for item in self.items]

    @property
    def items(self):
        '''The flattened tree: this phrase followed by all its
        descendants in depth-first order. Works on staged trees.'''
        return (self, *self.iter_descendants())

    def validate_tree(self):
        '''Check this phrase and its staged descendants are saveable:
        one speaker across the whole tree, and no persisted segment
        changing its audio.'''
        for item in self.items:
            item._validate_for_save()
            if item.speaker_id != self.speaker_id:
                m = f'{item.object_type} speaker_id does not match its '
                m += 'phrase; reassign the speaker on the whole tree.'
                raise ValueError(m)

    def delete(self):
        '''Delete this phrase and all its descendants from the database.'''
        all_keys = self.all_keys
        self.store.delete_many(all_keys)

    @property
    def phrase_start(self):
        return self.start

    @property
    def phrase_id(self):
        return self.identifier

    @property
    def words(self):
        '''Return all words in this phrase.'''
        return self.children

    @property
    def syllables(self):
        '''Return all syllables in this phrase.'''
        return list(self.iter_descendants_of_type(Syllable))

    @property
    def phones(self):
        '''Return all phones in this phrase.'''
        return list(self.iter_descendants_of_type(Phone))

    def filter_words(self, **filters):
        '''Return this phrase's words matching filters.'''
        return query.filter_objects(self.words, **filters)

    def filter_syllables(self, **filters):
        '''Return this phrase's syllables matching filters.'''
        return query.filter_objects(self.syllables, **filters)

    def filter_phones(self, **filters):
        '''Return this phrase's phones matching filters.'''
        return query.filter_objects(self.phones, **filters)


class Word(Segment):
    METADATA_FIELDS = {'overlap_code', 'ipa'}
    ipa = ''

    @property
    def phrase_start(self):
        return self.parent_start

    @property
    def phrase_id(self):
        return self.parent_id

    @property
    def syllables(self):
        '''Return all syllables in this word.'''
        return list(self.iter_descendants_of_type(Syllable))

    @property
    def phones(self):
        '''Return all phones in this word.'''
        return list(self.iter_descendants_of_type(Phone))

    def filter_syllables(self, **filters):
        '''Return this word's syllables matching filters.'''
        return query.filter_objects(self.syllables, **filters)

    def filter_phones(self, **filters):
        '''Return this word's phones matching filters.'''
        return query.filter_objects(self.phones, **filters)

class Syllable(Segment):
    METADATA_FIELDS = {'overlap_code', 'stress_code'}
    phrase_id = EMPTY_ID
    phrase_start = 0
    stress_code = 9

    @property
    def stress(self):
        return utils.reverse_stress_dict.get(self.stress_code, 'unknown')

    @property
    def phones(self):
        '''Return all phones in this syllable.'''
        return self.children

    def _phones_at(self, position):
        return [p for p in self.phones if p.position == position]

    @property
    def onset(self):
        '''Phones in the syllable onset (empty until positions assigned).'''
        return self._phones_at('onset')

    @property
    def nucleus(self):
        '''Phones in the syllable nucleus (empty until positions assigned).'''
        return self._phones_at('nucleus')

    @property
    def coda(self):
        '''Phones in the syllable coda (empty until positions assigned).'''
        return self._phones_at('coda')

    @property
    def word(self):
        '''Return the parent word of this syllable.'''
        return self.parent

    def filter_phones(self, **filters):
        '''Return this syllable's phones matching filters.'''
        return query.filter_objects(self.phones, **filters)

class Phone(Segment):
    METADATA_FIELDS = {'overlap_code', 'position_code'}

    phrase_id = EMPTY_ID
    phrase_start = 0
    position_code = 9      # stored int: 1=onset 2=nucleus 3=coda, 9=unassigned

    @property
    def position(self):
        '''Syllable position string from the stored position_code
        ('unknown' if unassigned).'''
        return utils.reverse_position_dict.get(self.position_code, 'unknown')

    @position.setter
    def position(self, value):
        self.position_code = utils.position_dict.get(value, 9)

    @property
    def linguistic_features(self):
        '''Static IPA reference info for this phone's label (articulatory
        descriptors + binary distinctive-feature matrix), or None if the
        label is not a known IPA symbol.

        NB: the binary distinctive-feature matrix is verified against the
        panphon reference (~98.4% agreement); a few contested features
        (laryngeals, central/low vowels) reflect convention choices - see
        phone_features. The 'type' field and articulatory descriptors are
        reliable.'''
        return phone_features.get_phone_features(self.label)

    @property
    def type(self):
        '''Phone type ('vowel' or 'consonant') for this phone's label, or
        None if the label is not a known IPA symbol.'''
        info = phone_features.get_phone_features(self.label)
        return info['type'] if info else None

    @property
    def linguistic_features_vector(self):
        '''The binary distinctive-feature matrix of ``linguistic_features``
        (its ``features`` sub-dict, not the articulatory descriptors) as a
        numeric tuple (+1/-1/0, 0 = not applicable) in
        linguistic_features_names order, or None for an unknown label.
        Distinct from neural embeddings (see ``embedding``).'''
        return phone_features.get_feature_vector(self.label)

    @property
    def linguistic_features_names(self):
        '''Canonical feature names, positionally aligned with
        linguistic_features_vector.'''
        return phone_features.FEATURE_ORDER

    @property
    def stress(self):
        '''Suprasegmental stress for this phone, derived from its parent
        syllable ('unstressed'/'primary'/'secondary'/'unknown'). It is a
        syllable-level property, so every phone in the syllable - including
        consonants - reports the same value.'''
        syllable = self.syllable
        return syllable.stress if syllable else 'unknown'

    @property
    def syllable(self):
        '''Return the parent syllable of this phone.'''
        return self.parent

    @property
    def word(self):
        '''Return the parent word of this phone.'''
        if self.parent is None: return None
        return self.parent.parent


Phrase.child_class = Word
Word.child_class = Syllable
Word.parent_class = Phrase
Syllable.child_class = Phone
Syllable.parent_class = Word
Phone.child_class = None
Phone.parent_class = Syllable
