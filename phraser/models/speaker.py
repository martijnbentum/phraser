from .. import query
from .. import utils
from ..storage import key_helper
from ..utils import R, B, GR, RE
from . import store_access


class Speaker:
    IDENTITY_FIELDS= {'name', 'dataset'}
    DB_FIELDS = {'name', 'dataset','identifier'}
    METADATA_FIELDS = {'gender_code', 'age', 'language', 'dialect', 'region'}

    flags = 0
    gender_code = 9
    age = 0
    dataset = ''
    dialect = ''
    region = ''
    language = ''

    def __init__(self, name =None, dataset = None, save=False, overwrite=False,
        store=None, **kwargs):
        self.object_type = self.__class__.__name__
        self.name = name
        self.dataset = dataset
        self.identifier = key_helper.make_identifier()
        self.overwrite = overwrite
        self._store = store

        if 'gender' in kwargs:
            self.gender_code = utils.gender_dict[kwargs.pop('gender')]
        utils.check_init_fields(self, kwargs, self.METADATA_FIELDS)
        for k, v in kwargs.items():
            setattr(self, k, v)

        if save:
            self.save(overwrite=overwrite)

    def __repr__(self):
        if len(self.name) > 12: name = self.name[:9] + '...'
        else: name = self.name

        m = f'{R}Speaker{RE} {B}name {RE}{name:<20} | '
        m += f'{GR}ID={self.identifier.hex()}{RE}'
        return m

    def __eq__(self, other):
        if not isinstance(other, Speaker):
            return False
        return self.identifier == other.identifier

    def __hash__(self):
        return hash(self.identifier)

    def __contains__(self, key):
        return hasattr(self, key)

    def add_audio(self, audio, update_database=True):
        if not hasattr(self, '_audios'): self._audios = []
        if audio not in self._audios:
            self._audios.append(audio)
        if update_database:
            self.store.DB.write_speaker_audio_link(self, audio)

    def gender(self):
        return utils.reverse_gender_dict[self.gender_code]

    @property
    def audios(self):
        if hasattr(self, '_audios'): return self._audios
        audio_keys = self.store.DB.speaker_to_audio_keys(self)
        self._audios = self.store.load_many(audio_keys)
        return self._audios

    @property
    def phrase_keys(self):
        if hasattr(self, '_phrase_keys'): return self._phrase_keys
        self._phrase_keys = []
        for audio in self.audios:
            for phrase in audio.phrases:
                if phrase.speaker_id == self.identifier:
                    self._phrase_keys.append(phrase.key)
        return self._phrase_keys

    @property
    def phrases(self):
        """Return all phrases across all audios for this speaker."""
        if hasattr(self, '_phrases'): return self._phrases
        self._phrases = self.store.load_many(self.phrase_keys)
        return self._phrases

    @property
    def words(self):
        """Return all words across all phrases for this speaker."""
        words = []
        for phrase in self.phrases:
            for word in phrase.words:
                words.append(word)
        return words

    @property
    def syllables(self):
        """Return all syllables across all phrases for this speaker."""
        syllables = []
        for phrase in self.phrases:
            for syllable in phrase.syllables:
                syllables.append(syllable)
        return syllables

    @property
    def phones(self):
        """Return all phones across all phrases for this speaker."""
        phones = []
        for phrase in self.phrases:
            for phone in phrase.phones:
                phones.append(phone)
        return phones

    def filter_phrases(self, **filters):
        '''Return this speaker's phrases matching filters.'''
        return query.filter_objects(self.phrases, **filters)

    def filter_words(self, **filters):
        '''Return this speaker's words matching filters.'''
        return query.filter_objects(self.words, **filters)

    def filter_syllables(self, **filters):
        '''Return this speaker's syllables matching filters.'''
        return query.filter_objects(self.syllables, **filters)

    def filter_phones(self, **filters):
        '''Return this speaker's phones matching filters.'''
        return query.filter_objects(self.phones, **filters)

    @property
    def key(self):
        """Return the LMDB key for this segment."""
        return key_helper.instance_to_key(self)

    def save(self, overwrite=False, fail_gracefully=False):
        self.store.save(self, overwrite=overwrite,
            fail_gracefully=fail_gracefully)

    def delete(self):
        self.store.delete(self.key)

    @property
    def store(self): return store_access.get_store(self)

    @property
    def exists_in_db(self): return store_access.exists_in_db(self)
