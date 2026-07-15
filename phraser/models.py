from . import key_helper
from . import query
from . import utils
from .segment import Segment, Phrase, Word, Syllable, Phone
from .store import ClosedStoreError, UnboundStoreError
from .utils import R, B, GR, RE


class Audio:
    IDENTITY_FIELDS= {'filename'}
    METADATA_FIELDS = {'sample_rate', 'duration', 'n_channels', 'dataset',
        'language', 'dialect'}
    DB_FIELDS = {'filename', 'identifier'}

    dialect = ''
    language = ''
    dataset = ''
    n_channels = 0
    sample_rate = 0

    def __init__(self, filename = None,  save=False, overwrite=False,
        store=None, **kwargs):
        self.object_type = self.__class__.__name__
        self.filename = filename
        self.identifier = key_helper.make_identifier()
        self.overwrite = overwrite
        self._store = store

        self._set_kwargs(**kwargs)

        if save:
            self.save(overwrite=overwrite)

    def __repr__(self):
        if len(self.filename) > 20:
            filename = self.filename.split('/')[-1]
            if len(filename) > 20: filename = '...' + filename[-17:]
        else: filename = self.filename
        m = f'{R}Audio{RE} {B}filename {RE}{filename} '
        if hasattr(self, 'duration'):
            m += f'{B}duration {RE}{self.duration} | '
        m += f'{GR}ID={self.identifier.hex()}{RE}'
        return m

    def __eq__(self, other):
        if not isinstance(other, Audio):
            return False
        return self.filename == other.filename

    def __hash__(self):
        return hash(self.filename)

    def _set_kwargs(self, **kwargs):
        for k, v in kwargs.items():
            if k == 'duration' and not isinstance(v, int):
                raise ValueError('duration should be in milliseconds and should be int')
            setattr(self, k, v)

    def add_speaker(self, speaker, update_database=True):
        speaker.add_audio(self, update_database=update_database)

    def has_extra(self):
        if hasattr(self, 'extra') and self.extra:
            return True
        return False


    @property
    def speakers(self):
        if hasattr(self, '_speakers'): return self._speakers
        speakers = [x.speaker for x in self.phrases]
        unique_speakers = set(speakers)
        self._speakers = list(unique_speakers)
        return self._speakers

    @property
    def phrase_keys(self):
        if hasattr(self, '_phrase_keys'): return self._phrase_keys
        self._phrase_keys = list(self.store.DB.audio_id_to_child_keys(
            self.identifier))
        return self._phrase_keys

    @property
    def phrases(self):
        if hasattr(self, '_phrases'): return self._phrases
        self._phrases= self.store.load_many(self.phrase_keys)
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

    @property
    def speakers_query(self):
        """Return a query object for all phrases for this speaker."""
        return query.queryset_from_items(self.phrases, self.store)
    @property
    def words_query(self):
        """Return a query object for all words for this speaker."""
        return query.queryset_from_items(self.words, self.store)
    @property
    def syllables_query(self):
        """Return a query object for all syllables for this speaker."""
        return query.queryset_from_items(self.syllables, self.store)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, self.store)

    @property
    def key(self):
        """Return the LMDB key for this segment."""
        return key_helper.instance_to_key(self)

    def save(self, overwrite=False, fail_gracefully=False):
        self.store.save(self, overwrite=overwrite,
            fail_gracefully=fail_gracefully)

    @property
    def store(self):
        store = getattr(self, '_store', None)
        if store is None:
            name = self.__class__.__name__
            m = f'{name} is not bound to a Store. Create it with store=... '
            m += f'or Store.create_{name}().'
            raise UnboundStoreError(m)
        if store.closed:
            name = self.__class__.__name__
            m = f'{name} is bound to a closed Store. '
            m += 'Call store.open() to reopen it.'
            raise ClosedStoreError(m)
        return store

    @property
    def exists_in_db(self):
        cls = self.__class__
        lookup = {k: getattr(self, k) for k in cls.IDENTITY_FIELDS}
        existing = self.store.query_for_class(cls).get_or_none(**lookup)
        return existing is not None



class Speaker:
    IDENTITY_FIELDS= {'name', 'dataset'}
    DB_FIELDS = {'name', 'dataset','identifier'}
    METADATA_FIELDS = {'gender', 'age', 'language', 'dialect', 'region',
        'channel'}
    FIELDS = DB_FIELDS.union(METADATA_FIELDS)

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

        # Extra metadata
        extra = {}
        if 'extra' in kwargs:
            e = kwargs.pop('extra')
            if not isinstance(e, dict):
                raise ValueError("extra must be a dict")
            extra.update( e )
        for k, v in kwargs.items():
            if k in self.FIELDS:
                setattr(self, k, v)
            elif not k in extra:
                extra[k] = v
        self.extra = extra

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
        return hasattr(self, key) or key in self.extra

    def add_audio(self, audio, update_database=True):
        if not hasattr(self, '_audios'): self._audios = []
        if audio not in self._audios:
            self._audios.append(audio)
        if update_database:
            self.store.DB.write_speaker_audio_link(self, audio)

    def has_extra(self):
        if hasattr(self, 'extra') and self.extra:
            return True
        return False

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
            self._phrase_keys += audio.phrase_keys
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

    @property
    def phrases_query(self):
        """Return a query object for all phrases for this speaker."""
        return query.queryset_from_items(self.phrases, self.store)
    @property
    def words_query(self):
        """Return a query object for all words for this speaker."""
        return query.queryset_from_items(self.words, self.store)
    @property
    def syllables_query(self):
        """Return a query object for all syllables for this speaker."""
        return query.queryset_from_items(self.syllables, self.store)
    @property
    def phones_query(self):
        """Return a query object for all phones for this speaker."""
        return query.queryset_from_items(self.phones, self.store)

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
    def metadata_present(self):
        names = []
        for name in self.METADATA_FIELDS:
            if hasattr(self, name):
                names.append(name)
        return names

    @property
    def store(self):
        store = getattr(self, '_store', None)
        if store is None:
            name = self.__class__.__name__
            m = f'{name} is not bound to a Store. Create it with store=... '
            m += f'or Store.create_{name}().'
            raise UnboundStoreError(m)
        if store.closed:
            name = self.__class__.__name__
            m = f'{name} is bound to a closed Store. '
            m += 'Call store.open() to reopen it.'
            raise ClosedStoreError(m)
        return store

    @property
    def exists_in_db(self):
        cls = self.__class__
        lookup = {k: getattr(self, k) for k in cls.IDENTITY_FIELDS}
        existing = self.store.query_for_class(cls).get_or_none(**lookup)
        return existing is not None
