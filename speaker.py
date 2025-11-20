import file_handler


def load_speaker(speaker_id = None, number = None, speaker_file = None,
    language = None):
    if speaker_id: return Speaker(speaker_id = speaker_id)
    if number and language is None: 
        raise ValueError('If number is provided, language must also be provided')
    spids = file_handler.load_speaker_ids()
    pid = str(number).zfill(5) 
    spids = [x for x in spids if pid in x]
    speaker_id = speaker_number_to_id(number, language)
    if speaker_id not in spids:
        m = f'Speaker ID {speaker_id} not found in CGN, '
        m += f'found speaker IDs: {", ".join(spids)} with number {number}'
        raise ValueError(m)
    print(f'Loading speaker {speaker_id}, number {number}, {spids}')
    return Speaker(speaker_id = speaker_id, speaker_file = speaker_file)

def load_all_speakers():
    speaker_file= file_handler.load_speaker_file()
    header, data = speaker_file
    speakers = []
    for line in data:
        speaker_id = line[4]
        speaker = Speaker(speaker_id, speaker_file)
        speakers.append(speaker)
    return speakers

class Speakers:
    def __init__(self):
        self.speakers = load_all_speakers()
        self.speaker_ids = [s.speaker_id for s in self.speakers]

    def __repr__(self):
        m = f'Speakers collection with {len(self.speakers)} speakers'
        return m

    def get_speaker(self, speaker_id = None, number = None, language = None):
        if speaker_id == number == None:
            raise ValueError('Either speaker_id or number must be provided')
        if number and language is None:
            m = 'If number is provided, language must also be provided'
            raise ValueError(m)
        speaker_id = speaker_id or speaker_number_to_id(number, language)
        for speaker in self.speakers:
            if speaker_id and speaker.speaker_id == speaker_id:
                return speaker

    def dutch(self, selection = None):
        if selection is None: selection = self.speakers
        output = []
        for speaker in selection:
            if speaker.dutch:
                output.append(speaker)
        return output

    def flemish(self, selection = None):
        if selection is None: selection = self.speakers
        output = []
        for speaker in selection:
            if speaker.flemish:
                output.append(speaker)
        return output

    def males(self, selection = None):
        if selection is None: selection = self.speakers
        output = []
        for speaker in selection:
            if speaker.sex == 'male':
                output.append(speaker)
        return output

    def females(self, selection = None):
        if selection is None: selection = self.speakers
        output = []
        for speaker in selection:
            if speaker.sex == 'female':
                output.append(speaker)
        return output
        
    def age_range(self, min_age = None, max_age = None, selection = None):
        if selection is None: selection = self.speakers
        if min_age is None and max_age is None:
            return selection 
        output = []
        for speaker in selection:
            if speaker.age is None: continue
            if min_age and speaker.age < min_age: continue
            if max_age and speaker.age > max_age: continue
            output.append(speaker)
        return output

    def filter(self, language = None, sex =None, min_age = None, max_age = None):
        selection = self.speakers
        if language:
            selection = getattr(self, language)()
        if sex:
            selection = getattr(self, sex + 's')(selection)
        if min_age or max_age:
            selection = self.age_range(min_age, max_age, selection)
        return selection
        

class Speaker:
    def __init__(self, speaker_id, speaker_file= None,parent = None):
        self.speaker_id = speaker_id
        self.number = int(speaker_id[1:])
        self.parent = parent
        self.speaker_file = speaker_file
        if speaker_id[0] == 'N': self.dutch = True
        else: self.dutch = False
        if speaker_id[0] == 'V': self.flemish = True
        else: self.flemish = False
        self._load_info()
    
    def __repr__(self):
        m = f'Speaker {self.speaker_id}'
        if self.sex:m += f' {self.sex}'
        if self.age: m += f' {self.age}'
        return m

    def __eq__(self, other):
        return self.speaker_id == other.speaker_id

    def _load_info(self):
        self.info = file_handler.load_speaker_info(self.speaker_id, 
            self.speaker_file)
        try:self.birth_year = int(self.info['birthYear'])
        except: 
            self.birth_year = None
            self.age = None
        else: self.age = 2000 - self.birth_year
        if self.info['sex'] == 'sex1': self.sex = 'male'
        elif self.info['sex'] == 'sex2': self.sex = 'female'
        else: self.sex = None
        self.tiers= {'ort':[], 'awd_word':[], 'awd_word_fon': [], 
            'awd_seg_fon':[],'fon':[]}

    @property
    def has_tiers(self):
        for tierlist in self.tiers.values():
            if tierlist: return True
        return False

    @property
    def available_tiers(self):
         for tier_type, tierlist in self.tiers.items():
             if tierlist:
                 yield tier_type       
        
    @property
    def ort_tiers(self):
        return self.tiers['ort']

    @property
    def awd_word_tiers(self):
        return self.tiers['awd_word']

    @property
    def awd_word_fon_tiers(self):
        return self.tiers['awd_word_fon']

    @property
    def awd_seg_fon_tiers(self):
        return self.tiers['awd_seg_fon']

    @property
    def fon_tiers(self):
        return self.tiers['fon']

    @property
    def ort_segments(self):
        segments = []
        for tier in self.ort_tiers:
            segments.extend(tier.segments)
        return segments

    @property
    def awd_word_segments(self):
        segments = []
        for tier in self.awd_word_tiers:
            segments.extend(tier.segments)
        return segments

    @property
    def awd_word_fon_segments(self):
        segments = []
        for tier in self.awd_word_fon_tiers:
            segments.extend(tier.segments)
        return segments

    def awd_seg_fon_segments(self):
        segments = []
        for tier in self.awd_seg_fon_tiers:
            segments.extend(tier.segments)
        return segments

    @property
    def fon_segments(self):
        segments = []
        for tier in self.fon_tiers:
            segments.extend(tier.segments)
        return segments

        
def speaker_number_to_id(number, language):
    if language.lower() in ['n','d','dutch']:
        speaker_id = f'N{number:05d}'
    elif language.lower() in ['v','f', 'flemish']:
        speaker_id = f'V{number:05d}'
    else:
        raise ValueError('Language must be either dutch (n,d) or flemish (v,f)')
    return speaker_id


speakers = Speakers()
