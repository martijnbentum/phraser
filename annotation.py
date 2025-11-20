import file_handler
import locations
from progressbar import progressbar
import speaker
import time
import pickle

speakers = speaker.speakers
file_mapper = file_handler.file_mapper

class Metadata:
    def __init__(self, use_pickle = True):
        self.use_pickle = use_pickle
        self._load_annotations()
        self.speakers = speakers
        self.file_mapper = file_mapper

    def __repr__(self):
        m = 'CGN Metadata with annotations'
        return m

    def _load_annotations(self):
        use_pickle = self.use_pickle
        self.ort= Annotation(annotation_type = 'ort', use_pickle = use_pickle)
        self.awd= Annotation(annotation_type = 'awd', use_pickle = use_pickle)
        self.fon= Annotation(annotation_type = 'fon', use_pickle = use_pickle)

    @property
    def ort_segments(self):
        return self.ort.segments

    @property
    def awd_segments(self):
        return self.awd.segments

    @property
    def fon_segments(self):
        return self.fon.segments

    @property
    def ort_speakers(self):
        return self.ort.speaker_dict
        

class Annotation:
    def __init__(self, annotation_type = 'ort', use_pickle = True):
        filename = getattr(locations, f'{annotation_type}_annotation_pickle')
        self.pickle_filename = filename
        if use_pickle:loaded = self._load_pickle(annotation_type)
        if not use_pickle or not loaded: self._set_info(annotation_type)

    def __repr__(self):
        m = f'Annotation ({self.annotation_type}) '
        m += f', {len(self.textgrids)} textgrids, {len(self.segments)} segments'
        return m


    def _set_info(self, annotation_type):
        print(f'loading from {annotation_type} annotation files')
        self.annotation_type = annotation_type
        d = getattr(file_mapper, annotation_type)
        self.cgn_ids = list(d.keys())
        self._load_textgrids()
        #self.make_speaker_dict()
        self._save_pickle()

    def _load_textgrids(self):
        self.textgrids= []
        self.segments = []
        self.speakers = []
        for cgn_id in progressbar(self.cgn_ids):
            textgrid= Textgrid(cgn_id = cgn_id, parent = self,
                annotation_type = self.annotation_type)
            self.textgrids.append(textgrid)
            self.segments.extend(textgrid.segments)
            for speaker in textgrid.speakers:
                if speaker not in self.speakers:
                    self.speakers.append(speaker)

    def _load_pickle(self, annotation_type):
        if not self.pickle_filename.exists(): return False
        filename = self.pickle_filename
        start = time.time()
        m=f'Loading pickled {annotation_type} annotations from {filename}'
        print(m)
        instance = file_handler.load_pickled_annotations(self, annotation_type)
        self.__dict__.update(instance.__dict__)
        end = time.time()
        print(f'Loaded pickled annotations in {end - start:.2f} seconds')
        return True

    def _save_pickle(self):
        print(f'Saving pickled annotations to {self.pickle_filename}')
        with open(self.pickle_filename, 'wb') as f:
            pickle.dump(self, f)

    def make_speaker_dict(self):
        self.speaker_dict = {}
        for textgrid in self.textgrids:
            for speaker in textgrid.speakers:
                speaker_id = speaker.speaker_id
                if speaker_id not in self.speaker_dict:
                    self.speaker_dict[speaker_id] = []
                self.speaker_dict[speaker_id].append(textgrid)

    @property
    def awd_word_segments(self):
        if self.annotation_type != 'awd': return []
        segments = []
        for textgrid in self.textgrids:
            segments.extend(textgrid.awd_word_segments)
        return segments

    @property
    def awd_word_fon_segments(self):
        if self.annotation_type != 'awd': return []
        segments = []
        for textgrid in self.textgrids:
            segments.extend(textgrid.awd_word_fon_segments)
        return segments

    @property
    def awd_seg_fon_segments(self):
        if self.annotation_type != 'awd': return []
        segments = []
        for textgrid in self.textgrids:
            segments.extend(textgrid.awd_seg_fon_segments)
        return segments
        
class Textgrid:
    def __init__(self, identifier = None, cgn_id = None, number = None,
        annotation_type = 'ort', parent = None):
        if identifier == cgn_id == number == None:
            m = 'Either identifier,number or cgn_id must be provided'
            raise ValueError(m)
        if number: 
            d = file_mapper.number_to_cgn_id
            if number in d:cgn_id = d[number]
            else: raise ValueError(f'Number {number} not found in file map')
        if identifier is None: 
            identifier = f'{cgn_id}_{annotation_type}_annotation'
        if cgn_id is None: cgn_id = identifier.split('_')[0]
        self.cgn_id = cgn_id
        self.audio_filename = file_mapper.cgn_id_to_audio_filename(cgn_id)
        self.component = file_mapper.cgn_id_to_component(cgn_id)
        self.number = cgn_id.split('.')[0][2:]
        self.annotation_type = annotation_type
        self.parent = parent
        self._load_textgrid()
        self._add_tier_names_as_attributes()

    def __repr__(self):
        m = f'TG ({self.annotation_type}): {self.cgn_id}'
        return m

    def __str__(self):
        m = f'Textgrid ({self.annotation_type}): {self.cgn_id}\n'
        m += f'Audio file: {self.audio_filename}\n'
        m += f'Component: {self.component}\n'
        m += f'Number of tiers: {len(self.tiers)}\n'
        m += f'Speakers: {", ".join([s.speaker_id for s in self.speakers])}\n'
        m += f'Tiers:\n'
        for tier in self.tiers:
            m += f'  - {tier.name} ({tier.n_segments} segments, '
            m += f'duration {tier.duration:.2f} s)\n'
        return m

    def __eq__(self, other):
        return self.cgn_id == other.cgn_id and \
            self.annotation_type == other.annotation_type

    def _load_textgrid(self):
        textgrid = file_mapper.cgn_id_to_textgrid(
            cgn_id = self.cgn_id,
            annotation_type = self.annotation_type)
        self.tiers = []
        self.segments = []
        self.tier_names = textgrid.getNames()
        self.speaker = self._load_speakers()
        for tier_name, tier in zip(self.tier_names, textgrid.tiers):
            spk = self.tier_name_to_speaker(tier_name)
            tier = Tier(tier_name, tier.intervals, speaker = spk, 
                parent=self)
            self.tiers.append(tier)
            if spk: self.segments.extend(tier.segments)

    def _load_speakers(self):
        self.speakers = []
        for name in self.tier_names:
            if name in speakers.speaker_ids:
                speaker = speakers.get_speaker(speaker_id = name)
                self.speakers.append(speaker)

    def tier_name_to_speaker(self, tier_name):
        for speaker in self.speakers:
            if speaker.speaker_id in tier_name :
                return speaker
        return None

    def _add_tier_names_as_attributes(self):
        for tier in self.tiers:
            attr_name = tier.name.replace(' ', '_')
            setattr(self, attr_name, tier)

    @property
    def awd_word_segments(self):
        if self.annotation_type != 'awd': return []
        segments = []
        for segment in self.segments:
            if '_' not in segment.name:
                segments.append(segment)
        return segments

    @property
    def awd_word_fon_segments(self):
        if self.annotation_type != 'awd': return []
        segments = []
        for segment in self.segments:
            if '_FON' in segment.name:
                segments.append(segment)
        return segments

    @property
    def awd_seg_fon_segments(self):
        if self.annotation_type != 'awd': return []
        segments = []
        for segment in self.segments:
            if '_SEG' in segment.name:
                segments.append(segment)
        return segments

            
class Tier:
    def __init__(self, name, intervals, speaker, parent):
        self.name = name
        self.intervals = intervals
        self.parent = parent
        self.speaker = speaker
        self.cgn_id = self.parent.cgn_id
        self.annotation_type = self.parent.annotation_type
        self.audio_filename = self.parent.audio_filename
        self.component = self.parent.component
        self._set_info()

    def __repr__(self):
        m = f'Tier ({self.annotation_type}): {self.name}'
        m += f', comp {self.component}, {len(self.segments)} segments'
        m += f', duration {self.duration}'
        if self.speaker:
            m += f', speaker {self.speaker.speaker_id}'
        return m

    def __eq__(self,other):
        return self.name == other.name and \
            self.cgn_id == other.cgn_id and \
            self.annotation_type == other.annotation_type

    def _set_info(self):
        self.start_time = self.intervals[0].minTime
        self.end_time = self.intervals[-1].maxTime
        self.duration = self.end_time - self.start_time
        self.segments = []
        self._other_segments = []
        for index, interval in enumerate(self.intervals):
            segment = Segment(name = self.name, index = index,
                start_time = interval.minTime,
                end_time = interval.maxTime, label = interval.mark,
                speaker = self.speaker, parent = self)
            if segment.ok: self.segments.append(segment)
            else: self._other_segments.append(segment)
        self.n_segments = len(self.segments)
        self.special = self.name in ['BACKGROUND', 'COMMENT']
        if self.special and self.speaker:
            raise ValueError('SPECIAL tiers should not have speakers')
        if self.speaker:
            if self.annotation_type == 'awd': 
                key = self.make_speaker_tier_key_for_awd()
            else: key = self.annotation_type
            if self not in self.speaker.tiers[key]:
                self.speaker.tiers[key].append(self)

    def make_speaker_tier_key_for_awd(self):
        if '_FON' in self.name:
            return 'awd_word_fon'
        if '_SEG' in self.name:
            return 'awd_seg_fon'
        if '_' in self.name: 
            raise ValueError(f'Unexpected AWD tier name {self.name}')
        return 'awd_word'

            
class Segment:
    def __init__(self, name, index, start_time, end_time, label, speaker, 
        parent):
        self.name = name
        self.index = index
        self.start_time = start_time
        self.end_time = end_time
        self.duration = end_time - start_time
        self.label = label
        self.parent = parent
        self.speaker = speaker
        self.cgn_id = parent.cgn_id
        self.annotation_type = parent.annotation_type
        self.audio_filename = parent.audio_filename
        self.component = parent.component
        self.ok = bool(self.label)

    def __repr__(self):
        m = f'Seg({round(self.start_time,3):<7}, '
        m += f'{self.label:<75}, '
        m += f'{self.duration:.2f} s) ' 
        return m

    def __str__(self):
        m = f'Segment ({self.annotation_type}): {self.cgn_id}\n'
        m += f'Label: {self.label}\n'
        m += f'Tier: {self.name}\n'
        if self.speaker:
            m += f'Speaker: {self.parent.speaker.speaker_id}\n'
        m += f'Duration: {self.duration:.2f} '
        m += f' ({self.start_time:.3f} - {self.end_time:.3f}) seconds'
        return m

    def __eq__(self, other):
        return self.cgn_id == other.cgn_id and \
            self.annotation_type == other.annotation_type and \
            self.name == other.name and \
            self.index == other.index 

    def previous_segment(self):
        tier = self.parent
        if self.index - 1 >= 0:
            return tier.segments[self.index - 1]
        else:
            return None

    def next_segment(self):
        tier = self.parent
        if self.index + 1 < len(tier.segments):
            return tier.segments[self.index + 1]
        else:
            return None



    
