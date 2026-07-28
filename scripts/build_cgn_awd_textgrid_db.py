'''Build a fresh Phraser database from original CGN ORT and AWD files.

Import this module and call build_cgn_awd_database from Python or IPython.
'''

from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass, field
import json
from pathlib import Path

from dutch_syllabifier import resyllabify_phones
from dutch_text_clean import clean
from phone_mapper.cgn import cgn_to_ipa
from progressbar import progressbar
from textgrid import TextGrid

from phraser import Store, models
from phraser import audio as audio_helper
from phraser import locations, syllable_structure, utils


ORT_NON_SPEAKER_TIERS = {'BACKGROUND', 'COMMENT'}
SKIPPED_SEGMENT_MARKS = {'[]', '#'}
FON_BOUNDARY_MARKS = {'!', '#', '[]', '=', '-', '_', ' '}
MAPPING_KEYS = sorted(cgn_to_ipa, key=len, reverse=True)
RESUME_AUDIT_COUNT = 3
cgn_corpus_dir = Path('/vol/bigdata/corpora2/CGN2')
cgn_audio_dir = cgn_corpus_dir / 'data/audio/wav'
cgn_ort_dir = Path('../data/ort/')
cgn_awd_dir = Path('../data/awd/')
cgn_db_path = Path('../data/cgn_awd_lmdb')
cgn_speaker_file = cgn_corpus_dir / 'data/meta/text/speakers.txt'
cgn_report_file = Path('../data/cgn_awd_import_report.json')


@dataclass(frozen=True)
class CgnRecordingFiles:
    '''Audio and transcription files for one CGN recording.'''

    stem: str
    audio_path: Path
    ort_path: Path
    awd_path: Path


@dataclass
class ImportReport:
    '''Bounded, JSON-serializable counters and issue examples.'''

    sample_limit: int = 20
    counts: Counter = field(default_factory=Counter)
    samples: dict = field(default_factory=dict)

    def record(self, kind, count=1, **details):
        self.counts[kind] += count
        if not details: return
        examples = self.samples.setdefault(kind, [])
        if len(examples) < self.sample_limit:
            examples.append(details)

    def to_dict(self):
        items = self.counts.items()
        counts = dict(sorted(items))
        return {'counts': counts, 'samples': self.samples}


@dataclass
class CgnImportState:
    store: Store
    report: ImportReport
    speaker_metadata: dict
    id_to_speaker: dict
    filename_to_audio: dict
    audit_stems: set


@dataclass
class _StagedCgnRecording:
    audio: object
    new_audio: bool
    id_to_speaker: dict
    new_speakers: list
    phrases: list


def build_cgn_awd_database(audio_dir=cgn_audio_dir, db_path=cgn_db_path,
    awd_dir=cgn_awd_dir, ort_dir=cgn_ort_dir,
    speaker_file=cgn_speaker_file, resume=False, strict_pairs=False,
    report_file=cgn_report_file, show_progress=True):
    '''Build the original-alignment CGN database one recording at a time.
    audio_dir:      root searched recursively for WAV files
    db_path:        target LMDB directory
    awd_dir:        original CGN AWD directory
    ort_dir:        original CGN ORT directory
    speaker_file:   CGN speakers.txt; missing entries become placeholders
    resume:         audit the final three database audios, repair missing
                    label indexes, and continue an existing database
    strict_pairs:   if False, report and skip incomplete source triples
                    if True, require identical Audio, AWD, and ORT stems
    report_file:    JSON output path; use None to disable
    show_progress:  show recording and per-recording LMDB batch progress
    '''
    report = ImportReport()
    if db_path is None: db_path = cgn_db_path
    if audio_dir is None: audio_dir = cgn_audio_dir
    if awd_dir is None: awd_dir = cgn_awd_dir
    if ort_dir is None: ort_dir = cgn_ort_dir
    if speaker_file is None: speaker_file = cgn_speaker_file
    target = validate_target_path(db_path, resume=resume)
    recordings = collect_cgn_audio_and_transcription_files(
        audio_dir, ort_dir, awd_dir, strict=strict_pairs, report=report)
    speaker_metadata = load_speaker_metadata(speaker_file, report)
    store = Store(path=target)
    state = make_cgn_import_state(
        store, report, speaker_metadata, recordings, resume=resume)
    iterable = progressbar(recordings) if show_progress else recordings
    try:
        for recording in iterable: process_cgn_recording(recording, state)
    finally:
        store.close()
        _save_report(report, report_file)
    if report.counts['recording_errors']:
        count = report.counts['recording_errors']
        message = f'{count} recording imports failed; see the import report'
        raise RuntimeError(message)
    return report


def load_textgrid(filename):
    '''Load one original CGN TextGrid.'''
    return TextGrid.fromFile(str(filename))


def _collect_files_by_stem(directory, suffix, source):
    '''Recursively index one source directory by bare filename stem.'''
    root = Path(directory).expanduser().resolve()
    if not root.is_dir():
        message = f'{source}_dir is not a directory: {root}'
        raise ValueError(message)
    output = {}
    for path in sorted(root.rglob('*')):
        if not path.is_file(): continue
        if path.suffix.lower() != suffix: continue
        if path.stem in output:
            first = output[path.stem]
            message = f'duplicate {source} stem {path.stem}: '
            message += f'{first} and {path}'
            raise ValueError(message)
        output[path.stem] = path.resolve()
    return root, output


def _report_missing_recording_files(stem, audio, ort, awd, report):
    '''Report each missing member of one source-file triple.'''
    if stem not in audio:
        report.record('missing_audio', recording=stem)
    if stem not in ort:
        report.record('missing_ort', recording=stem)
    if stem not in awd:
        report.record('missing_awd', recording=stem)


def collect_cgn_audio_and_transcription_files(audio_dir, ort_dir, awd_dir,
    strict=False, report=None):
    '''Collect complete CGN Audio, ORT, and AWD triples by recording stem.
    audio_dir:  root searched recursively for WAV files
    ort_dir:    root searched recursively for ORT files
    awd_dir:    root searched recursively for AWD files
    strict:     require identical stems across all three roots
    report:     optional ImportReport receiving discovery issues
    '''
    report = report or ImportReport()
    audio_root, audio = _collect_files_by_stem(audio_dir, '.wav', 'audio')
    ort_root, ort = _collect_files_by_stem(ort_dir, '.ort', 'ort')
    awd_root, awd = _collect_files_by_stem(awd_dir, '.awd', 'awd')
    audio_count = len(audio)
    ort_count = len(ort)
    awd_count = len(awd)
    audio_root_text = str(audio_root)
    ort_root_text = str(ort_root)
    awd_root_text = str(awd_root)
    report.record('audio_files_discovered', audio_count,
        audio_dir=audio_root_text)
    report.record('ort_files_discovered', ort_count,
        ort_dir=ort_root_text)
    report.record('awd_files_discovered', awd_count,
        awd_dir=awd_root_text)
    all_stems = set(audio) | set(ort) | set(awd)
    complete_stems = set(audio) & set(ort) & set(awd)
    for stem in sorted(all_stems - complete_stems):
        _report_missing_recording_files(stem, audio, ort, awd, report)
    if strict and all_stems != complete_stems:
        raise ValueError('Audio, ORT, and AWD recording stems do not match')
    complete_count = len(complete_stems)
    report.record('paired_recordings', complete_count)
    recordings = []
    for stem in sorted(complete_stems):
        files = CgnRecordingFiles(
            stem, audio[stem], ort[stem], awd[stem])
        recordings.append(files)
    return recordings


def awd_speaker_tiers(textgrid):
    '''Return validated speaker -> (word, FON, SEG) tier triples.'''
    tiers = textgrid.tiers
    if len(tiers) % 3:
        message = 'AWD tier count is not divisible by three'
        raise ValueError(message)
    output = {}
    for index in range(0, len(tiers), 3):
        word, fon, segment = tiers[index:index + 3]
        expected = (word.name, f'{word.name}_FON', f'{word.name}_SEG')
        actual = (word.name, fon.name, segment.name)
        if actual != expected:
            message = f'invalid AWD tier triple: {actual}; expected {expected}'
            raise ValueError(message)
        if word.name in output:
            raise ValueError(f'duplicate AWD speaker tier: {word.name}')
        _validate_parallel_word_tiers(word, fon)
        output[word.name] = (word, fon, segment)
    return output


def _validate_parallel_word_tiers(word_tier, fon_tier):
    if len(word_tier.intervals) != len(fon_tier.intervals):
        raise ValueError(f'{word_tier.name} and {fon_tier.name} differ in size')
    for word, fon in zip(word_tier.intervals, fon_tier.intervals):
        same_start = abs(word.minTime - fon.minTime) < 0.000001
        same_end = abs(word.maxTime - fon.maxTime) < 0.000001
        if not same_start or not same_end:
            message = f'{word_tier.name} and {fon_tier.name} times differ'
            raise ValueError(message)


def ort_speaker_tiers(textgrid):
    '''Return non-empty ORT speaker tiers, excluding annotation tiers.'''
    output = {}
    for tier in textgrid.tiers:
        if tier.name in ORT_NON_SPEAKER_TIERS: continue
        has_text = any(interval.mark.strip() for interval in tier.intervals)
        if not has_text: continue
        output[tier.name] = tier
    return output


def validate_speaker_tiers(ort_tiers, awd_tiers):
    ort_names = set(ort_tiers)
    awd_names = set(awd_tiers)
    if ort_names == awd_names: return
    message = 'ORT and AWD speaker tiers differ'
    message += f'; ORT-only={sorted(ort_names - awd_names)}'
    message += f'; AWD-only={sorted(awd_names - ort_names)}'
    raise ValueError(message)


def _get_matching_symbol(text, index, symbols):
    for symbol in symbols:
        if text.startswith(symbol, index): return symbol
    return None


def map_cgn_transcription(text, report=None, **context):
    '''Map a compact CGN FON transcription to IPA, retaining markers.'''
    report = report or ImportReport()
    output = []
    index = 0
    while index < len(text):
        match = _get_matching_symbol(text, index, MAPPING_KEYS)
        if match is not None:
            output.append(cgn_to_ipa[match])
            index += len(match)
            continue
        marker = _get_matching_symbol(text, index, FON_BOUNDARY_MARKS)
        if marker is not None:
            output.append(marker)
            index += len(marker)
            continue
        symbol = text[index]
        report.record('unknown_fon_symbol', symbol=symbol, **context)
        output.append(symbol)
        index += 1
    return ''.join(output)


def _clean_word_label(mark):
    mark = mark.strip()
    if mark == '_': return mark
    unreliable = mark.startswith('!')
    source = mark[1:] if unreliable else mark
    label = clean.clean_dutch_cgn(source)
    if not label: label = source
    return f'!{label}' if unreliable else label


def _milliseconds(seconds):
    return int(round(seconds * 1000))


def _interval_index_at_start(intervals, starts, timestamp):
    '''Find the interval owning a start time, choosing the later boundary.'''
    index = bisect_right(starts, timestamp) - 1
    if index < 0: return None
    interval = intervals[index]
    if timestamp <= interval.maxTime + 0.000001: return index
    return None


def _phrase_index_at_midpoint(intervals, midpoint):
    for index, interval in enumerate(intervals):
        if not interval.mark.strip(): continue
        if interval.minTime <= midpoint <= interval.maxTime: return index
    return None


def _phone_kwargs(audio, speaker, interval, store):
    return {
        'start': _milliseconds(interval.minTime),
        'end': _milliseconds(interval.maxTime),
        'audio_id': audio.identifier,
        'speaker_id': speaker.identifier,
        'store': store,
    }


def _phones_by_word(word_tier, segment_tier, audio, speaker, store,
    report, context):
    intervals = word_tier.intervals
    starts = [interval.minTime for interval in intervals]
    output = {index: [] for index in range(len(intervals))}
    for segment in segment_tier.intervals:
        mark = segment.mark.strip()
        if not mark: continue
        owner = _interval_index_at_start(intervals, starts, segment.minTime)
        if owner is None or not intervals[owner].mark.strip():
            report.record('unassigned_phone', mark=mark, **context)
            continue
        word_mark = intervals[owner].mark.strip()
        if word_mark.startswith('!') or mark.startswith('!'):
            report.record('unreliable_alignment', word=word_mark, **context)
            continue
        if mark in SKIPPED_SEGMENT_MARKS:
            report.record('skipped_segment_marker', mark=mark, **context)
            continue
        ipa = cgn_to_ipa.get(mark)
        if ipa is None:
            report.record('unknown_phone_symbol', symbol=mark, **context)
            continue
        kwargs = _phone_kwargs(audio, speaker, segment, store)
        phone = models.Phone(label=ipa, **kwargs)
        output[owner].append(phone)
        report.record('phones_staged')
    return output


def _add_syllables(word, phones, report, context):
    if not phones: return
    fallback = False
    try:
        groups = resyllabify_phones(phones)
    except ValueError as error:
        groups = [phones]
        fallback = True
        kind = 'fallback_syllable'
        if 'no vowel nucleus' not in str(error):
            kind = 'syllabifier_error'
        error_text = str(error)
        report.record(kind, error=error_text, word=word.label, **context)
    for group in groups:
        label = ''.join(x.label for x in group)
        start = min(x.start for x in group)
        end = max(x.end for x in group)
        syllable = models.Syllable(label=label, start=start, end=end,
            audio_id=word.audio_id, speaker_id=word.speaker_id,
            store=word.store)
        syllable.add_parent(word)
        syllable.add_children(group)
        if fallback:
            report.record('syllables_staged')
            continue
        try:
            syllable_structure.assign_syllable_positions_to_phones(group)
        except ValueError as error:
            error_text = str(error)
            report.record('phone_position_error', error=error_text,
                word=word.label, **context)
        report.record('syllables_staged')


def _speaker_phrase_trees(ort_tier, awd_tiers, audio, speaker, awd_path,
    store, report, multiple_speakers):
    word_tier, fon_tier, segment_tier = awd_tiers
    context = {'recording': awd_path.stem, 'speaker': speaker.name}
    phones_by_word = _phones_by_word(word_tier, segment_tier, audio, speaker,
        store, report, context)
    phrase_words = {index: [] for index in range(len(ort_tier.intervals))}
    word_fon_pairs = zip(word_tier.intervals, fon_tier.intervals)
    for index, (word_interval, fon_interval) in enumerate(word_fon_pairs):
        mark = word_interval.mark.strip()
        if not mark: continue
        midpoint = (word_interval.minTime + word_interval.maxTime) / 2
        intervals = ort_tier.intervals
        phrase_index = _phrase_index_at_midpoint(intervals, midpoint)
        if phrase_index is None:
            report.record('unassigned_word', word=mark,
                midpoint=midpoint, **context)
            continue
        label = _clean_word_label(mark)
        start = _milliseconds(word_interval.minTime)
        end = _milliseconds(word_interval.maxTime)
        word = models.Word(label=label, start=start, end=end,
            audio_id=audio.identifier, speaker_id=speaker.identifier,
            store=store)
        transcription = fon_interval.mark.strip()
        word.ipa = map_cgn_transcription(transcription,
            report=report, word=mark, **context)
        if not multiple_speakers:
            word.overlap_code = utils.overlap_dict[False]
        _add_syllables(word, phones_by_word[index], report, context)
        phrase_words[phrase_index].append(word)
        report.record('words_staged')

    phrases = []
    for index, words in phrase_words.items():
        if not words:
            anchor = ort_tier.intervals[index].mark.strip()
            if anchor:
                report.record('empty_phrase_anchor', index=index, **context)
            continue
        interval = ort_tier.intervals[index]
        label = clean.clean_dutch_cgn(interval.mark)
        if not label: label = ' '.join(word.label for word in words)
        start = min(word.start for word in words)
        end = max(word.end for word in words)
        phrase = models.Phrase(label=label, start=start, end=end,
            audio_id=audio.identifier, speaker_id=speaker.identifier,
            store=store)
        phrase.filename = str(awd_path.resolve())
        if not multiple_speakers:
            phrase.overlap_code = utils.overlap_dict[False]
        phrase.add_children(words)
        phrases.append(phrase)
        report.record('phrases_staged')
    return phrases


def textgrids_to_phrase_trees(ort_textgrid, awd_textgrid, audio, speakers,
    awd_path, store, report=None):
    '''Create staged Phrase trees from one paired ORT/AWD recording.'''
    report = report or ImportReport()
    ort_tiers = ort_speaker_tiers(ort_textgrid)
    awd_tiers = awd_speaker_tiers(awd_textgrid)
    active_awd = {}
    for name, tiers in awd_tiers.items():
        word_tier = tiers[0]
        has_words = any(x.mark.strip() for x in word_tier.intervals)
        if has_words: active_awd[name] = tiers
    missing = set(active_awd) - set(speakers)
    if missing:
        missing = sorted(missing)
        message = f'missing Speaker objects: {missing}'
        raise ValueError(message)
    validate_speaker_tiers(ort_tiers, active_awd)
    multiple = len(active_awd) > 1
    phrases = []
    path = Path(awd_path)
    for name, tiers in active_awd.items():
        speaker_phrases = _speaker_phrase_trees(ort_tiers[name], tiers,
            audio, speakers[name], path, store, report, multiple)
        phrases.extend(speaker_phrases)
    phrases.sort(key=lambda phrase: (phrase.start, phrase.speaker_id))
    return phrases


def load_speaker_metadata(filename, report=None):
    '''Load CGN speaker metadata independently of the WebMAUS scripts.'''
    report = report or ImportReport()
    path = Path(filename)
    if not path.exists():
        path_text = str(path)
        report.record('missing_speaker_file', filename=path_text)
        return {}
    text = path.read_text(encoding='utf-8')
    rows = []
    for line in text.splitlines():
        if line: rows.append(line.split('\t'))
    if not rows: return {}
    header = rows[0]
    output = {}
    for row in rows[1:]:
        info = dict(zip(header, row))
        speaker_id = info.get('ID')
        if speaker_id: output[speaker_id] = info
    return output


def _speaker_from_metadata(name, metadata, store, report):
    info = metadata.get(name, {})
    gender = {'sex1': 'male', 'sex2': 'female'}
    try:
        age = 2000 - int(info.get('birthYear', ''))
    except ValueError:
        age = 0
    if name.startswith('N'): dialect = 'nl-NL'
    elif name.startswith('V'): dialect = 'nl-BE'
    else: dialect = 'unknown'
    region = info.get('resRegion', '')
    speaker = models.Speaker(name=name, dataset='cgn', age=age,
        language='nld', dialect=dialect, region=region, store=store)
    gender_name = gender.get(info.get('sex'), 'unknown')
    speaker.gender_code = utils.gender_dict[gender_name]
    if not info:
        report.record('missing_speaker_metadata', speaker=name)
    return speaker


def _audio_from_path(filename, store):
    filename = normalize_audio_filename(filename)
    info = audio_helper.audio_info(filename)
    parent = Path(filename).parent.name.lower()
    if parent == 'nl': dialect = 'nl-NL'
    elif parent == 'vl': dialect = 'nl-BE'
    else: dialect = 'unknown'
    return models.Audio(filename=filename, duration=info['duration'],
        n_channels=info['n_channels'], sample_rate=info['sample_rate'],
        dataset='cgn', language='nld', dialect=dialect, store=store)


def validate_target_path(db_path, resume=False):
    '''Protect the legacy database and fresh-build semantics.'''
    target = Path(db_path).expanduser()
    target = target.resolve()
    legacy = Path(locations.cgn_lmdb).expanduser()
    legacy = legacy.resolve()
    if target == legacy:
        raise ValueError('refusing to write to the configured legacy CGN DB')
    if target.exists() and not target.is_dir():
        raise ValueError(f'database target is not a directory: {target}')
    nonempty = target.exists() and any(target.iterdir())
    if nonempty and not resume:
        raise ValueError('database target is non-empty; set resume=True')
    return target


def _load_existing_recording_phrases(store, audio, recording):
    filename = str(recording.awd_path)
    keys = store.DB.audio_id_to_child_keys(audio.identifier, 'Phrase')
    keys = list(keys)
    phrases = store.load_many(keys)
    return [phrase for phrase in phrases if phrase.filename == filename]


def normalize_audio_filename(filename):
    path = Path(filename).expanduser()
    return str(path.resolve())


def _validate_cgn_database_audio(audio):
    if audio.dataset != 'cgn':
        raise ValueError('CGN AWD database contains non-CGN Audio')
    normalized = normalize_audio_filename(audio.filename)
    if audio.filename != normalized:
        message = f'database Audio filename is not normalized: {audio.filename}'
        raise ValueError(message)


def _validate_cgn_database_audios(audios):
    for audio in audios: _validate_cgn_database_audio(audio)
    filenames = [audio.filename for audio in audios]
    stems = [Path(filename).stem for filename in filenames]
    if len(set(filenames)) != len(audios):
        raise ValueError('duplicate database Audio filename')
    if len(set(stems)) != len(audios):
        raise ValueError('duplicate database Audio stem')


def _validate_cgn_database_speakers(speakers):
    datasets = [speaker.dataset for speaker in speakers]
    if any(dataset != 'cgn' for dataset in datasets):
        raise ValueError('CGN AWD database contains non-CGN Speaker')
    id_to_speaker = {speaker.name: speaker for speaker in speakers}
    identifiers = [speaker.identifier for speaker in speakers]
    if len(id_to_speaker) != len(speakers):
        raise ValueError('duplicate CGN speaker ID in database')
    if len(set(identifiers)) != len(speakers):
        raise ValueError('duplicate internal Speaker identifier in database')
    return id_to_speaker


def _get_database_audios_by_filename(store):
    '''Return all validated database Audio objects indexed by filename.'''
    audios = list(store.audios.all())
    _validate_cgn_database_audios(audios)
    return {audio.filename: audio for audio in audios}


def _get_database_speakers_by_id(store):
    '''Return all validated database Speaker objects indexed by CGN ID.'''
    speakers = list(store.speakers.all())
    return _validate_cgn_database_speakers(speakers)


def _validate_database_audio_sources(recordings, audios):
    filename_by_stem = {}
    for recording in recordings:
        filename_by_stem[recording.stem] = str(recording.audio_path)
    for audio in audios:
        stem = Path(audio.filename).stem
        expected = filename_by_stem.get(stem)
        if expected is None: continue
        if expected == audio.filename: continue
        message = f'database audio path changed for {stem}: '
        message += f'{audio.filename} != {expected}'
        raise ValueError(message)


def _select_resume_audit_stems(recordings, filename_to_audio,
    count=RESUME_AUDIT_COUNT):
    '''Return the final persisted source stems in deterministic order.'''
    stored = []
    for recording in recordings:
        filename = str(recording.audio_path)
        if filename in filename_to_audio: stored.append(recording.stem)
    return set(stored[-count:])


def make_cgn_import_state(store, report, speaker_metadata, recordings,
    resume=False):
    '''Create fresh state or load and validate state for a resumed import.'''
    if not resume:
        report.record('database_audio_files', 0)
        report.record('resume_audit_recordings', 0)
        audit_stems = set()
        return CgnImportState(
            store, report, speaker_metadata, {}, {}, audit_stems)
    filename_to_audio = _get_database_audios_by_filename(store)
    audios = filename_to_audio.values()
    _validate_database_audio_sources(recordings, audios)
    id_to_speaker = _get_database_speakers_by_id(store)
    audit_stems = _select_resume_audit_stems(
        recordings, filename_to_audio)
    audio_count = len(filename_to_audio)
    audit_count = len(audit_stems)
    report.record('database_audio_files', audio_count)
    report.record('resume_audit_recordings', audit_count)
    return CgnImportState(store, report, speaker_metadata, id_to_speaker,
        filename_to_audio, audit_stems)


def _phrase_signatures(phrases):
    output = []
    for phrase in phrases:
        signature = (phrase.speaker_id, phrase.start, phrase.end, phrase.label)
        output.append(signature)
    return sorted(output)


def _collect_recording_segments(phrases):
    segments = []
    for phrase in phrases: segments.extend(phrase.items)
    return segments


def _find_missing_label_index_keys(store, phrases):
    segments = _collect_recording_segments(phrases)
    expected = [segment.label_index_key for segment in segments]
    values = store.DB.load_many(expected, db_name='label_segment')
    missing = []
    for key, value in zip(expected, values):
        if value is None: missing.append(key)
    return expected, missing


def repair_missing_label_indices(store, phrases, recording, report):
    expected, missing = _find_missing_label_index_keys(store, phrases)
    expected_count = len(expected)
    missing_count = len(missing)
    report.record('label_indices_checked', expected_count,
        recording=recording)
    if not missing: return
    store.DB.write_many_label_index_links(missing)
    report.record('label_indices_repaired', missing_count,
        recording=recording)


def get_or_stage_recording_speakers(speaker_ids, state):
    '''Get known speakers and stage missing speakers without saving them.'''
    id_to_speaker = {}
    new_speakers = []
    for speaker_id in sorted(speaker_ids):
        speaker = state.id_to_speaker.get(speaker_id)
        if speaker is None:
            speaker = _speaker_from_metadata(
                speaker_id, state.speaker_metadata, state.store, state.report)
            new_speakers.append(speaker)
        id_to_speaker[speaker_id] = speaker
    return id_to_speaker, new_speakers


def stage_cgn_recording(recording, audio, state):
    '''Build one recording completely in memory before database writes.'''
    ort_textgrid = load_textgrid(recording.ort_path)
    awd_textgrid = load_textgrid(recording.awd_path)
    awd_tiers = awd_speaker_tiers(awd_textgrid)
    id_to_speaker, new_speakers = get_or_stage_recording_speakers(
        awd_tiers, state)
    new_audio = audio is None
    if new_audio: audio = _audio_from_path(recording.audio_path, state.store)
    phrases = textgrids_to_phrase_trees(
        ort_textgrid, awd_textgrid, audio, id_to_speaker,
        recording.awd_path, state.store, state.report)
    return _StagedCgnRecording(audio, new_audio, id_to_speaker,
        new_speakers, phrases)


def _record_import_error(report, recording, error, phase):
    error_text = f'{type(error).__name__}: {error}'
    report.record('recording_errors', recording=recording.stem, phase=phase,
        error=error_text)
    kind = {
        'resume': 'resume_integrity_errors',
        'source': 'source_recording_errors',
        'write': 'recording_write_errors',
    }[phase]
    report.record(kind, recording=recording.stem, error=error_text)
    report.record('recordings_skipped', recording=recording.stem)


def try_stage_cgn_recording(recording, audio, state):
    try:
        return stage_cgn_recording(recording, audio, state)
    except Exception as error:
        _record_import_error(state.report, recording, error, 'source')
        return None


def check_and_repair_existing_recording(recording, staged, state):
    if not staged.new_audio:
        existing = _load_existing_recording_phrases(
            state.store, staged.audio, recording)
    else:
        existing = []
    if not existing: return False
    if _phrase_signatures(existing) != _phrase_signatures(staged.phrases):
        error = ValueError('resume found a partial or changed recording')
        _record_import_error(state.report, recording, error, 'resume')
        raise error
    try:
        repair_missing_label_indices(
            state.store, existing, recording.stem, state.report)
    except Exception as error:
        _record_import_error(state.report, recording, error, 'write')
        raise
    state.report.record('recordings_audited', recording=recording.stem)
    state.report.record('recordings_skipped', recording=recording.stem)
    return True


def save_cgn_recording(recording, staged, state):
    '''Save one recording and update import state after all writes succeed.'''
    try:
        if staged.new_speakers:
            state.store.save_many(staged.new_speakers)
        if staged.new_audio: state.store.save(staged.audio)
        for speaker in staged.id_to_speaker.values():
            state.store.DB.write_speaker_audio_link(speaker, staged.audio)
        state.store.save_phrase_trees(staged.phrases)
    except Exception as error:
        _record_import_error(state.report, recording, error, 'write')
        raise
    new_speakers = {speaker.name: speaker for speaker in staged.new_speakers}
    state.id_to_speaker.update(new_speakers)
    filename = str(recording.audio_path)
    if staged.new_audio: state.filename_to_audio[filename] = staged.audio
    kind = 'recordings_saved' if staged.new_audio else 'recordings_repaired'
    state.report.record(kind, recording=recording.stem)
    state.store.close()
    state.store.open()


def process_cgn_recording(recording, state):
    '''Fast-skip, audit, repair, or save one ordered CGN recording.'''
    filename = str(recording.audio_path)
    audio = state.filename_to_audio.get(filename)
    if audio is not None and recording.stem not in state.audit_stems:
        state.report.record(
            'recordings_fast_skipped', recording=recording.stem)
        state.report.record('recordings_skipped', recording=recording.stem)
        return
    staged = try_stage_cgn_recording(recording, audio, state)
    if staged is None: return
    if check_and_repair_existing_recording(recording, staged, state): return
    save_cgn_recording(recording, staged, state)


def _save_report(report, filename=None):
    if filename is None: return
    data = report.to_dict()
    text = json.dumps(data, ensure_ascii=False, indent=2)
    Path(filename).write_text(text + '\n', encoding='utf-8')
