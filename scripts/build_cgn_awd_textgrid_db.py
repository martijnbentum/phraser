'''Build a fresh Phraser database from original CGN ORT and AWD files.'''

import argparse
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


@dataclass(frozen=True)
class AnnotationPair:
    '''One recording's paired original CGN annotation files.'''

    stem: str
    ort: Path
    awd: Path


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


def load_textgrid(filename):
    '''Load one original CGN TextGrid.'''
    return TextGrid.fromFile(str(filename))


def pair_annotation_files(ort_dir, awd_dir, strict=False, report=None):
    '''Pair ORT and AWD files by recording stem.'''
    report = report or ImportReport()
    ort = {path.stem: path for path in Path(ort_dir).glob('*.ort')}
    awd = {path.stem: path for path in Path(awd_dir).glob('*.awd')}
    for stem in sorted(ort.keys() - awd.keys()):
        filename = str(ort[stem])
        report.record('missing_awd', stem=stem, ort=filename)
    for stem in sorted(awd.keys() - ort.keys()):
        filename = str(awd[stem])
        report.record('missing_ort', stem=stem, awd=filename)
    if strict and ort.keys() != awd.keys():
        raise ValueError('ORT and AWD recording stems do not match')
    stems = sorted(ort.keys() & awd.keys())
    count = len(stems)
    report.record('paired_recordings', count)
    return [AnnotationPair(stem, ort[stem], awd[stem]) for stem in stems]


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


def map_cgn_transcription(text, report=None, **context):
    '''Map a compact CGN FON transcription to IPA, retaining markers.'''
    report = report or ImportReport()
    output = []
    index = 0
    while index < len(text):
        match = next((key for key in MAPPING_KEYS
            if text.startswith(key, index)), None)
        if match is not None:
            output.append(cgn_to_ipa[match])
            index += len(match)
            continue
        marker = next((value for value in FON_BOUNDARY_MARKS
            if text.startswith(value, index)), None)
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


def load_audio_filenames(filename, report=None):
    '''Return recording stem -> audio path from a newline-delimited file.'''
    report = report or ImportReport()
    path = Path(filename)
    if not path.exists():
        path_text = str(path)
        report.record('missing_audio_filename_list', filename=path_text)
        return {}
    output = {}
    text = path.read_text(encoding='utf-8')
    for line in text.splitlines():
        if not line.strip(): continue
        audio_path = Path(line.strip()).expanduser()
        audio_path = audio_path.resolve()
        if audio_path.stem in output:
            raise ValueError(f'duplicate audio stem: {audio_path.stem}')
        output[audio_path.stem] = audio_path
    return output


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
    filename = str(filename)
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
        raise ValueError('database target is non-empty; pass --resume')
    return target


def _existing_recording_phrases(store, audio, awd_path):
    filename = str(Path(awd_path).resolve())
    keys = store.DB.audio_id_to_child_keys(audio.identifier, 'Phrase')
    keys = list(keys)
    phrases = store.load_many(keys)
    return [phrase for phrase in phrases if phrase.filename == filename]


def _phrase_signatures(phrases):
    output = []
    for phrase in phrases:
        signature = (phrase.speaker_id, phrase.start, phrase.end, phrase.label)
        output.append(signature)
    return sorted(output)


def _write_report(report, filename=None):
    data = report.to_dict()
    text = json.dumps(data, ensure_ascii=False, indent=2)
    if filename:
        Path(filename).write_text(text + '\n', encoding='utf-8')
    print(text)


def build_database(ort_dir, awd_dir, audio_filename_list, speaker_file,
    db_path, resume=False, strict_pairs=False, report_file=None):
    '''Build a complete independent CGN database one recording at a time.'''
    report = ImportReport()
    target = validate_target_path(db_path, resume=resume)
    pairs = pair_annotation_files(ort_dir, awd_dir, strict=strict_pairs,
        report=report)
    audio_filenames = load_audio_filenames(audio_filename_list, report)
    metadata = load_speaker_metadata(speaker_file, report)
    store = Store(path=target)
    store.refresh_query_roots()
    audios = {}
    for audio in store.audios.all():
        stem = Path(audio.filename).stem
        audios[stem] = audio
    speakers = {}
    for speaker in store.speakers.all():
        if speaker.dataset == 'cgn': speakers[speaker.name] = speaker
    try:
        for pair in progressbar(pairs):
            try:
                audio_path = audio_filenames.get(pair.stem)
                if audio_path is None or not audio_path.exists():
                    report.record('missing_audio', recording=pair.stem,
                        filename=str(audio_path) if audio_path else '')
                    report.record('recordings_skipped',
                        recording=pair.stem)
                    continue
                ort_textgrid = load_textgrid(pair.ort)
                awd_textgrid = load_textgrid(pair.awd)
                tier_names = set(awd_speaker_tiers(awd_textgrid))
                recording_speakers = {}
                new_speakers = []
                for name in tier_names:
                    speaker = speakers.get(name)
                    if speaker is None:
                        speaker = _speaker_from_metadata(
                            name, metadata, store, report)
                        speakers[name] = speaker
                        new_speakers.append(speaker)
                    recording_speakers[name] = speaker
                audio = audios.get(pair.stem)
                new_audio = audio is None
                if new_audio:
                    audio = _audio_from_path(audio_path, store)
                    audios[pair.stem] = audio
                phrases = textgrids_to_phrase_trees(
                    ort_textgrid, awd_textgrid, audio, recording_speakers,
                    pair.awd, store, report)
                existing = _existing_recording_phrases(
                    store, audio, pair.awd) if not new_audio else []
                if existing:
                    if _phrase_signatures(existing) != \
                        _phrase_signatures(phrases):
                        message = 'resume found a partial or changed recording'
                        raise ValueError(message)
                    report.record('recordings_skipped', recording=pair.stem)
                    continue
                if new_speakers: store.save_many(new_speakers)
                if new_audio: store.save(audio)
                for speaker in recording_speakers.values():
                    store.DB.write_speaker_audio_link(speaker, audio)
                store.save_phrase_trees(phrases)
                report.record('recordings_saved', recording=pair.stem)
                store.close()
                store.open()
            except Exception as error:
                report.record('recording_errors', recording=pair.stem,
                    error=f'{type(error).__name__}: {error}')
                report.record('recordings_skipped', recording=pair.stem)
    finally:
        store.close()
    _write_report(report, report_file)
    if report.counts['recording_errors']:
        count = report.counts['recording_errors']
        message = f'{count} recording imports failed; see the import report'
        raise RuntimeError(message)
    return report


def make_argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ort-dir', type=Path,
        default=locations.cgn_ort_directory)
    parser.add_argument('--awd-dir', type=Path,
        default=locations.data / 'awd')
    parser.add_argument('--audio-filenames', type=Path,
        default=locations.audio_filenames)
    parser.add_argument('--speaker-file', type=Path,
        default=locations.cgn_speaker_file)
    parser.add_argument('--db-path', type=Path, required=True)
    parser.add_argument('--report-file', type=Path)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--strict-pairs', action='store_true')
    return parser


def main():
    args = make_argument_parser().parse_args()
    build_database(args.ort_dir, args.awd_dir, args.audio_filenames,
        args.speaker_file, args.db_path, resume=args.resume,
        strict_pairs=args.strict_pairs, report_file=args.report_file)


if __name__ == '__main__':
    main()
