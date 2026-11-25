'''Collect speaker, audio and clip infos for a Common Voice import.

No store access. Reads, per language folder in the FLAC working copy
(locations.common_voice_base / <iso>):

validated.tsv         one row per clip, read by column name; headers differ
                      by release (accent vs accents, extra columns), only
                      the columns below are used.
clip_stems.txt        clips that have both a FLAC and a TextGrid.
flac_durations.tsv    exact durations from the FLAC headers, written once by
                      flac_durations.py (opening 1.4 M files on NFS is slow).

prepare(iso) returns plain dicts: speakers and audios ready for
models.Speaker(**info) / models.Audio(**info), clips (one phrase each) and
a report of what was skipped or ambiguous. Everything here takes seconds,
so nothing is cached.
'''
import csv
from collections import Counter, defaultdict

from phraser import locations

from .speaker_metadata import cv_age, cv_gender

DATASET = 'common_voice'
LANGUAGES = ('nld', 'eng', 'fra', 'deu', 'hun', 'ita', 'pol', 'spa')


def language_dir(iso, base=None):
    if iso not in LANGUAGES: raise ValueError(f'unknown language {iso!r}')
    if base is None: base = locations.common_voice_base
    return base / iso


def read_validated(iso, base=None):
    '''Return validated.tsv rows as dicts with the used fields only:
    stem, client_id, sentence, up_votes, down_votes, gender, age, accent.'''
    path = language_dir(iso, base) / 'validated.tsv'
    csv.field_size_limit(10**8)
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        accent = 'accents' if 'accents' in reader.fieldnames else 'accent'
        for r in reader:
            rows.append({'stem': r['path'].rsplit('.', 1)[0],
                'client_id': r['client_id'], 'sentence': r['sentence'],
                'up_votes': int(r['up_votes']),
                'down_votes': int(r['down_votes']),
                'gender': r['gender'].strip(), 'age': r['age'].strip(),
                'accent': r[accent].strip()})
    return rows


def read_clip_stems(iso, base=None):
    '''Stems of clips that have both a FLAC and a TextGrid.'''
    path = language_dir(iso, base) / 'clip_stems.txt'
    with open(path, encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}


def read_durations(iso, base=None):
    '''Return {stem: (sample_rate, n_channels, duration_ms)}.'''
    path = language_dir(iso, base) / 'flac_durations.tsv'
    durations = {}
    with open(path, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f, delimiter='\t'):
            durations[r['stem']] = (int(r['sample_rate']),
                int(r['n_channels']), int(r['duration_ms']))
    return durations


def most_common_value(values):
    '''Most common non-empty value ('' if none); ties go to the value seen
    first. Contributors sometimes fill in a field on some clips only.'''
    counts = Counter(v for v in values if v)
    if not counts: return ''
    return counts.most_common(1)[0][0]


def make_speaker_infos(rows, iso):
    '''Return ({client_id: speaker info}, conflicts). conflicts lists the
    speakers with two different non-empty values for a field.'''
    by_speaker = defaultdict(list)
    for row in rows: by_speaker[row['client_id']].append(row)
    infos, conflicts = {}, []
    for client_id, speaker_rows in by_speaker.items():
        values = {}
        for field in ('gender', 'age', 'accent'):
            column = [r[field] for r in speaker_rows]
            distinct = sorted({v for v in column if v})
            if len(distinct) > 1:
                conflicts.append({'client_id': client_id, 'field': field,
                    'values': distinct})
            values[field] = most_common_value(column)
        infos[client_id] = {'name': client_id, 'dataset': DATASET,
            'gender': cv_gender(values['gender']),
            'age': cv_age(values['age']), 'language': iso,
            'region': values['accent'], 'dialect': ''}
    return infos, conflicts


def clip_rank(clip, speaker):
    '''Sort key, best first: speakers with gender and age known, then net
    votes, then stem (deterministic). Accent is left out: it is empty for
    most hun/ita/pol rows.'''
    known = (speaker['gender'] != 'unknown') + (speaker['age'] != 0)
    return (-known, -(clip['up_votes'] - clip['down_votes']), clip['stem'])


def select_clips(clips, speakers, max_clips=None, max_per_speaker=None):
    '''Return the selected clips.

    max_per_speaker  keep each speaker's best ranked clips only.
    max_clips        take clips round-robin over speakers (one clip per
                     speaker per round, speakers ordered by their best
                     clip), so a few prolific speakers cannot fill the
                     selection. None keeps every clip.
    '''
    by_speaker = defaultdict(list)
    for clip in clips: by_speaker[clip['client_id']].append(clip)
    queues = []
    for client_id, speaker_clips in by_speaker.items():
        speaker = speakers[client_id]
        speaker_clips.sort(key=lambda c: clip_rank(c, speaker))
        queues.append(speaker_clips[:max_per_speaker])
    queues.sort(key=lambda q: clip_rank(q[0], speakers[q[0]['client_id']]))
    if max_clips is None:
        return [clip for queue in queues for clip in queue]
    selected, round_index = [], 0
    while len(selected) < max_clips:
        taken = [q[round_index] for q in queues if round_index < len(q)]
        if not taken: break
        selected.extend(taken[:max_clips - len(selected)])
        round_index += 1
    return selected


def prepare(iso, max_clips=None, max_per_speaker=None, base=None):
    '''Return {'speakers', 'audios', 'clips', 'report'} for one language.

    speakers  speaker infos (models.Speaker kwargs) of selected clips
    audios    audio infos (models.Audio kwargs), one per selected clip
    clips     {'stem', 'client_id', 'sentence', 'audio_filename',
              'textgrid_filename'} per selected clip
    report    counts, rows skipped for lack of FLAC/TextGrid or duration,
              and speaker metadata conflicts
    '''
    folder = language_dir(iso, base)
    rows = read_validated(iso, base)
    stems = read_clip_stems(iso, base)
    durations = read_durations(iso, base)
    no_files = [r['stem'] for r in rows if r['stem'] not in stems]
    no_duration = [r['stem'] for r in rows
        if r['stem'] in stems and r['stem'] not in durations]
    usable = [r for r in rows if r['stem'] in stems and r['stem'] in durations]
    all_speakers, conflicts = make_speaker_infos(rows, iso)
    selected = select_clips(usable, all_speakers, max_clips, max_per_speaker)
    speaker_ids = {clip['client_id'] for clip in selected}
    speakers = [all_speakers[s] for s in sorted(speaker_ids)]
    audios, clips = [], []
    for row in selected:
        stem = row['stem']
        audio_filename = str(folder / 'clips' / f'{stem}.flac')
        sample_rate, n_channels, duration = durations[stem]
        audios.append({'filename': audio_filename, 'sample_rate': sample_rate,
            'n_channels': n_channels, 'duration': duration,
            'dataset': DATASET, 'language': iso, 'dialect': ''})
        clips.append({'stem': stem, 'client_id': row['client_id'],
            'sentence': row['sentence'], 'audio_filename': audio_filename,
            'textgrid_filename': str(folder / 'textgrids'
                / f'{stem}.TextGrid')})
    report = {'iso': iso, 'n_rows': len(rows), 'n_usable': len(usable),
        'n_selected': len(selected), 'n_speakers': len(speakers),
        'no_files': no_files, 'no_duration': no_duration,
        'conflicts': conflicts}
    return {'speakers': speakers, 'audios': audios, 'clips': clips,
        'report': report}


def print_report(report):
    '''Print a short summary of a prepare report.'''
    print(f"{report['iso']}: {report['n_rows']} rows, "
        f"{report['n_usable']} usable, {report['n_selected']} selected "
        f"from {report['n_speakers']} speakers")
    print(f"  skipped: {len(report['no_files'])} without FLAC/TextGrid, "
        f"{len(report['no_duration'])} without duration")
    for c in report['conflicts']:
        print(f"  conflict {c['client_id'][:12]}... {c['field']}: "
            f"{c['values']}")
