'''Recompute persisted overlap codes using segment spans and speech words.

Dry runs open LMDB read-only. Apply mode commits one complete audio at a
time, changes only overlap-code bytes, and verifies after reopening LMDB.
Keep annotations unchanged while migrating. Use a consistent database copy
for the initial dry run and migration.

Import and call from Python or IPython:
    from scripts.migrate_overlap import migrate_overlap
    report = migrate_overlap('/path/to/database-copy')
    report['summary']

Apply and resume use a separate checkpoint:
    report = migrate_overlap('/path/to/database-copy', apply=True)
    report = migrate_overlap('/path/to/database-copy', apply=True, resume=True)

Close any Store for this database before calling. The returned dictionary
contains the report_path; default reports are saved beside the database.
'''

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import lmdb

from phraser import key_helper, struct_value
from phraser.overlap import SEGMENT_TYPES, set_overlap_codes


DEFINITION = 'segment-span-other-speaker-words-v1'
REPORT_VERSION = 1


def _open_database(path, readonly=True):
    '''Open an existing LMDB without creating databases or indexes.'''
    env = lmdb.open(str(path), readonly=readonly, create=False,
        max_dbs=3, map_size=1024**4)
    try:
        main = env.open_db(b'main', create=False)
    except Exception:
        env.close()
        raise
    return env, main


def _audio_inventory(env, main):
    '''Include empty audios and segments without a persisted audio row.'''
    audios = {}
    with env.begin(db=main) as txn:
        for key, raw in txn.cursor():
            object_type = key_helper.key_to_object_type(key)
            if object_type == 'Audio':
                fields = struct_value.unpack_instance('Audio', raw)
                audio_id = key_helper.key_to_identifier(key)
                audios[audio_id.hex()] = fields['filename']
            elif object_type in SEGMENT_TYPES:
                audio_id = key_helper.key_to_audio_identifier(key)
                audios.setdefault(audio_id.hex(), None)
    return dict(sorted(audios.items()))


def _load_rows(txn, main, audio_id):
    '''Read every segment directly, including unlinked words and phones.'''
    rows = []
    for object_type in SEGMENT_TYPES:
        prefix = key_helper.pack_audio_scan_prefix(audio_id, object_type)
        cursor = txn.cursor(db=main)
        if not cursor.set_range(prefix): continue
        for key, raw in cursor:
            if not key.startswith(prefix): break
            fields = key_helper.key_to_info(key)
            fields.update(struct_value.unpack_instance(object_type, raw))
            if fields['version'] != struct_value.VERSION:
                raise ValueError(f'Unsupported row version for {key.hex()}')
            item = SimpleNamespace(**fields)
            rows.append((key, raw, item))
    return rows


def _fingerprint(rows):
    '''Hash exact segment keys and values in stable scan order.'''
    digest = hashlib.sha256()
    for key, raw, item in rows:
        digest.update(len(key).to_bytes(8, 'big'))
        digest.update(key)
        digest.update(len(raw).to_bytes(8, 'big'))
        digest.update(raw)
    return digest.hexdigest()


def _replace_code(raw, item):
    '''Change only the overlap byte, preserving flags and all other fields.'''
    layout = struct_value.LAYOUTS[item.object_type.lower()]
    offset = layout['fixed_fields'].index('overlap_code')
    expected = '>' + 'B' * (offset + 1)
    if not layout['fixed_fmt'].startswith(expected):
        raise ValueError('Unsupported overlap header layout')
    return raw[:offset] + bytes([item.overlap_code]) + raw[offset + 1:]


def _phrase_pause_count(items):
    '''Count overlapping phrases with no overlapping owned word.'''
    parents = set()
    for item in items:
        if item.object_type == 'Word' and item.overlap_code == 1:
            parents.add(item.parent_id)
    count = 0
    for item in items:
        if item.object_type != 'Phrase' or item.overlap_code != 1: continue
        if item.identifier not in parents: count += 1
    return count


def _calculate(rows):
    '''Compute replacements, transitions, and a bounded sample of changes.'''
    items = [item for key, raw, item in rows]
    old_codes = [item.overlap_code for item in items]
    set_overlap_codes(items)
    levels = {name: Counter(total=0, changed=0) for name in SEGMENT_TYPES}
    updated, examples = [], []
    for (key, raw, item), old in zip(rows, old_codes):
        stats = levels[item.object_type]
        stats['total'] += 1
        stats[f'{old}->{item.overlap_code}'] += 1
        changed = old != item.overlap_code
        stats['changed'] += int(changed)
        if changed and len(examples) < 5:
            examples.append({'key': key.hex(), 'level': item.object_type,
                'label': item.label, 'start': item.start, 'end': item.end,
                'old': old, 'new': item.overlap_code})
        updated.append((key, _replace_code(raw, item), item))
    result = {'levels': levels, 'examples': examples,
        'phrases_without_overlapping_words': _phrase_pause_count(items)}
    return updated, result


def _process_audio(env, main, audio_id, apply, previous=None):
    '''Compute and optionally commit an audio in one isolated transaction.'''
    with env.begin(write=apply) as txn:
        rows = _load_rows(txn, main, bytes.fromhex(audio_id))
        if previous is not None:
            if _fingerprint(rows) != previous['fingerprint']:
                raise ValueError(f'Completed audio changed: {audio_id}')
            return previous
        updated, result = _calculate(rows)
        if apply:
            for (key, raw, item), (_, new_raw, _) in zip(rows, updated):
                if raw != new_raw: txn.put(key, new_raw, db=main)
        result['fingerprint'] = _fingerprint(updated if apply else rows)
        return result


def _write_report(path, report):
    '''Atomically replace the JSON checkpoint after a committed audio.'''
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write('\n')
        handle.flush()
    temporary.replace(path)


def _new_report(path, audios, apply):
    '''Create a checkpoint tied to this database, rule, and audio inventory.'''
    return {'report_version': REPORT_VERSION, 'definition': DEFINITION,
        'database': str(path), 'mode': 'apply' if apply else 'dry-run',
        'inventory': audios, 'audios': {}, 'complete': False,
        'verified': False}


def _read_checkpoint(report_path, expected):
    '''Reject checkpoints for another database, rule, mode, or inventory.'''
    report = json.loads(report_path.read_text())
    for name in ('report_version', 'definition', 'database', 'mode',
        'inventory'):
        if report[name] != expected[name]:
            raise ValueError(f'Checkpoint mismatch: {name}')
    return report


def _summarize(report):
    '''Aggregate transitions over both new and previously completed audios.'''
    levels = {name: Counter(total=0, changed=0) for name in SEGMENT_TYPES}
    pauses = 0
    for result in report['audios'].values():
        for name, counts in result['levels'].items():
            levels[name].update(counts)
        pauses += result['phrases_without_overlapping_words']
    report['summary'] = {'levels': levels,
        'phrases_without_overlapping_words': pauses,
        'audios_processed': len(report['audios']),
        'audios_total': len(report['inventory'])}


def _verify(path, report):
    '''Reopen LMDB and check processed codes against fresh computation.'''
    env, main = _open_database(path)
    try:
        if _audio_inventory(env, main) != report['inventory']:
            raise ValueError('Audio inventory changed before verification')
        for audio_id, expected in report['audios'].items():
            with env.begin() as txn:
                rows = _load_rows(txn, main, bytes.fromhex(audio_id))
            if _fingerprint(rows) != expected['fingerprint']:
                message = f'Audio changed before verification: {audio_id}'
                raise ValueError(message)
            updated, result = _calculate(rows)
            for (key, raw, item), (_, new_raw, _) in zip(rows, updated):
                if raw != new_raw:
                    message = f'Overlap verification failed: {key.hex()}'
                    raise ValueError(message)
    finally:
        env.close()


def migrate_overlap(db_path, report_path=None, apply=False, resume=False,
    limit=None, show_progress=True):
    '''Report or migrate all overlap codes, one audio at a time.

    db_path:        existing LMDB directory; use a consistent database copy
    report_path:    optional JSON checkpoint; defaults beside the database
    apply:          persist updates and verify after reopening; default dry run
    resume:         continue an apply checkpoint and validate completed audios
    limit:          optional positive maximum number of new audios to process
    show_progress:  print progress after each audio; defaults to True

    Returns a dictionary: inspect report['summary'], report['audios'],
    report['complete'], report['verified'], and report['report_path'].
    Default report names are <database>.overlap-dry-run.json and
    <database>.overlap-apply.json. Close existing Stores for this path first.

    Resume assumes annotations remain unchanged. A commit interrupted before
    checkpointing is safely recomputed; its original transition counts may
    be lost. The report distinguishes a complete run from a limited batch.
    '''
    path = Path(db_path).expanduser().resolve()
    if report_path is None:
        mode = 'apply' if apply else 'dry-run'
        report_path = path.with_name(f'{path.name}.overlap-{mode}.json')
    report_path = Path(report_path).expanduser().resolve()
    _validate_paths(path, report_path, apply, resume, limit)
    env, main = _open_database(path, readonly=not apply)
    try:
        audios = _audio_inventory(env, main)
        report = _new_report(path, audios, apply)
        if resume: report = _read_checkpoint(report_path, report)
        report['report_path'] = str(report_path)
        report['verified'] = False
        processed = 0
        for audio_id in audios:
            previous = report['audios'].get(audio_id)
            if previous is None and limit is not None and processed >= limit:
                continue
            result = _process_audio(env, main, audio_id, apply, previous)
            report['audios'][audio_id] = result
            if previous is None: processed += 1
            if show_progress:
                print(f'{len(report["audios"])}/{len(audios)}: {audio_id}',
                    flush=True)
            _summarize(report)
            _write_report(report_path, report)
        report['complete'] = len(report['audios']) == len(audios)
        _summarize(report)
        _write_report(report_path, report)
    finally:
        env.close()
    if apply:
        _verify(path, report)
        report['verified'] = True
        _write_report(report_path, report)
    return report


def _validate_paths(path, report_path, apply, resume, limit):
    '''Reject missing databases, accidental overwrites, and invalid options.'''
    if not (path / 'data.mdb').is_file():
        raise ValueError(f'Existing LMDB directory required: {path}')
    if report_path.is_relative_to(path):
        raise ValueError('Report must be outside the LMDB directory')
    if not report_path.parent.is_dir():
        raise ValueError('Report parent directory must already exist')
    if resume and not apply:
        raise ValueError('Resume requires apply mode')
    if report_path.exists() and not resume:
        raise FileExistsError(f'Report already exists: {report_path}')
    if resume and not report_path.is_file():
        raise ValueError('Resume requires an existing report')
    if limit is not None and limit < 1:
        raise ValueError('Limit must be positive')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', help='existing LMDB directory')
    parser.add_argument('--report', help='JSON checkpoint; defaults beside DB')
    parser.add_argument('--apply', action='store_true', help='write updates')
    parser.add_argument('--resume', action='store_true', help='resume updates')
    parser.add_argument('--limit', type=int, help='maximum new audios this run')
    args = parser.parse_args()
    try:
        report = migrate_overlap(args.database, args.report, apply=args.apply,
            resume=args.resume, limit=args.limit, show_progress=True)
    except (OSError, ValueError, lmdb.Error) as error:
        parser.exit(1, f'{error}\n')
    print(json.dumps(report['summary'], indent=2, sort_keys=True))
    print(f'Complete: {report["complete"]}; verified: {report["verified"]}')


if __name__ == '__main__':
    main()
