'''The results of one audit run, saved to or loaded from a folder.

An AuditReport holds every CheckResult, the speaker tempo rows, the outlier
groups and run info. It is saved as JSON files in an audit_{stamp} folder,
never inside the store's own LMDB folder, and loaded with load_report.
'''

import json
from pathlib import Path
from types import SimpleNamespace

from .findings import ERROR, WARNING, CheckResult, Finding, OutlierGroup
from .findings import SpeakerTempo
from .store_snapshot import segment_key_audio_id

BYTES_FIELDS = ('key', 'other_key', 'index_key', 'audio_id', 'speaker_id')
IDENTITY_FIELDS = ('key', 'other_key', 'index_key', 'kind', 'field', 'reference',
    'audio_id', 'speaker_id')


class AuditReport:
    '''The results of one audit run, saved to or loaded from a folder.'''

    def __init__(self, results, speakers, outlier_groups, info):
        self.results = results
        self.checks = SimpleNamespace(**{r.name: r for r in results})
        self.speakers = speakers
        self.outlier_groups = outlier_groups
        self.info = info

    def __repr__(self):
        m = f'<AuditReport {self.info.stamp} errors={self.n_errors} '
        m += f'warnings={self.n_warnings}>'
        return m

    @property
    def errors(self):
        return [r for r in self.results if r.severity == ERROR]

    @property
    def warnings(self):
        return [r for r in self.results if r.severity == WARNING]

    @property
    def n_errors(self):
        return sum(r.n_findings for r in self.errors)

    @property
    def n_warnings(self):
        return sum(r.n_findings for r in self.warnings)

    def summary(self):
        '''Print row counts and the findings per check.'''
        print(f'audit of {self.info.store_path} at {self.info.stamp}')
        counts = ' '.join(f'{k}={v:,}' for k, v in self.info.row_counts.items())
        print(f'rows: {counts}')
        for title, results in (('errors', self.errors),
            ('warnings', self.warnings)):
            print(f'\n{title}')
            for result in results:
                print(_summary_line(result))
        print(f'\noutlier groups: {len(self.outlier_groups):,}', end=' | ')
        print(f'speakers: {len(self.speakers):,}')

    def save(self, directory='.'):
        '''Write the report to audit_{stamp} in directory; return its path.
        directory:    parent folder; never inside the store's LMDB folder
        '''
        path = _make_audit_directory(directory, self.info.stamp,
            self.info.store_path)
        _write_json(path / 'summary.json', _summary_to_json(self))
        for result in self.results:
            _write_json(path / f'{result.name}.json', _result_to_json(result))
        _write_json(path / 'speakers.json', _records_to_json(self.speakers))
        _write_json(path / 'outlier_groups.json',
            _records_to_json(self.outlier_groups))
        return path


def _make_audit_directory(directory, stamp, store_path):
    '''Create the audit folder; never inside the store's LMDB folder.'''
    path = Path(directory) / f'audit_{stamp}'
    store_root = Path(store_path).resolve()
    if path.resolve().is_relative_to(store_root):
        raise ValueError(f'audit folder is inside the store: {path}')
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path, content):
    with open(path, 'w', encoding='utf-8') as fout:
        json.dump(content, fout)


def _read_json(path):
    with open(path, encoding='utf-8') as fin:
        return json.load(fin)


def _record_to_json(record):
    content = dict(vars(record))
    for field in BYTES_FIELDS:
        if content.get(field) is None: continue
        content[field] = content[field].hex()
    return content


def _records_to_json(records):
    return [_record_to_json(record) for record in records]


def _record_from_json(cls, content):
    for field in BYTES_FIELDS:
        if content.get(field) is not None:
            content[field] = bytes.fromhex(content[field])
    return cls(**content)


def _result_to_json(result):
    return {'name': result.name, 'severity': result.severity,
        'n_checked': result.n_checked, 'n_unjudged': result.n_unjudged,
        'findings': _records_to_json(result.findings)}


def _result_from_json(content):
    findings = [_record_from_json(Finding, f) for f in content['findings']]
    return CheckResult(content['name'], content['severity'],
        content['n_checked'], findings, content['n_unjudged'])


def _summary_to_json(report):
    counts = {r.name: r.n_findings for r in report.results}
    return {'info': vars(report.info), 'n_errors': report.n_errors,
        'n_warnings': report.n_warnings, 'finding_counts': counts}


def _summary_line(result):
    m = f'  {result.name:<32} {result.n_findings:>10,} '
    m += f'of {result.n_checked:>12,}'
    if result.n_unjudged: m += f'  ({result.n_unjudged:,} unjudged)'
    sizes = result.count_by('size')
    if sizes: m += f'  {sizes}'
    return m


def _check_name(group):
    '''Return the name of the outlier check that produced a group.'''
    return f'outliers_{group.level}'


def load_report(path):
    '''Load an AuditReport from a folder written by report.save.
    path:    an audit_{stamp} folder
    '''
    path = Path(path)
    summary = _read_json(path / 'summary.json')
    results = []
    for name in summary['finding_counts']:
        results.append(_result_from_json(_read_json(path / f'{name}.json')))
    speakers = _read_json(path / 'speakers.json')
    speakers = [_record_from_json(SpeakerTempo, s) for s in speakers]
    groups = _read_json(path / 'outlier_groups.json')
    groups = [OutlierGroup(**group) for group in groups]
    for result in results:
        result.groups = [g for g in groups if result.name == _check_name(g)]
    info = SimpleNamespace(**summary['info'])
    return AuditReport(results, speakers, groups, info)


class ReportComparison:
    '''Findings new in a later report and resolved since an earlier one.'''

    def __init__(self, before, after, new, resolved, severities):
        self.before_stamp = before.info.stamp
        self.after_stamp = after.info.stamp
        self.audio_ids = _report_scope(after)
        self.new = new
        self.resolved = resolved
        self.severities = severities

    def __repr__(self):
        m = f'<ReportComparison {self.before_stamp} -> {self.after_stamp} '
        m += f'new={self.n_new} resolved={self.n_resolved}>'
        return m

    @property
    def n_new(self):
        return sum(len(findings) for findings in self.new.values())

    @property
    def n_resolved(self):
        return sum(len(findings) for findings in self.resolved.values())

    @property
    def n_new_errors(self):
        counts = [len(f) for n, f in self.new.items() if self._is_error(n)]
        return sum(counts)

    def _is_error(self, name):
        return self.severities[name] == ERROR

    def summary(self):
        '''Print the new and resolved findings per check that changed.'''
        scope = 'all recordings'
        if self.audio_ids is not None:
            scope = f'{len(self.audio_ids):,} recordings'
        print(f'{self.before_stamp} -> {self.after_stamp} ({scope})')
        for name in self.severities:
            n_new, n_resolved = len(self.new[name]), len(self.resolved[name])
            if not n_new and not n_resolved: continue
            m = f'  {name:<32} {self.severities[name]:<8} '
            m += f'new {n_new:>8,}  resolved {n_resolved:>8,}'
            print(m)
        print(f'new errors: {self.n_new_errors:,}')


def compare_reports(before, after, include_outliers=True):
    '''Compare the findings of two audits of one store, per check.
    before:              earlier AuditReport, full or scoped
    after:               later AuditReport; when scoped, before is limited
                         to its recordings for comparison
    include_outliers:    compare population outliers too; their fences may
                         shift when other durations or group sizes change

    Only checks present in both reports are compared; a scoped audit has no
    outlier checks. Across different scopes, missing_row and other_audio
    parent findings share an identity; their displayed kinds are preserved.
    '''
    audio_ids = _report_scope(after)
    before_ids = _report_scope(before)
    if before_ids is not None and not _covers(before_ids, audio_ids):
        raise ValueError('before does not cover the recordings of after')
    earlier = {result.name: result for result in before.results}
    new, resolved, severities = {}, {}, {}
    for result in after.results:
        if result.name not in earlier: continue
        if not include_outliers and result.name.startswith('outliers_'):
            continue
        old = _scoped_findings(earlier[result.name].findings, audio_ids)
        normalize_parent = result.name == 'parent_exists'
        normalize_parent = normalize_parent and before_ids != audio_ids
        new[result.name] = _missing_from(result.findings, old,
            normalize_parent)
        resolved[result.name] = _missing_from(old, result.findings,
            normalize_parent)
        severities[result.name] = result.severity
    return ReportComparison(before, after, new, resolved, severities)


def _report_scope(report):
    '''Return the audited recordings as a set of bytes, or None for all.'''
    audio_ids = getattr(report.info, 'audio_ids', None)
    if audio_ids is None: return None
    return {bytes.fromhex(audio_id) for audio_id in audio_ids}


def _covers(before_ids, after_ids):
    return after_ids is not None and after_ids <= before_ids


def _finding_identity(finding, normalize_parent):
    values = []
    for field in IDENTITY_FIELDS:
        value = getattr(finding, field, None)
        if normalize_parent and field == 'kind':
            if value in ('other_audio', 'missing_row'): value = 'missing_row'
        values.append(value)
    return tuple(values)


def _finding_audio_id(finding):
    audio_id = getattr(finding, 'audio_id', None)
    if audio_id is not None: return audio_id
    key = getattr(finding, 'key', None)
    return None if key is None else segment_key_audio_id(key)


def _scoped_findings(findings, audio_ids):
    if audio_ids is None: return findings
    return [f for f in findings if _finding_audio_id(f) in audio_ids]


def _missing_from(findings, others, normalize_parent=False):
    '''Return the findings with no finding of the same identity in others.'''
    identities = set()
    for other in others:
        identities.add(_finding_identity(other, normalize_parent))
    missing = []
    for finding in findings:
        if _finding_identity(finding, normalize_parent) not in identities:
            missing.append(finding)
    return missing
