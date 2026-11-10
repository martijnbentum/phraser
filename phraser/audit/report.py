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

BYTES_FIELDS = ('key', 'other_key', 'audio_id', 'speaker_id')


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
