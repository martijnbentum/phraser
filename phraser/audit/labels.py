'''Checks on phone and syllable labels and syllable positions.

Phone labels must be known phones with a syllable position, each syllable
needs a nucleus, and a syllable label must equal its joined phone labels.
'''

from ..phone_types import PHONE_TYPES
from .findings import WARNING, CheckResult, _finding
from .store_snapshot import _speech_phones

UNASSIGNED_POSITION = 9
NUCLEUS_POSITION = 2


def check_unknown_phone_labels(data):
    '''Find speech phones whose label is not a known phone.'''
    phones = _speech_phones(data)
    findings = []
    for phone in phones:
        if phone.label not in PHONE_TYPES: findings.append(_finding(phone))
    return CheckResult('unknown_phone_labels', WARNING, len(phones), findings)


def check_phone_positions(data):
    '''Find speech phones without an assigned syllable position.'''
    phones = _speech_phones(data)
    findings = []
    for phone in phones:
        if phone.position_code != UNASSIGNED_POSITION: continue
        findings.append(_finding(phone))
    return CheckResult('phone_positions', WARNING, len(phones), findings)


def check_syllable_nucleus(data):
    '''Find syllables with phones but no nucleus phone.'''
    findings = []
    n_checked = 0
    for syllable in data.syllables:
        phones = data.children.get(syllable.identifier)
        if phones is None: continue
        n_checked += 1
        codes = {phone.position_code for phone in phones}
        if NUCLEUS_POSITION in codes: continue
        unassigned = codes == {UNASSIGNED_POSITION}
        kind = 'positions_unassigned' if unassigned else 'no_nucleus'
        findings.append(_finding(syllable, kind=kind))
    return CheckResult('syllable_nucleus', WARNING, n_checked, findings)


def check_syllable_label_matches_phones(data):
    '''Find syllables whose label differs from their joined phone labels.
    Loaders write 'ru' or 'r u', so spaces in the label are ignored.'''
    findings = []
    n_checked = 0
    for syllable in data.syllables:
        phones = data.children.get(syllable.identifier)
        if phones is None: continue
        n_checked += 1
        joined = ''.join(phone.label for phone in phones)
        if joined == syllable.label.replace(' ', ''): continue
        findings.append(_finding(syllable, phone_labels=joined))
    name = 'syllable_label_matches_phones'
    return CheckResult(name, WARNING, n_checked, findings)
