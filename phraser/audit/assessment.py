'''Describe stored duration assessments without changing them.'''

from .findings import _finding


def annotate_duration(result, segments, judgments):
    '''Attach assessment agreement and retain only discrepancies as records.'''
    result.judgments = judgments
    by_key = {finding.key: finding for finding in result.findings}
    for segment in segments:
        expected = judgments.get(segment.key)
        stored = segment.duration_outlier
        agreement = assessment_agreement(stored, expected)
        counts = result.assessment_counts
        counts[agreement] = counts.get(agreement, 0) + 1
        finding = by_key.get(segment.key)
        if finding is not None:
            _annotate(finding, segment, expected, agreement)
        elif stored != 'unset' and agreement != 'matching':
            finding = _finding(segment)
            _annotate(finding, segment, expected, agreement)
            result.assessments.append(finding)
    return result


def assessment_agreement(stored, expected):
    if expected is None: return 'unjudged'
    if stored == 'unset': return 'unset'
    return 'matching' if stored == expected else 'conflicting'


def _annotate(finding, segment, expected, agreement):
    finding.stored_assessment = segment.duration_outlier
    finding.current_assessment = expected
    finding.assessment_status = agreement
    finding.timing_uncertain = segment.timing_uncertain
    finding.status = 'needs_attention'
    if agreement == 'matching': finding.status = 'acknowledged'
