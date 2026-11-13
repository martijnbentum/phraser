'''Named checks and the persisted data needed to run them.'''

from time import monotonic

SEGMENTS = ('Phrase', 'Word', 'Syllable', 'Phone')
DEPENDENCIES = {
    'start_before_end': SEGMENTS,
    'within_audio': ('Audio',) + SEGMENTS,
    'within_parent': ('Audio',) + SEGMENTS,
    'parent_exists': SEGMENTS,
    'identity_matches_parent': SEGMENTS,
    'siblings_do_not_overlap': SEGMENTS,
    'same_speaker_phrases': ('Phrase',),
    'speaker_exists': ('Speaker',) + SEGMENTS,
    'label_index': SEGMENTS + ('label_index',),
    'speaker_audio_links': ('Phrase', 'speaker_audio_links'),
    'orphans': ('Word', 'Syllable', 'Phone'),
    'childless': SEGMENTS,
    'children_span_parent': SEGMENTS,
    'unknown_phone_labels': ('Phone',),
    'phone_positions': ('Phone',),
    'syllable_nucleus': ('Syllable', 'Phone'),
    'syllable_label_matches_phones': ('Syllable', 'Phone'),
    'overlap_computed': SEGMENTS,
    'overlap_code_matches': SEGMENTS,
    'outliers_phone': ('Phone',),
    'outliers_syllable': ('Syllable', 'Phone'),
    'outliers_word': ('Word', 'Syllable', 'Phone'),
    'outliers_phrase': ('Phrase', 'Word'),
    'speaker_tempo': ('Speaker', 'Word', 'Syllable', 'Phone')}
POPULATION_CHECKS = tuple(n for n in DEPENDENCIES if n.startswith('outliers_'))
POPULATION_CHECKS += ('speaker_tempo',)


def select_checks(checks=None, scoped=False):
    '''Validate names and retain the standard check order.'''
    requested = set(DEPENDENCIES if checks is None else checks)
    if isinstance(checks, str): requested = {checks}
    unknown = requested - DEPENDENCIES.keys()
    if unknown: raise ValueError(f'Unknown audit checks: {sorted(unknown)}')
    skipped = {}
    if scoped:
        for name in POPULATION_CHECKS:
            if name not in requested: continue
            skipped[name] = 'requires the whole-store population'
            requested.remove(name)
    return [name for name in DEPENDENCIES if name in requested], skipped


def required_data(checks):
    required = set()
    for name in checks: required.update(DEPENDENCIES[name])
    return required


class Progress:
    '''Print phase starts and elapsed time, or send them to a callback.'''

    def __init__(self, output=True):
        self.output = output
        self.started = monotonic()

    def __call__(self, phase):
        if not self.output: return
        message = f'audit [{monotonic() - self.started:.1f}s] {phase}'
        if callable(self.output): self.output(message)
        else: print(message, flush=True)
