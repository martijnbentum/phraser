'''Counts and readable summaries of sequential sampling restrictions.'''

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SelectionStage:
    '''Counts before and after one condition, excluding earlier losses.'''

    condition: str
    before: int
    after: int
    types_before: int | None = None
    types_after: int | None = None
    types_below_minimum: int | None = None

    @property
    def removed(self):
        return self.before - self.after

    @property
    def types_removed(self):
        if self.types_before is None: return None
        return self.types_before - self.types_after

    def describe(self, min_tokens_per_type=None):
        '''One report line; type counts only when recorded.'''
        text = f'{self.condition}: {self.after:,} (removed {self.removed:,})'
        if self.types_after is None: return text
        text += f'; {self.types_after:,} types '
        text += f'(removed {self.types_removed:,}'
        if self.types_below_minimum is not None:
            text += f'; {self.types_below_minimum:,} fell below '
            text += f'{min_tokens_per_type:,} tokens per type'
        return text + ')'


@dataclass
class SelectionReport:
    '''Sequential eligibility losses and the final random draw.
    population:           description of the sampled object level
    starting_count:       unique candidates within the source material
    requested_count:      number requested; None for eligibility-only reports
    type_label:           requested segment label, or None for all labels
    min_tokens_per_type:  lexicon minimum named in type losses, or None
    label_counts:         None, or a list filtering extends with per-label
                          survivor counts after each stage (lexicons only)
    '''

    population: str
    starting_count: int
    requested_count: int | None
    type_label: str | None = None
    stages: list[SelectionStage] = field(default_factory=list)
    selected_count: int | None = 0
    min_tokens_per_type: int | None = None
    label_counts: list | None = field(default=None, repr=False,
        compare=False)

    @property
    def eligible_count(self):
        return self.stages[-1].after if self.stages else self.starting_count

    @property
    def shortfall(self):
        if self.requested_count is None: return None
        return max(0, self.requested_count - self.eligible_count)

    @property
    def unselected_count(self):
        '''Eligible candidates not included in the final draw.'''
        if self.selected_count is None: return None
        return self.eligible_count - self.selected_count

    def add_stage(self, condition, before, after, types_before=None,
        types_after=None, types_below_minimum=None):
        '''Append a stage whose input is the preceding stage's survivors.'''
        if before != self.eligible_count or not 0 <= after <= before:
            raise ValueError('stage counts must follow the previous stage')
        self.stages.append(SelectionStage(condition, before, after,
            types_before, types_after, types_below_minimum))

    def __str__(self):
        title = f'Selection: {self.population}'
        if self.type_label is not None: title += f' ({self.type_label!r})'
        lines = [title, f'Starting: {self.starting_count:,}']
        for stage in self.stages:
            lines.append(stage.describe(self.min_tokens_per_type))
        lines.append(f'Eligible: {self.eligible_count:,}')
        if self.requested_count is not None:
            lines.append(f'Requested: {self.requested_count:,}')
        if self.selected_count is not None:
            lines.append(f'Selected: {self.selected_count:,}')
            lines.append(f'Eligible but unselected: {self.unselected_count:,}')
        lines.append('Losses are sequential; each candidate is removed once.')
        if self.shortfall: lines.append(f'Shortfall: {self.shortfall:,}')
        return '\n'.join(lines)


class InsufficientSampleError(ValueError):
    '''A requested draw exceeds eligibility; inspect report for losses.'''

    def __init__(self, report):
        message = f'requested {report.requested_count} items but only '
        message += f'{report.eligible_count} are eligible'
        super().__init__(message)
        self.report = report
