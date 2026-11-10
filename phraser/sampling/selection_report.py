'''Counts and readable summaries of sequential sampling restrictions.'''

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SelectionStage:
    '''Counts before and after one condition, excluding earlier losses.'''

    condition: str
    before: int
    after: int

    @property
    def removed(self):
        return self.before - self.after


@dataclass
class SelectionReport:
    '''Sequential eligibility losses and the final random draw.
    population:       description of the sampled object level
    starting_count:   unique candidates within the provided source material
    requested_count:  number requested for the final draw
    type_label:       requested segment label, or None for all labels
    '''

    population: str
    starting_count: int
    requested_count: int
    type_label: str | None = None
    stages: list[SelectionStage] = field(default_factory=list)
    selected_count: int = 0

    @property
    def eligible_count(self):
        return self.stages[-1].after if self.stages else self.starting_count

    @property
    def shortfall(self):
        return max(0, self.requested_count - self.eligible_count)

    @property
    def unselected_count(self):
        '''Eligible candidates not included in the final draw.'''
        return self.eligible_count - self.selected_count

    def add_stage(self, condition, before, after):
        '''Append a stage whose input is the preceding stage's survivors.'''
        if before != self.eligible_count or not 0 <= after <= before:
            raise ValueError('stage counts must follow the previous stage')
        self.stages.append(SelectionStage(condition, before, after))

    def __str__(self):
        title = f'Selection: {self.population}'
        if self.type_label is not None: title += f' ({self.type_label!r})'
        lines = [title, f'Starting: {self.starting_count:,}']
        for stage in self.stages:
            lines.append(f'{stage.condition}: {stage.after:,} '
                f'(removed {stage.removed:,})')
        lines.extend([f'Eligible: {self.eligible_count:,}',
            f'Requested: {self.requested_count:,}',
            f'Selected: {self.selected_count:,}',
            f'Eligible but unselected: {self.unselected_count:,}',
            'Losses are sequential; each candidate is removed only once.'])
        if self.shortfall: lines.append(f'Shortfall: {self.shortfall:,}')
        return '\n'.join(lines)


class InsufficientSampleError(ValueError):
    '''A requested draw exceeds eligibility; inspect report for losses.'''

    def __init__(self, report):
        message = f'requested {report.requested_count} items but only '
        message += f'{report.eligible_count} are eligible'
        super().__init__(message)
        self.report = report
