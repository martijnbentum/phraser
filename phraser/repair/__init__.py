'''Reviewed store edits with durable reconstruction records beside an audit.

Create a proposal, inspect its summary and changes, save it, then apply and
re-audit. Immediate undo is a convenience for an unchanged Store. Saved
bytes support reviewed reconstruction, not a guarantee of safe restoration.
Bulk scripts can use save_record and write_receipt for a single progress log.
'''

from .changes import ChangePlan, add_change, apply_change_plan
from .changes import check_change_state, create_change_plan, load_change_plan
from .changes import plan_audio_ids, rollback_changes, save_change_plan
from .records import save_record, write_receipt
from .segment_changes import plan_delete, plan_update

__all__ = ['ChangePlan', 'add_change', 'apply_change_plan',
    'check_change_state', 'create_change_plan', 'load_change_plan',
    'plan_audio_ids', 'plan_delete',
    'plan_update', 'rollback_changes', 'save_change_plan', 'save_record',
    'write_receipt']
