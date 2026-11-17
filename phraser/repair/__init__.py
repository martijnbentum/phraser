'''General repair helpers with compact byte evidence and small receipts.

Create a bounded proposal, inspect its summary, save it, then apply and
choose relevant domain checks. Immediate undo is optional. Saved
bytes support reviewed reconstruction, not a guarantee of safe restoration.
The agent chooses repair policy and grouping; no session manager is needed.
'''

from .assessment import plan_duration_assessments
from .changes import ChangePlan, add_change, apply_change_plan
from .changes import check_change_state, create_change_plan, load_change_plan
from .changes import merge_change_plan, plan_audio_ids, rollback_changes
from .changes import save_change_plan
from .records import save_record, write_receipt
from .segment_changes import plan_delete, plan_update

from .verification import audit_change_plan, verification_checks
from .verification import verify_change_plan

__all__ = ['plan_duration_assessments', 'audit_change_plan',
    'verification_checks', 'verify_change_plan', 'ChangePlan', 'add_change',
    'apply_change_plan',
    'check_change_state', 'create_change_plan', 'load_change_plan',
    'merge_change_plan', 'plan_audio_ids', 'plan_delete',
    'plan_update', 'rollback_changes', 'save_change_plan', 'save_record',
    'write_receipt']
