from sqlalchemy.orm import Session

from app.models import Branch, BranchLoyalty, BranchSlotSettings


def ensure_branch_defaults(db: Session, branch: Branch) -> bool:
    """Create slot + loyalty defaults if missing. Returns True if rows were inserted."""
    changed = False
    if db.query(BranchSlotSettings).filter(BranchSlotSettings.branch_id == branch.id).one_or_none() is None:
        db.add(BranchSlotSettings(branch_id=branch.id))
        changed = True
    if db.query(BranchLoyalty).filter(BranchLoyalty.branch_id == branch.id).one_or_none() is None:
        db.add(BranchLoyalty(branch_id=branch.id, qualifying_service_count=10, tiers_json="[]"))
        changed = True
    if changed:
        db.flush()
    return changed
