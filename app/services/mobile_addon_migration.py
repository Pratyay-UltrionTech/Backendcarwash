"""One-time style migration: per-vehicle mobile_catalog_addon_items → mobile_global_addon_items."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.mobile import MobileCatalogAddonItem, MobileGlobalAddonItem


def ensure_mobile_global_addons_migrated(db: Session) -> None:
    """If global mobile add-ons are empty but legacy per-block rows exist, copy then remove legacy rows."""
    if db.query(MobileGlobalAddonItem).count() > 0:
        return
    legacy = db.query(MobileCatalogAddonItem).all()
    if not legacy:
        return
    for row in legacy:
        db.add(
            MobileGlobalAddonItem(
                id=row.id,
                name=row.name,
                price=row.price,
                description_points=row.description_points,
                active=row.active,
            )
        )
    db.flush()
    db.query(MobileCatalogAddonItem).delete(synchronize_session=False)
    db.commit()
