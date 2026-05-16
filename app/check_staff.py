from app.database import SessionLocal
from app.models.mobile import MobileServiceDriver
from app.models.staff import Washer






db = SessionLocal()
try:
    w_count = db.query(Washer).count()
    d_count = db.query(MobileServiceDriver).count()
    print(f"Washers: {w_count}")
    print(f"Drivers: {d_count}")
    
    if d_count > 0:
        d = db.query(MobileServiceDriver).first()
        print(f"Sample Driver: {d.emp_name} (ID: {d.id})")
finally:
    db.close()
