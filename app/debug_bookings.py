from app.database import SessionLocal
from app.models import BranchBooking, MobileBooking
from datetime import datetime, timezone, timedelta

db = SessionLocal()
try:
    now = datetime.now(timezone.utc)
    b_all = db.query(BranchBooking).count()
    m_all = db.query(MobileBooking).count()
    
    b_comp = db.query(BranchBooking).filter(BranchBooking.status == "completed").count()
    m_comp = db.query(MobileBooking).filter(MobileBooking.status == "completed").count()
    
    print(f"Total Branch Bookings: {b_all}")
    print(f"Total Mobile Bookings: {m_all}")
    print(f"Completed Branch: {b_comp}")
    print(f"Completed Mobile: {m_comp}")
    
    if b_all > 0:
        b = db.query(BranchBooking).first()
        print(f"Sample Booking: ID={b.id}, Status={b.status}, CompletedAt={b.completed_at}")
        
finally:
    db.close()
