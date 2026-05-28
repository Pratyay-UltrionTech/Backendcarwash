from app.database import SessionLocal
from app.models.booking import BranchBooking
from app.models.mobile import MobileBooking
from app.models.branch import Branch
from sqlalchemy import func

db = SessionLocal()
try:
    print("--- Branch Bookings ---")
    b_count = db.query(BranchBooking).count()
    b_completed = db.query(BranchBooking).filter(BranchBooking.status == "completed").count()
    print(f"Total: {b_count}, Completed: {b_completed}")
    
    latest_b = db.query(BranchBooking).order_by(BranchBooking.created_at.desc()).first()
    if latest_b:
        print(f"Latest Branch Booking: ID={latest_b.id}, Status={latest_b.status}, CreatedAt={latest_b.created_at}, CompletedAt={latest_b.completed_at}")
        
    print("\n--- Mobile Bookings ---")
    m_count = db.query(MobileBooking).count()
    m_completed = db.query(MobileBooking).filter(MobileBooking.status == "completed").count()
    print(f"Total: {m_count}, Completed: {m_completed}")
    
    latest_m = db.query(MobileBooking).order_by(MobileBooking.created_at.desc()).first()
    if latest_m:
        print(f"Latest Mobile Booking: ID={latest_m.id}, Status={latest_m.status}, CreatedAt={latest_m.created_at}, CompletedAt={latest_m.completed_at}")

finally:
    db.close()
