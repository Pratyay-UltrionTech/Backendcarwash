import asyncio
from app.database import SessionLocal
from app.api.v1.admin import _calculate_reports

db = SessionLocal()
try:
    print("--- Branch Data (This Month) ---")
    data_branch = _calculate_reports(db, branch_id=None, mobile=False, period="month", service_type=None, vehicle_type=None, start_date=None, end_date=None)
    print("Bookings:", data_branch.get("bookings"))
    print("Revenue:", data_branch.get("revenue"))
    
    print("\n--- Mobile Data (This Month) ---")
    data_mobile = _calculate_reports(db, branch_id=None, mobile=True, period="month", service_type=None, vehicle_type=None, start_date=None, end_date=None)
    print("Bookings:", data_mobile.get("bookings"))
    print("Revenue:", data_mobile.get("revenue"))

except Exception as e:
    import traceback
    traceback.print_exc()
finally:
    db.close()
