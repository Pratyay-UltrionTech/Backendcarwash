from sqlalchemy.orm import Session
from app.models import CustomerUser
from app.services.jsonutil import dumps_json, loads_json_array

def record_customer_vehicle(
    db: Session,
    customer_id: str,
    vehicle_type: str,
    vehicle_model: str,
    registration_number: str = "",
):
    if not customer_id or not vehicle_type or not vehicle_model:
        return
    
    user = db.query(CustomerUser).filter(CustomerUser.id == customer_id).one_or_none()
    if not user:
        return
    
    vehicles = loads_json_array(user.vehicles_json)
    reg = str(registration_number or "").strip().upper()
    
    # Update existing type or add new
    found = False
    for v in vehicles:
        if v.get("type") == vehicle_type and (v.get("model") or "").strip() == vehicle_model.strip():
            v["model"] = vehicle_model
            if reg:
                v["number"] = reg
            found = True
            break
    
    if not found:
        entry: dict[str, str] = {"type": vehicle_type, "model": vehicle_model}
        if reg:
            entry["number"] = reg
        vehicles.append(entry)
    
    user.vehicles_json = dumps_json(vehicles)
    db.add(user)
