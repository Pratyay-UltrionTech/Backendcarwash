import json
from app.database import SessionLocal, engine
from app.models.base import Base
from app.models.branch import Branch
from app.models.catalog import VehicleCatalogBlock, CatalogServiceItem, CatalogAddonItem
from app.models.customer import CustomerUser
from app.core.security import hash_password

def seed():
    print("Initializing tables...")
    Base.metadata.create_all(bind=engine)
    print("Seeding database...")
    db = SessionLocal()
    try:
        # 1. Create a Branch
        branch = db.query(Branch).filter(Branch.name == "Main Branch").first()
        if not branch:
            branch = Branch(
                name="Main Branch",
                location="123 Wash St, Clean City",
                zip_code="12345",
                bay_count=3
            )
            db.add(branch)
            db.flush()
            print(f"Created branch: {branch.name}")
        
        # 2. Create Vehicle Blocks
        sedan_block = db.query(VehicleCatalogBlock).filter(
            VehicleCatalogBlock.branch_id == branch.id, 
            VehicleCatalogBlock.vehicle_type == "Sedan"
        ).first()
        if not sedan_block:
            sedan_block = VehicleCatalogBlock(branch_id=branch.id, vehicle_type="Sedan")
            db.add(sedan_block)
            db.flush()
            print("Created Sedan block")
        
        suv_block = db.query(VehicleCatalogBlock).filter(
            VehicleCatalogBlock.branch_id == branch.id, 
            VehicleCatalogBlock.vehicle_type == "SUV"
        ).first()
        if not suv_block:
            suv_block = VehicleCatalogBlock(branch_id=branch.id, vehicle_type="SUV")
            db.add(suv_block)
            db.flush()
            print("Created SUV block")
            
        # 3. Create Services for Sedan
        if not db.query(CatalogServiceItem).filter(CatalogServiceItem.vehicle_block_id == sedan_block.id).first():
            services = [
                CatalogServiceItem(
                    vehicle_block_id=sedan_block.id,
                    name="Basic Wash",
                    price=25.0,
                    description_points=json.dumps(["Exterior Wash", "Hand Dry", "Tire Shine"]),
                    duration_minutes=30
                ),
                CatalogServiceItem(
                    vehicle_block_id=sedan_block.id,
                    name="Full Detail",
                    price=120.0,
                    description_points=json.dumps(["Interior Vacuum", "Wax Polish", "Engine Cleaning"]),
                    duration_minutes=120,
                    recommended=True
                )
            ]
            db.add_all(services)
            print("Added services for Sedan")

        # 4. Create Addons for Sedan
        if not db.query(CatalogAddonItem).filter(CatalogAddonItem.vehicle_block_id == sedan_block.id).first():
            addons = [
                CatalogAddonItem(
                    vehicle_block_id=sedan_block.id,
                    name="Fragrance",
                    price=5.0,
                    description_points=json.dumps(["Long lasting fresh scent"])
                ),
                CatalogAddonItem(
                    vehicle_block_id=sedan_block.id,
                    name="Pet Hair Removal",
                    price=15.0,
                    description_points=json.dumps(["Deep vacuum for pet owners"])
                )
            ]
            db.add_all(addons)
            print("Added addons for Sedan")

        # 5. Create a Dummy Customer
        customer_email = "test@example.com"
        if not db.query(CustomerUser).filter(CustomerUser.email == customer_email).first():
            customer = CustomerUser(
                email=customer_email,
                password_hash=hash_password("password123"),
                full_name="John Doe",
                phone="0123456789",
                vehicles_json=json.dumps([{"make": "Toyota", "model": "Camry", "type": "Sedan"}])
            )
            db.add(customer)
            print(f"Created customer: {customer_email}")

        db.commit()
        print("Seeding complete!")
    except Exception as e:
        print(f"Error during seeding: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed()
