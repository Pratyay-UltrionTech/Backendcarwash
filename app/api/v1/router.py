from fastapi import APIRouter

from app.api.v1 import (
    admin,
    admin_mobile,
    auth,
    customer,
    manager,
    manager_mobile,
    public,
    public_mobile,
    washer,
    washer_mobile,
)

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(auth.router)
api_router.include_router(admin.router)
api_router.include_router(admin_mobile.router)
api_router.include_router(manager.router)
api_router.include_router(manager_mobile.router)
api_router.include_router(washer.router)
api_router.include_router(washer_mobile.router)
api_router.include_router(customer.router)
api_router.include_router(public.router)
api_router.include_router(public_mobile.router)
