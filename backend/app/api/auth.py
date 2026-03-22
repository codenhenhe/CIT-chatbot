from fastapi import APIRouter, HTTPException
from app.core.security import create_access_token

router = APIRouter()

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "123456"

@router.post("/login")
def login(data: dict):
    if data["username"] != ADMIN_USERNAME or data["password"] != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({
        "sub": "admin",
        "role": "admin"
    })

    return {"access_token": token}