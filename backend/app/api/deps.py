from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer
from jose import JWTError
from app.core.security import decode_token

security = HTTPBearer()

def require_admin(credentials=Depends(security)):
    token = credentials.credentials

    try:
        payload = decode_token(token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")

    return payload