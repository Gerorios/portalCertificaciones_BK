from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.database import get_db
from app.services.auth import (
    authenticate, create_access_token, get_current_user
)
from app.models import Usuario

router = APIRouter(prefix="/auth", tags=["auth"])


class Token(BaseModel):
    access_token: str
    token_type: str
    usuario: dict


class PasswordChange(BaseModel):
    password_actual: str
    password_nuevo: str


@router.post("/login", response_model=Token)
def login(
    form: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db),
):
    user = authenticate(db, form.username, form.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token({"sub": user.email})
    return {
        "access_token": token,
        "token_type": "bearer",
        "usuario": {
            "id":        user.id,
            "nombre":    user.nombre,
            "email":     user.email,
            "rol":       user.rol,
            "contratos": user.contratos_list,
        },
    }


@router.get("/me")
def me(current: Usuario = Depends(get_current_user)):
    return {
        "id":        current.id,
        "nombre":    current.nombre,
        "email":     current.email,
        "rol":       current.rol,
        "contratos": current.contratos_list,
    }


@router.post("/cambiar-password")
def cambiar_password(
    data: PasswordChange,
    current: Usuario = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    from app.services.auth import verify_password, hash_password
    if not verify_password(data.password_actual, current.password):
        raise HTTPException(status_code=400, detail="Contraseña actual incorrecta")
    current.password = hash_password(data.password_nuevo)
    db.commit()
    return {"mensaje": "Contraseña actualizada"}
