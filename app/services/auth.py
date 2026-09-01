from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.models import Usuario

settings   = get_settings()
pwd_ctx    = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2     = OAuth2PasswordBearer(tokenUrl="/auth/login")


def hash_password(plain: str) -> str:
    return pwd_ctx.hash(plain)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.access_token_expire_minutes)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )

@dataclass
class PrincipalHoras:
    """Usuario autenticado con token de Horas — solo lectura en Etapa 1."""
    nombre: str
    rol: str                      # admin | gerente | jefe (ya mapeado)
    contratos_list: list = field(default_factory=list)
    ver_incidencia: bool = False
    id: int = 0                   # sin fila en la BD del portal
    email: str = ""                # usado por /auth/me

_NIVEL_A_ROL = {"admin": "admin", "lectura": "gerente", "carga": "jefe"}

def principal_desde_token_horas(payload: dict) -> PrincipalHoras:
    """Mapea el claim `cert` del token de Horas a un principal de lectura del portal."""
    cert = payload.get("cert")
    if not cert or cert.get("nivel") not in _NIVEL_A_ROL:
        raise HTTPException(status_code=403, detail="Sin acceso al módulo Certificaciones")
    return PrincipalHoras(
        nombre=payload.get("email", ""),
        rol=_NIVEL_A_ROL[cert["nivel"]],
        contratos_list=[k.upper() for k in cert.get("ks", [])],
        ver_incidencia=bool(cert.get("inc")),
        email=payload.get("email", ""),
    )

def decode_any_token(token: str) -> dict:
    """Prueba primero el secret del portal (legacy); el de Horas solo si está configurado."""
    try:
        return jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    except JWTError:
        pass
    if settings.horas_jwt_secret:
        try:
            return jwt.decode(token, settings.horas_jwt_secret, algorithms=["HS256"])
        except JWTError:
            pass
    raise HTTPException(status_code=401, detail="Token inválido o expirado",
                        headers={"WWW-Authenticate": "Bearer"})

def get_usuario_by_email(db: Session, email: str) -> Optional[Usuario]:
    return db.query(Usuario).filter(
        Usuario.email == email.lower(),
        Usuario.activo == True
    ).first()

def authenticate(db: Session, email: str, password: str) -> Optional[Usuario]:
    user = get_usuario_by_email(db, email)
    if not user or not verify_password(password, user.password):
        return None
    return user


def get_current_user(
    token: str = Depends(oauth2),
    db: Session = Depends(get_db),
) -> "Usuario | PrincipalHoras":
    payload = decode_any_token(token)
    if "cuil" in payload:
        return principal_desde_token_horas(payload)
    email: str = payload.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Token sin usuario")
    user = get_usuario_by_email(db, email)
    if not user:
        raise HTTPException(status_code=401, detail="Usuario no encontrado")
    return user

def require_admin(current: Usuario = Depends(get_current_user)) -> Usuario:
    if current.rol != "admin":
        raise HTTPException(status_code=403, detail="Se requiere rol admin")
    return current

def require_jefe_or_admin(current: Usuario = Depends(get_current_user)) -> Usuario:
    return current  # cualquier usuario autenticado pasa

def check_contrato_access(usuario: Usuario, codigo_k: str) -> None:
    """Verifica que el usuario tenga acceso al contrato K solicitado."""
    if usuario.rol == "admin":
        return
    if codigo_k.upper() not in usuario.contratos_list:
        raise HTTPException(
            status_code=403,
            detail=f"No tenés acceso al contrato {codigo_k}"
        )
