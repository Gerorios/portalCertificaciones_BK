from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, EmailStr
from typing import Optional

from app.database import get_db
from app.models import Usuario
from app.services.auth import require_admin, hash_password

router = APIRouter(prefix="/admin", tags=["admin"])


class UsuarioCreate(BaseModel):
    nombre:    str
    email:     EmailStr
    password:  str
    rol:       str = "jefe"
    contratos: Optional[str] = None


class UsuarioUpdate(BaseModel):
    nombre:    Optional[str] = None
    rol:       Optional[str] = None
    contratos: Optional[str] = None
    activo:    Optional[bool] = None
    password:  Optional[str] = None


@router.get("/usuarios")
def listar_usuarios(
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    rows = db.query(Usuario).order_by(Usuario.id).all()
    return [
        {
            "id":        u.id,
            "nombre":    u.nombre,
            "email":     u.email,
            "rol":       u.rol,
            "contratos": u.contratos,
            "activo":    u.activo,
        }
        for u in rows
    ]


@router.post("/usuarios", status_code=201)
def crear_usuario(
    data: UsuarioCreate,
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    existente = db.query(Usuario).filter(Usuario.email == data.email.lower()).first()
    if existente:
        raise HTTPException(400, "Ya existe un usuario con ese email")

    u = Usuario(
        nombre   = data.nombre,
        email    = data.email.lower(),
        password = hash_password(data.password),
        rol      = data.rol,
        contratos= data.contratos,
        activo   = True,
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    return {"id": u.id, "mensaje": "Usuario creado"}


@router.patch("/usuarios/{user_id}")
def actualizar_usuario(
    user_id: int,
    data: UsuarioUpdate,
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    u = db.query(Usuario).filter(Usuario.id == user_id).first()
    if not u:
        raise HTTPException(404, "Usuario no encontrado")

    if data.nombre    is not None: u.nombre    = data.nombre
    if data.rol       is not None: u.rol       = data.rol
    if data.contratos is not None: u.contratos = data.contratos
    if data.activo    is not None: u.activo    = data.activo
    if data.password  is not None: u.password  = hash_password(data.password)

    db.commit()
    return {"mensaje": "Usuario actualizado"}


@router.delete("/cargas/{log_id}")
def eliminar_carga(
    log_id: int,
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Elimina un lote de certificaciones por su log_id.
    Útil para deshacer una carga errónea.
    """
    row = db.execute(text(
        "SELECT archivo_nombre, periodo FROM carga_log WHERE id = :id"
    ), {"id": log_id}).fetchone()

    if not row:
        raise HTTPException(404, "Carga no encontrada")

    db.execute(text("""
        DELETE FROM fact_certificaciones
        WHERE archivo_origen = :archivo
          AND DATE_FORMAT(fecha, '%Y-%m') = :periodo
    """), {"archivo": row[0], "periodo": row[1]})

    db.execute(text("DELETE FROM carga_log WHERE id = :id"), {"id": log_id})
    db.commit()

    return {"mensaje": f"Carga '{row[0]}' eliminada ({row[1]})"}


@router.get("/estadisticas")
def estadisticas_globales(
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    total_filas = db.execute(text("SELECT COUNT(*) FROM fact_certificaciones")).scalar()
    total_monto = db.execute(text("SELECT SUM(total_mes) FROM fact_certificaciones")).scalar()
    contratos   = db.execute(text("""
        SELECT dc.codigo_k, COUNT(*) lineas, SUM(fc.total_mes) monto
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        GROUP BY dc.codigo_k ORDER BY dc.codigo_k
    """)).fetchall()

    return {
        "total_filas":  total_filas,
        "total_monto":  float(total_monto or 0),
        "por_contrato": [dict(r._mapping) for r in contratos],
    }
