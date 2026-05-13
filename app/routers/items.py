from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional

from app.database import get_db
from app.models import Usuario
from app.services.auth import require_admin

router = APIRouter(prefix="/items", tags=["items"])


class ItemCreate(BaseModel):
    item_codigo:    str
    codigo_k:       str
    grupo:          Optional[str] = None
    subgrupo:       Optional[str] = None
    tarea:          Optional[str] = None
    frecuencia:     Optional[str] = None
    contratista:    Optional[str] = None
    ptos_gasnor:    Optional[float] = None
    unidad_medida:  Optional[str] = None
    tipo:           Optional[str] = None
    contrato_nombre:Optional[str] = None


class ItemUpdate(BaseModel):
    grupo:          Optional[str] = None
    subgrupo:       Optional[str] = None
    tarea:          Optional[str] = None
    frecuencia:     Optional[str] = None
    contratista:    Optional[str] = None
    ptos_gasnor:    Optional[float] = None
    unidad_medida:  Optional[str] = None
    tipo:           Optional[str] = None
    contrato_nombre:Optional[str] = None
    codigo_k:       Optional[str] = None


@router.get("/")
def listar_items(
    codigo_k: Optional[str] = None,
    buscar:   Optional[str] = None,
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Lista ítems filtrados por contrato y/o búsqueda de código/tarea."""
    filtros = "WHERE 1=1"
    params  = {}

    if codigo_k:
        filtros += " AND dc.codigo_k = :k"
        params["k"] = codigo_k.upper()

    if buscar:
        filtros += " AND (di.item_codigo LIKE :b OR di.tarea LIKE :b)"
        params["b"] = f"%{buscar}%"

    rows = db.execute(text(f"""
        SELECT
            di.id_item, di.item_codigo, dc.codigo_k,
            di.grupo, di.subgrupo, di.tarea,
            di.frecuencia, di.contratista,
            di.ptos_gasnor, di.unidad_medida, di.tipo,
            di.contrato_nombre
        FROM dim_item di
        JOIN dim_contrato dc ON di.id_contrato = dc.id_contrato
        {filtros}
        ORDER BY dc.codigo_k, di.item_codigo
        LIMIT 500
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


@router.post("/", status_code=201)
def crear_item(
    data: ItemCreate,
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    # Verificar que el contrato existe
    contrato = db.execute(text(
        "SELECT id_contrato FROM dim_contrato WHERE codigo_k = :k"
    ), {"k": data.codigo_k.upper()}).fetchone()

    if not contrato:
        raise HTTPException(400, f"Contrato {data.codigo_k} no encontrado")

    # Verificar que no existe el mismo item en ese contrato
    existente = db.execute(text("""
        SELECT id_item FROM dim_item
        WHERE REPLACE(item_codigo,'.', ',') = REPLACE(:codigo, '.', ',')
          AND id_contrato = :id_contrato
    """), {"codigo": data.item_codigo, "id_contrato": contrato[0]}).fetchone()

    if existente:
        raise HTTPException(400, f"El ítem {data.item_codigo} ya existe en {data.codigo_k}")

    db.execute(text("""
        INSERT INTO dim_item (
            item_codigo, id_contrato, grupo, subgrupo, tarea,
            frecuencia, contratista, ptos_gasnor,
            unidad_medida, tipo, contrato_nombre
        ) VALUES (
            :item_codigo, :id_contrato, :grupo, :subgrupo, :tarea,
            :frecuencia, :contratista, :ptos_gasnor,
            :unidad_medida, :tipo, :contrato_nombre
        )
    """), {
        "item_codigo":     data.item_codigo,
        "id_contrato":     contrato[0],
        "grupo":           data.grupo,
        "subgrupo":        data.subgrupo,
        "tarea":           data.tarea,
        "frecuencia":      data.frecuencia,
        "contratista":     data.contratista,
        "ptos_gasnor":     data.ptos_gasnor,
        "unidad_medida":   data.unidad_medida,
        "tipo":            data.tipo,
        "contrato_nombre": data.contrato_nombre,
    })
    db.commit()
    return {"mensaje": f"Ítem {data.item_codigo} creado en {data.codigo_k}"}


@router.patch("/{id_item}")
def actualizar_item(
    id_item: int,
    data: ItemUpdate,
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    # Verificar que existe
    item = db.execute(text(
        "SELECT id_item FROM dim_item WHERE id_item = :id"
    ), {"id": id_item}).fetchone()

    if not item:
        raise HTTPException(404, "Ítem no encontrado")

    # Construir SET dinámico solo con campos enviados
    campos = {k: v for k, v in data.model_dump().items() if v is not None and k != "codigo_k"}

    if data.codigo_k:
        contrato = db.execute(text(
            "SELECT id_contrato FROM dim_contrato WHERE codigo_k = :k"
        ), {"k": data.codigo_k.upper()}).fetchone()
        if not contrato:
            raise HTTPException(400, f"Contrato {data.codigo_k} no encontrado")
        campos["id_contrato"] = contrato[0]

    if not campos:
        return {"mensaje": "Sin cambios"}

    set_clause = ", ".join(f"{k} = :{k}" for k in campos)
    campos["id"] = id_item

    db.execute(text(f"UPDATE dim_item SET {set_clause} WHERE id_item = :id"), campos)
    db.commit()
    return {"mensaje": "Ítem actualizado"}


@router.delete("/{id_item}")
def eliminar_item(
    id_item: int,
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    # Verificar que no tenga certificaciones asociadas
    en_uso = db.execute(text(
        "SELECT COUNT(*) FROM fact_certificaciones WHERE id_item = :id"
    ), {"id": id_item}).scalar()

    if en_uso > 0:
        raise HTTPException(
            400,
            f"No se puede eliminar: el ítem tiene {en_uso} certificaciones cargadas"
        )

    db.execute(text("DELETE FROM dim_item WHERE id_item = :id"), {"id": id_item})
    db.commit()
    return {"mensaje": "Ítem eliminado"}


@router.get("/contratos")
def listar_contratos(
    _: Usuario = Depends(require_admin),
    db: Session = Depends(get_db),
):
    rows = db.execute(text(
        "SELECT id_contrato, codigo_k FROM dim_contrato ORDER BY codigo_k"
    )).fetchall()
    return [dict(r._mapping) for r in rows]