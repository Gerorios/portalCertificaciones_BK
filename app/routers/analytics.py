from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from app.database import get_db
from app.models import Usuario
from app.services.auth import get_current_user

router = APIRouter(prefix="/analytics", tags=["analytics"])


def require_gerente_or_admin(current: Usuario = Depends(get_current_user)) -> Usuario:
    if current.rol not in ("admin", "gerente"):
        raise HTTPException(403, "Se requiere rol gerente o admin")
    return current


@router.get("/evolucion-mensual")
def evolucion_mensual(
    desde: Optional[str] = None,   # formato: 2025-01
    hasta: Optional[str] = None,   # formato: 2026-05
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    filtro = ""
    params = {}
    if desde:
        filtro += " AND DATE_FORMAT(fc.fecha, '%Y-%m') >= :desde"
        params["desde"] = desde
    if hasta:
        filtro += " AND DATE_FORMAT(fc.fecha, '%Y-%m') <= :hasta"
        params["hasta"] = hasta

    rows = db.execute(text(f"""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m') AS periodo,
            SUM(fc.total_mes)              AS monto_total
        FROM fact_certificaciones fc
        WHERE 1=1 {filtro}
        GROUP BY periodo
        ORDER BY periodo ASC
    """), params).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/por-contrato-mes")
def por_contrato_mes(
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    filtro = ""
    params = {}
    if desde:
        filtro += " AND DATE_FORMAT(fc.fecha, '%Y-%m') >= :desde"
        params["desde"] = desde
    if hasta:
        filtro += " AND DATE_FORMAT(fc.fecha, '%Y-%m') <= :hasta"
        params["hasta"] = hasta

    rows = db.execute(text(f"""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m') AS periodo,
            dc.codigo_k                    AS contrato,
            SUM(fc.total_mes)              AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE 1=1 {filtro}
        GROUP BY periodo, dc.codigo_k
        ORDER BY periodo ASC, dc.codigo_k
    """), params).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/top-items")
def top_items(
    desde:  Optional[str] = None,
    hasta:  Optional[str] = None,
    limite: int = 10,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    filtro = ""
    params = {"limite": limite}
    if desde:
        filtro += " AND DATE_FORMAT(fc.fecha, '%Y-%m') >= :desde"
        params["desde"] = desde
    if hasta:
        filtro += " AND DATE_FORMAT(fc.fecha, '%Y-%m') <= :hasta"
        params["hasta"] = hasta

    rows = db.execute(text(f"""
        SELECT
            di.item_codigo,
            LEFT(fc.tarea, 60)  AS tarea,
            dc.codigo_k         AS contrato,
            SUM(fc.total_mes)   AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_item     di ON fc.id_item     = di.id_item
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE 1=1 {filtro}
        GROUP BY di.item_codigo, fc.tarea, dc.codigo_k
        ORDER BY monto_total DESC
        LIMIT :limite
    """), params).fetchall()
    return [dict(r._mapping) for r in rows]