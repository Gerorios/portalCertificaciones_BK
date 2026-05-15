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


def _filtros(desde, hasta, contrato):
    f, p = "", {}
    if desde:
        f += " AND DATE_FORMAT(fc.fecha, '%Y-%m') >= :desde"
        p["desde"] = desde
    if hasta:
        f += " AND DATE_FORMAT(fc.fecha, '%Y-%m') <= :hasta"
        p["hasta"] = hasta
    if contrato:
        f += " AND dc.codigo_k = :contrato"
        p["contrato"] = contrato
    return f, p


@router.get("/evolucion-mensual")
def evolucion_mensual(
    desde:    Optional[str] = None,
    hasta:    Optional[str] = None,
    contrato: Optional[str] = None,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    f, p = _filtros(desde, hasta, contrato)
    rows = db.execute(text(f"""
        SELECT DATE_FORMAT(fc.fecha, '%Y-%m') AS periodo,
               SUM(fc.total_mes)              AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE 1=1 {f}
        GROUP BY periodo ORDER BY periodo ASC
    """), p).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/por-contrato-mes")
def por_contrato_mes(
    desde:    Optional[str] = None,
    hasta:    Optional[str] = None,
    contrato: Optional[str] = None,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    f, p = _filtros(desde, hasta, contrato)
    rows = db.execute(text(f"""
        SELECT DATE_FORMAT(fc.fecha, '%Y-%m') AS periodo,
               dc.codigo_k                    AS contrato,
               SUM(fc.total_mes)              AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE 1=1 {f}
        GROUP BY periodo, dc.codigo_k
        ORDER BY periodo ASC, dc.codigo_k
    """), p).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/top-items")
def top_items(
    desde:    Optional[str] = None,
    hasta:    Optional[str] = None,
    contrato: Optional[str] = None,
    limite:   int = 10,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    f, p = _filtros(desde, hasta, contrato)
    p["limite"] = limite
    rows = db.execute(text(f"""
        SELECT di.item_codigo,
               LEFT(fc.tarea, 60)                        AS tarea,
               dc.codigo_k                               AS contrato,
               SUM(fc.total_mes)                         AS monto_total,
               SUM(fc.cantidades * di.ptos_gasnor)        AS pgn_total
        FROM fact_certificaciones fc
        JOIN dim_item     di ON fc.id_item     = di.id_item
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE 1=1 {f}
        GROUP BY di.item_codigo, fc.tarea, dc.codigo_k
        ORDER BY monto_total DESC
        LIMIT :limite
    """), p).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/interanual")
def interanual(
    contrato: Optional[str] = None,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """
    Compara mes a mes año actual vs año anterior.
    Devuelve facturación Y PGN certificado para cada mes y año.
    PGN = SUM(cantidades * ptos_gasnor del ítem)
    """
    f, p = "", {}
    if contrato:
        f = " AND dc.codigo_k = :contrato"
        p["contrato"] = contrato

    rows = db.execute(text(f"""
        SELECT
            YEAR(fc.fecha)                          AS anio,
            MONTH(fc.fecha)                         AS mes,
            DATE_FORMAT(fc.fecha, '%Y-%m')          AS periodo,
            SUM(fc.total_mes)                       AS monto_total,
            SUM(fc.cantidades * di.ptos_gasnor)     AS pgn_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        JOIN dim_item     di ON fc.id_item     = di.id_item
        WHERE YEAR(fc.fecha) IN (YEAR(CURDATE()), YEAR(CURDATE()) - 1)
          {f}
        GROUP BY anio, mes, periodo
        ORDER BY mes ASC, anio ASC
    """), p).fetchall()

    # Detectar años disponibles
    anios = sorted(set(dict(r._mapping)["anio"] for r in rows), reverse=True)
    anio_actual   = anios[0] if len(anios) > 0 else None
    anio_anterior = anios[1] if len(anios) > 1 else None

    # Organizar por mes
    datos = {}
    for r in rows:
        d   = dict(r._mapping)
        mes = d["mes"]
        if mes not in datos:
            datos[mes] = {
                "mes": mes,
                "monto_actual":    None, "monto_anterior":    None,
                "pgn_actual":      None, "pgn_anterior":      None,
                "var_monto":       None, "var_pgn":           None,
            }
        if d["anio"] == anio_actual:
            datos[mes]["monto_actual"] = float(d["monto_total"] or 0)
            datos[mes]["pgn_actual"]   = float(d["pgn_total"]   or 0)
        elif d["anio"] == anio_anterior:
            datos[mes]["monto_anterior"] = float(d["monto_total"] or 0)
            datos[mes]["pgn_anterior"]   = float(d["pgn_total"]   or 0)

    # Calcular variaciones
    for mes, d in datos.items():
        if d["monto_actual"] is not None and d["monto_anterior"] and d["monto_anterior"] > 0:
            d["var_monto"] = round((d["monto_actual"] - d["monto_anterior"]) / d["monto_anterior"] * 100, 1)
        if d["pgn_actual"] is not None and d["pgn_anterior"] and d["pgn_anterior"] > 0:
            d["var_pgn"] = round((d["pgn_actual"] - d["pgn_anterior"]) / d["pgn_anterior"] * 100, 1)

    return {
        "anio_actual":   anio_actual,
        "anio_anterior": anio_anterior,
        "meses":         [datos[m] for m in sorted(datos.keys())],
    }


@router.get("/contratos")
def contratos(
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    rows = db.execute(text(
        "SELECT codigo_k FROM dim_contrato ORDER BY codigo_k"
    )).fetchall()
    return [r[0] for r in rows]