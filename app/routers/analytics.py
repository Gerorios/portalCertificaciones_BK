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
               LEFT(fc.tarea, 60) AS tarea,
               dc.codigo_k        AS contrato,
               SUM(fc.total_mes)  AS monto_total
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
    Compara mes a mes el año actual vs el año anterior.
    Devuelve para cada mes: monto año actual, monto año anterior y variación %.
    """
    f, p = "", {}
    if contrato:
        f = " AND dc.codigo_k = :contrato"
        p["contrato"] = contrato

    rows = db.execute(text(f"""
        SELECT
            YEAR(fc.fecha)                    AS anio,
            MONTH(fc.fecha)                   AS mes,
            DATE_FORMAT(fc.fecha, '%Y-%m')    AS periodo,
            SUM(fc.total_mes)                 AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE YEAR(fc.fecha) IN (YEAR(CURDATE()), YEAR(CURDATE()) - 1)
          {f}
        GROUP BY anio, mes, periodo
        ORDER BY mes ASC, anio ASC
    """), p).fetchall()

    # Organizar por mes
    datos = {}
    anio_actual   = None
    anio_anterior = None

    for r in rows:
        d = dict(r._mapping)
        if anio_actual is None or d["anio"] > anio_actual:
            anio_actual = d["anio"]
        if anio_anterior is None or (d["anio"] < anio_actual and d["anio"] > (anio_anterior or 0)):
            anio_anterior = d["anio"]

    for r in rows:
        d    = dict(r._mapping)
        mes  = d["mes"]
        anio = d["anio"]
        if mes not in datos:
            datos[mes] = {"mes": mes, "actual": None, "anterior": None}
        if anio == anio_actual:
            datos[mes]["actual"]   = float(d["monto_total"] or 0)
            datos[mes]["periodo_actual"] = d["periodo"]
        else:
            datos[mes]["anterior"] = float(d["monto_total"] or 0)
            datos[mes]["periodo_anterior"] = d["periodo"]

    resultado = []
    for mes in sorted(datos.keys()):
        d        = datos[mes]
        actual   = d.get("actual")
        anterior = d.get("anterior")
        variacion = None
        if actual is not None and anterior is not None and anterior > 0:
            variacion = round((actual - anterior) / anterior * 100, 1)
        resultado.append({
            "mes":              mes,
            "actual":           actual,
            "anterior":         anterior,
            "variacion_pct":    variacion,
            "anio_actual":      anio_actual,
            "anio_anterior":    anio_anterior,
        })

    return resultado


@router.get("/contratos")
def contratos(
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    rows = db.execute(text(
        "SELECT codigo_k FROM dim_contrato ORDER BY codigo_k"
    )).fetchall()
    return [r[0] for r in rows]