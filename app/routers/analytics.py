"""
Router de analytics — datos agregados para el dashboard del gerente.
Solo lectura, sin modificaciones.
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db
from app.models import Usuario
from app.services.auth import get_current_user

router = APIRouter(prefix="/analytics", tags=["analytics"])


def require_gerente_or_admin(current: Usuario = Depends(get_current_user)) -> Usuario:
    if current.rol not in ("admin", "gerente"):
        from fastapi import HTTPException
        raise HTTPException(403, "Se requiere rol gerente o admin")
    return current


@router.get("/evolucion-mensual")
def evolucion_mensual(
    anios: int = 2,  # cuántos años hacia atrás
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """Evolución del total facturado mes a mes — para gráfico de línea."""
    rows = db.execute(text("""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m')  AS periodo,
            SUM(fc.total_mes)               AS monto_total,
            COUNT(*)                         AS lineas
        FROM fact_certificaciones fc
        WHERE fc.fecha >= DATE_SUB(CURDATE(), INTERVAL :anios YEAR)
        GROUP BY periodo
        ORDER BY periodo ASC
    """), {"anios": anios}).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/por-contrato-mes")
def por_contrato_mes(
    anios: int = 1,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """Total por contrato y mes — para gráfico de barras agrupadas."""
    rows = db.execute(text("""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m')  AS periodo,
            dc.codigo_k                      AS contrato,
            SUM(fc.total_mes)                AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE fc.fecha >= DATE_SUB(CURDATE(), INTERVAL :anios YEAR)
        GROUP BY periodo, dc.codigo_k
        ORDER BY periodo ASC, dc.codigo_k
    """), {"anios": anios}).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/opex-capex")
def opex_capex(
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """Distribución OPEX vs CAPEX histórica — para gráfico de dona."""
    rows = db.execute(text("""
        SELECT
            fc.tipo,
            SUM(fc.total_mes)   AS monto_total,
            COUNT(*)             AS lineas
        FROM fact_certificaciones fc
        WHERE fc.tipo IS NOT NULL
        GROUP BY fc.tipo
    """)).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/top-items")
def top_items(
    limite: int = 10,
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """Top ítems por monto total certificado."""
    rows = db.execute(text("""
        SELECT
            di.item_codigo,
            LEFT(fc.tarea, 60)   AS tarea,
            dc.codigo_k          AS contrato,
            SUM(fc.total_mes)    AS monto_total,
            SUM(fc.cantidades)   AS cantidad_total
        FROM fact_certificaciones fc
        JOIN dim_item     di ON fc.id_item     = di.id_item
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        GROUP BY di.item_codigo, fc.tarea, dc.codigo_k
        ORDER BY monto_total DESC
        LIMIT :limite
    """), {"limite": limite}).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/por-provincia")
def por_provincia(
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """Total certificado por provincia."""
    rows = db.execute(text("""
        SELECT
            pv.provincia,
            SUM(fc.total_mes)   AS monto_total,
            COUNT(*)             AS lineas
        FROM fact_certificaciones fc
        JOIN ma_provincias pv ON fc.id_provincia = pv.id
        GROUP BY pv.provincia
        ORDER BY monto_total DESC
    """)).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/kpis")
def kpis(
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """KPIs globales para las métricas del header."""
    total_monto = db.execute(text(
        "SELECT SUM(total_mes) FROM fact_certificaciones"
    )).scalar() or 0

    total_lineas = db.execute(text(
        "SELECT COUNT(*) FROM fact_certificaciones"
    )).scalar() or 0

    contratos_activos = db.execute(text("""
        SELECT COUNT(DISTINCT id_contrato)
        FROM fact_certificaciones
        WHERE fecha >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
    """)).scalar() or 0

    ultimo_periodo = db.execute(text("""
        SELECT DATE_FORMAT(MAX(fecha), '%Y-%m')
        FROM fact_certificaciones
    """)).scalar() or "—"

    # Variacion vs mes anterior
    monto_mes_actual = db.execute(text("""
        SELECT SUM(total_mes) FROM fact_certificaciones
        WHERE DATE_FORMAT(fecha, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
    """)).scalar() or 0

    monto_mes_anterior = db.execute(text("""
        SELECT SUM(total_mes) FROM fact_certificaciones
        WHERE DATE_FORMAT(fecha, '%Y-%m') = DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m')
    """)).scalar() or 0

    variacion = 0
    if monto_mes_anterior > 0:
        variacion = round((monto_mes_actual - monto_mes_anterior) / monto_mes_anterior * 100, 1)

    return {
        "total_monto":       float(total_monto),
        "total_lineas":      int(total_lineas),
        "contratos_activos": int(contratos_activos),
        "ultimo_periodo":    ultimo_periodo,
        "monto_mes_actual":  float(monto_mes_actual),
        "variacion_pct":     variacion,
    }