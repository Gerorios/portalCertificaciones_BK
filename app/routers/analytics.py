from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, List
from datetime import date

from app.database import get_db
from app.models import Usuario
from app.services.auth import get_current_user

router = APIRouter(prefix="/analytics", tags=["analytics"])


def require_analytics(current: Usuario = Depends(get_current_user)) -> Usuario:
    if current.rol not in ("admin", "gerente", "jefe"):
        raise HTTPException(403, "Sin acceso a analytics")
    return current


def require_gerente_or_admin(current: Usuario = Depends(get_current_user)) -> Usuario:
    if current.rol not in ("admin", "gerente"):
        raise HTTPException(403, "Se requiere rol gerente o admin")
    return current


def _filtros(
    desde:      Optional[str],
    hasta:      Optional[str],
    contratos:  List[str],
    provincias: List[str],
    tipo:       Optional[str],
    current:    Usuario,
):
    f, p = "", {}

    if desde:
        f += " AND DATE_FORMAT(fc.fecha, '%Y-%m') >= :desde"
        p["desde"] = desde
    if hasta:
        f += " AND DATE_FORMAT(fc.fecha, '%Y-%m') <= :hasta"
        p["hasta"] = hasta

    if current.rol == "jefe":
        ks_propios = current.contratos_list or []
        ks = [k for k in contratos if k in ks_propios] if contratos else ks_propios
        if ks:
            ks_str = ", ".join(f"'{k}'" for k in ks)
            f += f" AND dc.codigo_k IN ({ks_str})"
    elif contratos:
        ks_str = ", ".join(f"'{k}'" for k in contratos)
        f += f" AND dc.codigo_k IN ({ks_str})"

    if provincias:
        pv_str = ", ".join(f"'{pv}'" for pv in provincias)
        f += f" AND pv.provincia IN ({pv_str})"

    if tipo and tipo in ("OPEX", "CAPEX"):
        f += " AND fc.tipo = :tipo"
        p["tipo"] = tipo

    return f, p


# ── Base FROM sin dim_item (para endpoints que no necesitan datos del ítem) ──
# PGN se calcula con fc.ptos_gasnor (viene de la certificación, igual que PBI)
# COALESCE por si alguna fila tiene ptos_gasnor NULL (archivos sin esa columna)

@router.get("/evolucion-mensual")
def evolucion_mensual(
    desde:      Optional[str]  = None,
    hasta:      Optional[str]  = None,
    contratos:  List[str]      = Query(default=[]),
    provincias: List[str]      = Query(default=[]),
    tipo:       Optional[str]  = None,
    current:    Usuario        = Depends(require_analytics),
    db:         Session        = Depends(get_db),
):
    f, p = _filtros(desde, hasta, contratos, provincias, tipo, current)
    rows = db.execute(text(f"""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m')                          AS periodo,
            SUM(fc.total_mes)                                       AS monto_total,
            SUM(fc.cantidades * COALESCE(fc.ptos_gasnor, 0))       AS pgn_total
        FROM fact_certificaciones fc
        JOIN dim_contrato  dc ON fc.id_contrato  = dc.id_contrato
        JOIN ma_provincias pv ON fc.id_provincia = pv.id
        WHERE 1=1 {f}
        GROUP BY periodo ORDER BY periodo ASC
    """), p).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/por-contrato-mes")
def por_contrato_mes(
    desde:      Optional[str]  = None,
    hasta:      Optional[str]  = None,
    contratos:  List[str]      = Query(default=[]),
    provincias: List[str]      = Query(default=[]),
    tipo:       Optional[str]  = None,
    current:    Usuario        = Depends(require_analytics),
    db:         Session        = Depends(get_db),
):
    f, p = _filtros(desde, hasta, contratos, provincias, tipo, current)
    rows = db.execute(text(f"""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m')                          AS periodo,
            dc.codigo_k                                             AS contrato,
            SUM(fc.total_mes)                                       AS monto_total,
            SUM(fc.cantidades * COALESCE(fc.ptos_gasnor, 0))       AS pgn_total
        FROM fact_certificaciones fc
        JOIN dim_contrato  dc ON fc.id_contrato  = dc.id_contrato
        JOIN ma_provincias pv ON fc.id_provincia = pv.id
        WHERE 1=1 {f}
        GROUP BY periodo, dc.codigo_k
        ORDER BY periodo ASC, dc.codigo_k
    """), p).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/top-items")
def top_items(
    desde:      Optional[str]  = None,
    hasta:      Optional[str]  = None,
    contratos:  List[str]      = Query(default=[]),
    provincias: List[str]      = Query(default=[]),
    tipo:       Optional[str]  = None,
    limite:     int            = 10,
    current:    Usuario        = Depends(require_analytics),
    db:         Session        = Depends(get_db),
):
    # Este endpoint sí necesita dim_item para traer item_codigo y tarea
    f, p = _filtros(desde, hasta, contratos, provincias, tipo, current)
    p["limite"] = limite
    rows = db.execute(text(f"""
        SELECT
            di.item_codigo,
            LEFT(fc.tarea, 60) AS tarea,
            dc.codigo_k                                             AS contrato,
            SUM(fc.total_mes)                                       AS monto_total,
            SUM(fc.cantidades * COALESCE(fc.ptos_gasnor, 0))       AS pgn_total
        FROM fact_certificaciones fc
        JOIN dim_item      di ON fc.id_item      = di.id_item
        JOIN dim_contrato  dc ON fc.id_contrato  = dc.id_contrato
        JOIN ma_provincias pv ON fc.id_provincia = pv.id
        WHERE 1=1 {f}
        GROUP BY di.item_codigo, fc.tarea, dc.codigo_k
        ORDER BY monto_total DESC
        LIMIT :limite
    """), p).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/por-provincia")
def por_provincia(
    desde:      Optional[str]  = None,
    hasta:      Optional[str]  = None,
    contratos:  List[str]      = Query(default=[]),
    provincias: List[str]      = Query(default=[]),
    tipo:       Optional[str]  = None,
    current:    Usuario        = Depends(require_analytics),
    db:         Session        = Depends(get_db),
):
    f, p = _filtros(desde, hasta, contratos, provincias, tipo, current)
    rows = db.execute(text(f"""
        SELECT
            pv.provincia,
            SUM(fc.total_mes)                                       AS monto_total,
            SUM(fc.cantidades * COALESCE(fc.ptos_gasnor, 0))       AS pgn_total,
            COUNT(*)                                                AS lineas
        FROM fact_certificaciones fc
        JOIN dim_contrato  dc ON fc.id_contrato  = dc.id_contrato
        JOIN ma_provincias pv ON fc.id_provincia = pv.id
        WHERE 1=1 {f}
        GROUP BY pv.provincia
        ORDER BY monto_total DESC
    """), p).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/interanual")
def interanual(
    contratos:  List[str]      = Query(default=[]),
    provincias: List[str]      = Query(default=[]),
    tipo:       Optional[str]  = None,
    current:    Usuario        = Depends(require_analytics),
    db:         Session        = Depends(get_db),
):
    f, p = _filtros(None, None, contratos, provincias, tipo, current)

    rows = db.execute(text(f"""
        SELECT
            YEAR(fc.fecha)                                          AS anio,
            MONTH(fc.fecha)                                         AS mes,
            DATE_FORMAT(fc.fecha, '%Y-%m')                         AS periodo,
            SUM(fc.total_mes)                                       AS monto_total,
            SUM(fc.cantidades * COALESCE(fc.ptos_gasnor, 0))       AS pgn_total
        FROM fact_certificaciones fc
        JOIN dim_contrato  dc ON fc.id_contrato  = dc.id_contrato
        JOIN ma_provincias pv ON fc.id_provincia = pv.id
        WHERE YEAR(fc.fecha) IN (YEAR(CURDATE()), YEAR(CURDATE()) - 1)
          {f}
        GROUP BY anio, mes, periodo
        ORDER BY mes ASC, anio ASC
    """), p).fetchall()

    anios         = sorted(set(dict(r._mapping)["anio"] for r in rows), reverse=True)
    anio_actual   = anios[0] if len(anios) > 0 else None
    anio_anterior = anios[1] if len(anios) > 1 else None

    datos = {}
    for r in rows:
        d   = dict(r._mapping)
        mes = d["mes"]
        if mes not in datos:
            datos[mes] = {
                "mes": mes,
                "monto_actual": None, "monto_anterior": None,
                "pgn_actual":   None, "pgn_anterior":   None,
                "var_monto":    None, "var_pgn":         None,
            }
        if d["anio"] == anio_actual:
            datos[mes]["monto_actual"] = float(d["monto_total"] or 0)
            datos[mes]["pgn_actual"]   = float(d["pgn_total"]   or 0)
        elif d["anio"] == anio_anterior:
            datos[mes]["monto_anterior"] = float(d["monto_total"] or 0)
            datos[mes]["pgn_anterior"]   = float(d["pgn_total"]   or 0)

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
    current: Usuario = Depends(require_analytics),
    db: Session = Depends(get_db),
):
    if current.rol == "jefe":
        return current.contratos_list
    rows = db.execute(text(
        "SELECT codigo_k FROM dim_contrato ORDER BY codigo_k"
    )).fetchall()
    return [r[0] for r in rows]


@router.get("/provincias")
def provincias_analytics(
    current: Usuario = Depends(require_analytics),
    db: Session = Depends(get_db),
):
    rows = db.execute(text(
        "SELECT provincia FROM ma_provincias WHERE activo = 1 ORDER BY provincia"
    )).fetchall()
    return [r[0] for r in rows]


@router.get("/estado-cargas")
def estado_cargas(
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    contratos_rows = db.execute(text(
        "SELECT codigo_k FROM dim_contrato ORDER BY codigo_k"
    )).fetchall()
    todos_contratos = [r[0] for r in contratos_rows]

    cargas = db.execute(text("""
        SELECT cl.contrato, cl.periodo, cl.usuario_nombre,
               cl.cargado_en, cl.filas_cargadas, cl.estado
        FROM carga_log cl
        WHERE cl.periodo >= '2025-01' AND cl.estado != 'error'
        ORDER BY cl.periodo DESC, cl.contrato
    """)).fetchall()

    cargados = {}
    for r in cargas:
        d = dict(r._mapping)
        for k in [x.strip() for x in (d["contrato"] or "").split(",")]:
            if not k:
                continue
            key = f"{k}__{d['periodo']}"
            if key not in cargados:
                cargados[key] = {
                    "contrato":       k,
                    "periodo":        d["periodo"],
                    "usuario":        d["usuario_nombre"],
                    "cargado_en":     str(d["cargado_en"]).split("T")[0] if d["cargado_en"] else None,
                    "filas_cargadas": d["filas_cargadas"],
                    "estado":         d["estado"],
                }

    hoy = date.today()
    periodos = []
    a, m = 2025, 1
    while (a, m) <= (hoy.year, hoy.month):
        periodos.append(f"{a}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            a += 1

    resultado = []
    for periodo in reversed(periodos):
        for contrato in todos_contratos:
            key   = f"{contrato}__{periodo}"
            carga = cargados.get(key)
            [anio_p, mes_p] = periodo.split("-")
            es_actual = (int(anio_p) == hoy.year and int(mes_p) == hoy.month)
            if es_actual and hoy.day < 10:
                continue
            resultado.append({
                "contrato":       contrato,
                "periodo":        periodo,
                "cargado":        carga is not None,
                "usuario":        carga["usuario"]        if carga else None,
                "cargado_en":     carga["cargado_en"]     if carga else None,
                "filas_cargadas": carga["filas_cargadas"] if carga else None,
                "estado":         carga["estado"]         if carga else None,
            })
    return resultado


@router.get("/presupuesto")
def presupuesto_contratos(
    _: Usuario = Depends(require_gerente_or_admin),
    db: Session = Depends(get_db),
):
    """
    Consumo en $ del presupuesto Naturgy vigente por contrato.
    Solo incluye contratos con presupuesto activo cargado.
    """
    rows = db.execute(text("""
        SELECT
            dc.codigo_k                                            AS contrato,
            dc.descripcion                                         AS descripcion,
            dp.periodo_desde                                       AS periodo_desde,
            dp.periodo_hasta                                       AS periodo_hasta,
            dp.monto_presupuesto                                   AS monto_presupuesto,
            COALESCE(SUM(fc.total_mes), 0)                         AS consumido
        FROM dim_presupuesto_contrato dp
        JOIN dim_contrato dc ON dp.id_contrato = dc.id_contrato
        LEFT JOIN fact_certificaciones fc
               ON fc.id_contrato = dp.id_contrato
              AND fc.fecha BETWEEN dp.periodo_desde AND dp.periodo_hasta
        WHERE dp.activo = 1
        GROUP BY dc.codigo_k, dc.descripcion, dp.periodo_desde, dp.periodo_hasta, dp.monto_presupuesto
        ORDER BY (COALESCE(SUM(fc.total_mes), 0) / dp.monto_presupuesto) DESC
    """)).fetchall()

    resultado = []
    for r in rows:
        d = dict(r._mapping)
        monto     = float(d["monto_presupuesto"])
        consumido = float(d["consumido"])
        pct       = round((consumido / monto) * 100, 1) if monto else 0
        resultado.append({
            "contrato":          d["contrato"],
            "descripcion":       d["descripcion"],
            "periodo_desde":     str(d["periodo_desde"]),
            "periodo_hasta":     str(d["periodo_hasta"]),
            "monto_presupuesto": monto,
            "consumido":         consumido,
            "pct":               pct,
        })
    return resultado


@router.get("/kpis-jefe")
def kpis_jefe(
    current: Usuario = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if current.rol not in ("admin", "jefe"):
        raise HTTPException(403, "Solo para jefes de contrato")

    contratos = current.contratos_list
    if not contratos:
        return {}

    ks       = ", ".join(f"'{k}'" for k in contratos)
    filtro_k = f"AND dc.codigo_k IN ({ks})"

    datos = db.execute(text(f"""
        SELECT
            YEAR(fc.fecha)                                          AS anio,
            MONTH(fc.fecha)                                         AS mes,
            SUM(fc.total_mes)                                       AS monto_total,
            SUM(fc.cantidades * COALESCE(fc.ptos_gasnor, 0))       AS pgn_total,
            COUNT(*)                                                AS lineas
        FROM fact_certificaciones fc
        JOIN dim_contrato  dc ON fc.id_contrato  = dc.id_contrato
        JOIN ma_provincias pv ON fc.id_provincia = pv.id
        WHERE (
            DATE_FORMAT(fc.fecha, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
            OR DATE_FORMAT(fc.fecha, '%Y-%m') = DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 YEAR), '%Y-%m')
        )
        {filtro_k}
        GROUP BY anio, mes
        ORDER BY anio DESC
    """)).fetchall()

    hoy      = date.today()
    actual   = None
    anterior = None
    for r in datos:
        d = dict(r._mapping)
        if d["anio"] == hoy.year:
            actual = d
        else:
            anterior = d

    def variacion(a, b):
        if a is not None and b and float(b) > 0:
            return round((float(a) - float(b)) / float(b) * 100, 1)
        return None

    return {
        "monto_actual":   float(actual["monto_total"])   if actual   else None,
        "monto_anterior": float(anterior["monto_total"]) if anterior else None,
        "pgn_actual":     float(actual["pgn_total"])     if actual   else None,
        "pgn_anterior":   float(anterior["pgn_total"])   if anterior else None,
        "lineas_actual":  int(actual["lineas"])          if actual   else 0,
        "var_monto":      variacion(
            actual["monto_total"]   if actual   else None,
            anterior["monto_total"] if anterior else None,
        ),
        "var_pgn":        variacion(
            actual["pgn_total"]     if actual   else None,
            anterior["pgn_total"]   if anterior else None,
        ),
        "contratos": contratos,
    }