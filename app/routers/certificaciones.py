from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db
from app.models import Usuario, CargaLog
from app.services.auth import get_current_user, check_contrato_access
from app.services.parser import parsear_bytes
from app.services.carga import cargar_certificaciones

router = APIRouter(prefix="/certificaciones", tags=["certificaciones"])

MAX_FILE_MB = 20


@router.post("/preview")
async def preview(
    archivo: UploadFile = File(...),
    periodo_anio: int   = Form(...),
    periodo_mes: int    = Form(...),
    current: Usuario    = Depends(get_current_user),
):
    """
    Parsea el Excel y devuelve las filas con validaciones.
    NO escribe en la base de datos.
    """
    contenido = await archivo.read()
    if len(contenido) > MAX_FILE_MB * 1024 * 1024:
        raise HTTPException(400, f"El archivo supera los {MAX_FILE_MB} MB")

    resultado = parsear_bytes(contenido, archivo.filename, periodo_anio, periodo_mes)

    if not resultado["filas"]:
        raise HTTPException(422, "No se encontraron filas válidas en el archivo")

    # Verificar que el jefe solo suba sus contratos
    contratos_en_archivo = {f["contrato"] for f in resultado["filas"] if f.get("contrato")}
    for k in contratos_en_archivo:
        check_contrato_access(current, k)

    resumen = {
        "total":     len(resultado["filas"]),
        "con_error": sum(1 for f in resultado["filas"] if f["tiene_error"]),
        "advertencias": len([e for e in resultado["errores"] if e["campo"] != "provincia"]),
        "total_mes": _sumar_total(resultado["filas"]),
    }

    return {
        "archivo":   resultado["archivo"],
        "hojas":     resultado["hojas"],
        "periodo":   resultado["periodo"],
        "resumen":   resumen,
        "filas":     resultado["filas"],
        "errores":   resultado["errores"],
    }


@router.post("/confirmar")
async def confirmar(
    archivo: UploadFile = File(...),
    periodo_anio: int   = Form(...),
    periodo_mes: int    = Form(...),
    current: Usuario    = Depends(get_current_user),
    db: Session         = Depends(get_db),
):
    """
    Parsea y carga definitivamente las filas sin error a la BD.
    """
    contenido = await archivo.read()
    if len(contenido) > MAX_FILE_MB * 1024 * 1024:
        raise HTTPException(400, f"El archivo supera los {MAX_FILE_MB} MB")

    resultado = parsear_bytes(contenido, archivo.filename, periodo_anio, periodo_mes)

    contratos_en_archivo = {f["contrato"] for f in resultado["filas"] if f.get("contrato")}
    for k in contratos_en_archivo:
        check_contrato_access(current, k)

    filas_ok = [f for f in resultado["filas"] if not f["tiene_error"]]
    if not filas_ok:
        raise HTTPException(422, "No hay filas válidas para cargar")

    carga = cargar_certificaciones(db, filas_ok, current.id, current.nombre)

    log = CargaLog(
        usuario_id     = current.id,
        usuario_nombre = current.nombre,
        archivo_nombre = archivo.filename,
        contrato       = ", ".join(contratos_en_archivo),
        periodo        = f"{periodo_anio}-{periodo_mes:02d}",
        filas_cargadas = carga["insertadas"],
        filas_error    = carga["omitidas"],
        estado         = "ok" if not carga["errores"] else "parcial",
        detalle_errores= str(carga["errores"])[:2000] if carga["errores"] else None,
    )
    db.add(log)
    db.commit()

    return {
        "mensaje":    f"{carga['insertadas']} filas cargadas correctamente",
        "insertadas": carga["insertadas"],
        "omitidas":   carga["omitidas"],
        "errores":    carga["errores"][:10],
    }


@router.get("/historial")
def historial(
    current: Usuario = Depends(get_current_user),
    db: Session      = Depends(get_db),
):
    """Historial de cargas del usuario (o todas si es admin)."""
    if current.rol == "admin":
        rows = db.execute(text("""
            SELECT id, usuario_nombre, archivo_nombre, contrato,
                   periodo, filas_cargadas, estado, cargado_en
            FROM carga_log ORDER BY cargado_en DESC LIMIT 100
        """)).fetchall()
    else:
        rows = db.execute(text("""
            SELECT id, usuario_nombre, archivo_nombre, contrato,
                   periodo, filas_cargadas, estado, cargado_en
            FROM carga_log WHERE usuario_id = :uid
            ORDER BY cargado_en DESC LIMIT 50
        """), {"uid": current.id}).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/resumen")
def resumen(
    current: Usuario = Depends(get_current_user),
    db: Session      = Depends(get_db),
):
    """Total facturado por contrato y mes para el usuario actual."""
    if current.rol == "admin":
        filtro = ""
        params: dict = {}
    else:
        ks = ", ".join(f"'{k}'" for k in current.contratos_list)
        filtro = f"AND dc.codigo_k IN ({ks})"
        params = {}

    rows = db.execute(text(f"""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m')   AS periodo,
            dc.codigo_k                       AS contrato,
            fc.tipo,
            COUNT(*)                          AS lineas,
            SUM(fc.total_mes)                 AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE 1=1 {filtro}
        GROUP BY periodo, dc.codigo_k, fc.tipo
        ORDER BY periodo DESC, dc.codigo_k
        LIMIT 200
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


def _sumar_total(filas: list[dict]) -> float:
    total = 0.0
    for f in filas:
        try:
            total += float(f.get("total_mes") or 0)
        except (ValueError, TypeError):
            pass
    return round(total, 2)
