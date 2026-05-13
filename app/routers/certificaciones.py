import json

from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db
from app.models import Usuario, CargaLog
from app.services.auth import get_current_user, check_contrato_access
from app.services.parser import parsear_bytes
from app.services.carga import cargar_certificaciones
from app.services.cache import guardar, recuperar, limpiar
from app.services.parser_pdf import parsear_pdf_bytes

router = APIRouter(prefix="/certificaciones", tags=["certificaciones"])

MAX_FILE_MB = 20


# ── Preview ───────────────────────────────────────────────────
@router.post("/preview")
async def preview(
    archivo: UploadFile = File(...),
    periodo_anio: int   = Form(...),
    periodo_mes: int    = Form(...),
    current: Usuario    = Depends(get_current_user),
    db: Session         = Depends(get_db),
):
    """
    Parsea el Excel, guarda el resultado en caché y devuelve
    las filas con validaciones + un cache_id para confirmar.
    NO escribe en la base de datos.
    """
    contenido = await archivo.read()
    if len(contenido) > MAX_FILE_MB * 1024 * 1024:
        raise HTTPException(400, f"El archivo supera los {MAX_FILE_MB} MB")

    if archivo.filename.lower().endswith(".pdf"):
         resultado = parsear_pdf_bytes(contenido, archivo.filename, periodo_anio, periodo_mes) 
    else:
         resultado = parsear_bytes(contenido, archivo.filename, periodo_anio, periodo_mes)

    if not resultado["filas"]:
        raise HTTPException(422, "No se encontraron filas válidas en el archivo")


    filas_validas = [
    f for f in resultado["filas"]
    if float(f.get("cantidades") or 0) != 0
]

# Validar ítems contra dim_item
    for fila in filas_validas:
        item_codigo = fila.get("item_codigo", "").replace(".", ",")

        existe = db.execute(text("""
               SELECT 1 FROM dim_item
               WHERE REPLACE(item_codigo, '.', ',') = :item
               LIMIT 1
    """), {"item": item_codigo}).fetchone()

    if not existe:
        fila["tiene_error"]    = True
        fila["error_detalle"]  = f"Ítem {fila['item_codigo']} no encontrado en el maestro"

    resumen = {
        "total":      len(filas_validas),
        "con_error":  sum(1 for f in filas_validas if f["tiene_error"]),
        "total_mes":  _sumar_total(filas_validas),
    }

    # Guardar resultado completo en caché
    id_cache = guardar({
        "resultado":     resultado,
        "archivo":       archivo.filename,
        "periodo_anio":  periodo_anio,
        "periodo_mes":   periodo_mes,
        "usuario_id":    current.id,
    })

    return {
        "cache_id":  id_cache,
        "archivo":   resultado["archivo"],
        "hojas":     resultado["hojas"],
        "periodo":   resultado["periodo"],
        "resumen":   resumen,
        "filas":     filas_validas,
        "errores":   resultado["errores"],
    }


# ── Confirmar ─────────────────────────────────────────────────
@router.post("/confirmar")
async def confirmar(
    cache_id: str       = Form(...),
    hojas: str          = Form(default="[]"),
    filas_editadas: str = Form(default="[]"),
    current: Usuario    = Depends(get_current_user),
    db: Session         = Depends(get_db),
):
    """
    Carga las filas a la BD usando el caché del preview.
    Usa las filas editadas por el usuario si las hay.
    """
    hojas_seleccionadas = json.loads(hojas)
    filas_del_frontend  = json.loads(filas_editadas)

    # Recuperar del caché
    cached = recuperar(cache_id)
    if not cached:
        raise HTTPException(
            400,
            "La sesión expiró (30 minutos). Volvé a subir el archivo."
        )

    # Verificar que el usuario que confirma es el mismo que hizo el preview
    if cached["usuario_id"] != current.id:
        raise HTTPException(403, "No autorizado")

    if filas_del_frontend:
        # Usar las filas editadas por el usuario — estas tienen las correcciones
        filas_ok = [
            f for f in filas_del_frontend
            if not f.get("tiene_error")
            and float(f.get("cantidades") or 0) != 0
            and f.get("provincia")
        ]
        contratos_cargados = {f["contrato"] for f in filas_ok if f.get("contrato")}
    else:
        # Usar las filas del caché filtradas por hojas seleccionadas
        resultado = cached["resultado"]
        filas_ok = [
            f for f in resultado["filas"]
            if not f["tiene_error"]
            and float(f.get("cantidades") or 0) != 0
            and (not hojas_seleccionadas or f["hoja_origen"] in hojas_seleccionadas)
        ]
        contratos_cargados = {f["contrato"] for f in filas_ok if f.get("contrato")}

    # Verificar acceso
    for k in contratos_cargados:
        if k:
            check_contrato_access(current, k)

    if not filas_ok:
        raise HTTPException(422, "No hay filas válidas para cargar")

    # Asegurar archivo_origen en todas las filas
    for f in filas_ok:
        if not f.get("archivo_origen"):
            f["archivo_origen"] = cached["archivo"]

    carga = cargar_certificaciones(db, filas_ok, current.id, current.nombre)

    # Registrar en log
    log = CargaLog(
        usuario_id     = current.id,
        usuario_nombre = current.nombre,
        archivo_nombre = cached["archivo"],
        contrato       = ", ".join(c for c in contratos_cargados if c),
        periodo        = f"{cached['periodo_anio']}-{cached['periodo_mes']:02d}",
        filas_cargadas = carga["insertadas"],
        filas_error    = carga["omitidas"],
        estado         = "ok" if not carga["errores"] else "parcial",
        detalle_errores= str(carga["errores"])[:2000] if carga["errores"] else None,
    )
    db.add(log)
    db.commit()

    # Limpiar caché — ya no se necesita
    limpiar(cache_id)

    return {
        "mensaje":    f"{carga['insertadas']} filas cargadas correctamente",
        "insertadas": carga["insertadas"],
        "omitidas":   carga["omitidas"],
        "errores":    carga["errores"][:10],
    }


# ── Historial ─────────────────────────────────────────────────
@router.get("/historial")
def historial(
    current: Usuario = Depends(get_current_user),
    db: Session      = Depends(get_db),
):
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


# ── Resumen ───────────────────────────────────────────────────
@router.get("/resumen")
def resumen(
    current: Usuario = Depends(get_current_user),
    db: Session      = Depends(get_db),
):
    if current.rol == "admin":
        filtro = ""
        params: dict = {}
    else:
        ks     = ", ".join(f"'{k}'" for k in current.contratos_list)
        filtro = f"AND dc.codigo_k IN ({ks})"
        params = {}

    rows = db.execute(text(f"""
        SELECT
            DATE_FORMAT(fc.fecha, '%Y-%m') AS periodo,
            dc.codigo_k                    AS contrato,
            fc.tipo,
            COUNT(*)                       AS lineas,
            SUM(fc.total_mes)              AS monto_total
        FROM fact_certificaciones fc
        JOIN dim_contrato dc ON fc.id_contrato = dc.id_contrato
        WHERE 1=1 {filtro}
        GROUP BY periodo, dc.codigo_k, fc.tipo
        ORDER BY periodo DESC, dc.codigo_k
        LIMIT 200
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


# ── Detalle ───────────────────────────────────────────────────
@router.get("/detalle")
def detalle(
    periodo: str,
    contrato: str,
    current: Usuario = Depends(get_current_user),
    db: Session      = Depends(get_db),
):
    check_contrato_access(current, contrato)
    rows = db.execute(text("""
        SELECT
            di.item_codigo, fc.tarea, fc.tipo, pv.provincia AS provincia,
            fc.unidad_medida, fc.cantidades, fc.precio_unitario,
            fc.total_mes, fc.observaciones
        FROM fact_certificaciones fc
        JOIN dim_contrato  dc ON fc.id_contrato  = dc.id_contrato
        JOIN dim_item      di ON fc.id_item       = di.id_item
        JOIN ma_provincias pv ON fc.id_provincia  = pv.id
        WHERE dc.codigo_k = :k
          AND DATE_FORMAT(fc.fecha, '%Y-%m') = :periodo
        ORDER BY di.item_codigo
    """), {"k": contrato, "periodo": periodo}).fetchall()
    return [dict(r._mapping) for r in rows]


# ── Provincias ────────────────────────────────────────────────
@router.get("/provincias")
def provincias(
    current: Usuario = Depends(get_current_user),
    db: Session      = Depends(get_db),
):
    rows = db.execute(text(
        "SELECT id, provincia FROM ma_provincias WHERE activo = 1 ORDER BY provincia"
    )).fetchall()
    return [dict(r._mapping) for r in rows]


# ── Helper ────────────────────────────────────────────────────
def _sumar_total(filas: list[dict]) -> float:
    total = 0.0
    for f in filas:
        try:
            total += float(f.get("total_mes") or 0)
        except (ValueError, TypeError):
            pass
    return round(total, 2)