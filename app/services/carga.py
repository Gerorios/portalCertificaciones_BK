"""
Servicio de carga de certificaciones a la base de datos.

Lógica de resolución de contrato (regla única, ver `resolver_contrato_final`):
- Si el usuario editó el contrato en el preview → usar ese (máxima prioridad)
- Si no → usar el contrato del maestro de ítems (dim_item); si el ítem está
  en varios contratos, se prefiere el del archivo si coincide, si no el
  primero en orden determinista
- Fallback → contrato del archivo solo si el ítem no existe en el maestro
"""
from sqlalchemy.orm import Session
from sqlalchemy import text


def _resolver_id_item(db: Session, item_codigo: str, contrato_k: str) -> int | None:
    """Busca el id_item primero por código+contrato, luego solo por código."""
    row = db.execute(text("""
        SELECT di.id_item FROM dim_item di
        JOIN dim_contrato dc ON di.id_contrato = dc.id_contrato
        WHERE REPLACE(di.item_codigo, '.', ',') = :item
          AND dc.codigo_k = :k
        LIMIT 1
    """), {"item": item_codigo.replace(".", ","), "k": contrato_k}).fetchone()

    if row:
        return row[0]

    # Fallback: cualquier contrato
    row = db.execute(text("""
        SELECT id_item FROM dim_item
        WHERE REPLACE(item_codigo, '.', ',') = :item
        LIMIT 1
    """), {"item": item_codigo.replace(".", ",")}).fetchone()

    return row[0] if row else None


def _id_contrato_por_k(db: Session, codigo_k: str) -> int | None:
    """Devuelve el id_contrato para un código K."""
    row = db.execute(text(
        "SELECT id_contrato FROM dim_contrato WHERE codigo_k = :k"
    ), {"k": codigo_k}).fetchone()
    return row[0] if row else None


def _contratos_maestro(db: Session, item_codigo: str) -> list[str]:
    """Códigos K de todos los contratos donde el maestro tiene este ítem,
    en orden determinista."""
    if not item_codigo:
        return []
    rows = db.execute(text("""
        SELECT dc.codigo_k
        FROM dim_item di
        JOIN dim_contrato dc ON di.id_contrato = dc.id_contrato
        WHERE REPLACE(di.item_codigo, '.', ',') = :item
        ORDER BY di.id_item
    """), {"item": item_codigo.replace(".", ",")}).fetchall()
    return [r[0] for r in rows]


def resolver_contrato_final(
    db: Session,
    item_codigo: str,
    contrato_archivo: str | None,
    contrato_editado: str | None = None,
) -> tuple[str | None, str]:
    """
    Regla única de resolución de contrato (preview y carga usan ESTA función):
    1. editado por el usuario en el preview → gana siempre
    2. maestro (dim_item); si el ítem está en varios contratos, se prefiere
       el del archivo si coincide, si no el primero en orden determinista
    3. archivo, solo si el ítem no está en el maestro
    Devuelve (codigo_k | None, fuente) con fuente en {"editado","maestro","archivo"}.
    """
    if contrato_editado:
        return contrato_editado, "editado"
    ks = _contratos_maestro(db, item_codigo)
    if ks:
        if contrato_archivo in ks:
            return contrato_archivo, "maestro"
        return ks[0], "maestro"
    return (contrato_archivo or None), "archivo"


def anotar_contrato_final(db: Session, fila: dict) -> dict:
    """Anota en la fila el contrato que efectivamente se va a cargar.
    Idempotente: `contrato_archivo` preserva siempre el K original del archivo."""
    if "contrato_archivo" not in fila:
        fila["contrato_archivo"] = fila.get("contrato") or ""
    k_final, fuente = resolver_contrato_final(
        db,
        fila.get("item_codigo") or "",
        fila["contrato_archivo"],
        fila.get("contrato_editado"),
    )
    fila["contrato"] = k_final or ""
    fila["contrato_fuente"] = fuente
    fila["contrato_del_maestro"] = k_final if fuente == "maestro" else None
    return fila


def _ptos_gasnor_con_fallback(db: Session, valor_archivo, id_item: int):
    """
    ptos_gasnor a guardar en la certificación:
    - si el archivo lo trae → ese (coincide con Power BI)
    - si no (ej. K12, que no trae la columna) → el del maestro (dim_item),
      para que el PGN no quede en 0 en analytics
    - si el maestro tampoco lo tiene → None (como antes)
    """
    if valor_archivo not in (None, ""):
        return valor_archivo
    row = db.execute(text(
        "SELECT ptos_gasnor FROM dim_item WHERE id_item = :id"
    ), {"id": id_item}).fetchone()
    return row[0] if row else None


def _resolver_id_provincia(db: Session, nombre: str) -> int | None:
    row = db.execute(text(
        "SELECT id FROM ma_provincias WHERE UPPER(provincia) = UPPER(:n)"
    ), {"n": nombre}).fetchone()
    return row[0] if row else None


def cargar_certificaciones(
    db: Session,
    filas: list[dict],
    usuario_id: int,
    usuario_nombre: str,
) -> dict:
    insertadas = 0
    omitidas   = 0
    errores    = []

    for i, fila in enumerate(filas):
        if fila.get("tiene_error"):
            omitidas += 1
            continue

        # Regla única de resolución (la misma que vio el usuario en el preview)
        k_final, _fuente = resolver_contrato_final(
            db,
            fila["item_codigo"],
            fila.get("contrato_archivo") or fila.get("contrato"),
            fila.get("contrato_editado"),
        )

        id_item      = _resolver_id_item(db, fila["item_codigo"], k_final or "")
        id_contrato  = _id_contrato_por_k(db, k_final) if k_final else None
        id_provincia = _resolver_id_provincia(db, fila["provincia"])

        if not id_contrato:
            errores.append({"fila": i, "mensaje": f"Contrato {k_final} no encontrado"})
            omitidas += 1
            continue

        if not id_item:
            errores.append({"fila": i, "mensaje": f"Ítem {fila['item_codigo']} no encontrado"})
            omitidas += 1
            continue

        if not id_provincia:
            errores.append({"fila": i, "mensaje": f"Provincia '{fila['provincia']}' no encontrada"})
            omitidas += 1
            continue

        db.execute(text("""
            INSERT INTO fact_certificaciones (
                id_item, nombre_contrato, tarea, id_contrato,
                unidad_medida, ptos_gasnor, tipo, contratista,
                id_provincia, region, cantidades, precio_unitario,
                total_mes, observaciones, fecha,
                hoja_origen, archivo_origen, cargado_por
            ) VALUES (
                :id_item, :nombre_contrato, :tarea, :id_contrato,
                :unidad_medida, :ptos_gasnor, :tipo, :contratista,
                :id_provincia, :region, :cantidades, :precio_unitario,
                :total_mes, :observaciones, :fecha,
                :hoja_origen, :archivo_origen, :cargado_por
            )
        """), {
            "id_item":         id_item,
            "nombre_contrato": fila.get("nombre_contrato"),
            "tarea":           fila.get("tarea"),
            "id_contrato":     id_contrato,
            "unidad_medida":   fila.get("unidad_medida"),
            "ptos_gasnor":     _ptos_gasnor_con_fallback(db, fila.get("ptos_gasnor"), id_item),
            "tipo":            fila.get("tipo"),
            "contratista":     fila.get("contratista"),
            "id_provincia":    id_provincia,
            "region":          fila.get("region"),
            "cantidades":      fila.get("cantidades"),
            "precio_unitario": fila.get("precio_unitario"),
            "total_mes":       fila.get("total_mes"),
            "observaciones":   fila.get("observaciones"),
            "fecha":           fila.get("fecha"),
            "hoja_origen":     fila.get("hoja_origen"),
            "archivo_origen":  fila.get("archivo_origen"),
            "cargado_por":     usuario_nombre,
        })
        insertadas += 1

    db.commit()
    return {"insertadas": insertadas, "omitidas": omitidas, "errores": errores}