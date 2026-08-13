"""
Servicio de carga de certificaciones a la base de datos.

Lógica de resolución de contrato:
- Si el usuario editó el contrato en el preview → usar ese (máxima prioridad)
- Si no → usar el contrato del maestro de ítems (dim_item)
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


def _id_contrato_desde_maestro(db: Session, item_codigo: str) -> int | None:
    """Devuelve el id_contrato asignado al ítem en dim_item."""
    row = db.execute(text("""
        SELECT di.id_contrato FROM dim_item di
        WHERE REPLACE(di.item_codigo, '.', ',') = :item
        LIMIT 1
    """), {"item": item_codigo.replace(".", ",")}).fetchone()
    return row[0] if row else None


def _resolver_id_contrato(db: Session, item_codigo: str, contrato_k: str, usuario_edito: bool) -> int | None:
    """
    Resuelve id_contrato según prioridad:
    1. Si el usuario editó el contrato en el preview → usar ese K directamente
    2. Si no → usar el contrato del maestro de ítems
    3. Fallback → contrato del archivo si el ítem no existe en el maestro
    """
    if usuario_edito:
        # El usuario eligió explícitamente este contrato — respetarlo siempre
        return _id_contrato_por_k(db, contrato_k)

    # Sin edición: usar el contrato del maestro
    id_desde_maestro = _id_contrato_desde_maestro(db, item_codigo)
    if id_desde_maestro:
        return id_desde_maestro

    # Fallback: contrato del archivo
    return _id_contrato_por_k(db, contrato_k)


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

        # ¿El usuario editó el contrato manualmente en el preview?
        usuario_edito = "contrato_editado" in fila and bool(fila["contrato_editado"])
        contrato_k    = fila["contrato_editado"] if usuario_edito else fila["contrato"]

        id_item      = _resolver_id_item(db, fila["item_codigo"], contrato_k)
        id_contrato  = _resolver_id_contrato(db, fila["item_codigo"], contrato_k, usuario_edito)
        id_provincia = _resolver_id_provincia(db, fila["provincia"])

        if not id_contrato:
            errores.append({"fila": i, "mensaje": f"Contrato {contrato_k} no encontrado"})
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