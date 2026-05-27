"""
Servicio de carga de certificaciones a la base de datos.
Resuelve FKs automáticamente: item → id_item, contrato → id_contrato, etc.

El contrato se resuelve desde el maestro de ítems (dim_item), no desde
la certificación — así se corrigen errores de Naturgy donde asignan K3
a un ítem que pertenece a K2.
"""
from sqlalchemy.orm import Session
from sqlalchemy import text


def _resolver_id_item(db: Session, item_codigo: str, contrato_k: str) -> int | None:
    # Busca primero en el contrato correspondiente, si no lo encuentra busca en cualquier contrato
    row = db.execute(text("""
        SELECT di.id_item FROM dim_item di
        JOIN dim_contrato dc ON di.id_contrato = dc.id_contrato
        WHERE REPLACE(di.item_codigo, '.', ',') = :item
          AND dc.codigo_k = :k
        LIMIT 1
    """), {"item": item_codigo.replace(".", ","), "k": contrato_k}).fetchone()

    if row:
        return row[0]

    # Fallback: buscar solo por código sin importar el contrato
    row = db.execute(text("""
        SELECT id_item FROM dim_item
        WHERE REPLACE(item_codigo, '.', ',') = :item
        LIMIT 1
    """), {"item": item_codigo.replace(".", ",")}).fetchone()

    return row[0] if row else None


def _resolver_id_contrato_desde_maestro(db: Session, item_codigo: str, contrato_k: str, contrato_editado: str = None) -> int | None:
    """
    Resuelve el id_contrato en este orden de prioridad:
    1. Contrato editado por el usuario en el preview (máxima prioridad)
    2. Contrato asignado al ítem en el maestro (dim_item)
    3. Contrato de la certificación como fallback
    """
    # 1. Si el usuario editó el contrato en el preview, usarlo directamente
    if contrato_editado and contrato_editado.strip():
        row = db.execute(text(
            "SELECT id_contrato FROM dim_contrato WHERE codigo_k = :k"
        ), {"k": contrato_editado.strip()}).fetchone()
        if row:
            return row[0]

    # 2. Contrato que tiene el ítem en dim_item
    row = db.execute(text("""
        SELECT di.id_contrato
        FROM dim_item di
        WHERE REPLACE(di.item_codigo, '.', ',') = :item
        LIMIT 1
    """), {"item": item_codigo.replace(".", ",")}).fetchone()

    if row:
        return row[0]

    # 3. Fallback: contrato de la certificación
    row2 = db.execute(text(
        "SELECT id_contrato FROM dim_contrato WHERE codigo_k = :k"
    ), {"k": contrato_k}).fetchone()

    return row2[0] if row2 else None


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
    """
    Inserta las filas en fact_certificaciones resolviendo FKs.
    El contrato se toma del maestro de ítems, no de la certificación.

    Retorna: {"insertadas": N, "omitidas": N, "errores": [...]}
    """
    insertadas = 0
    omitidas   = 0
    errores    = []

    for i, fila in enumerate(filas):
        if fila.get("tiene_error"):
            omitidas += 1
            continue

        id_item      = _resolver_id_item(db, fila["item_codigo"], fila["contrato"])
        # contrato_editado: viene del preview si el usuario lo cambió manualmente
        contrato_editado = fila.get("contrato_editado") or fila.get("contrato")
        id_contrato  = _resolver_id_contrato_desde_maestro(db, fila["item_codigo"], fila["contrato"], contrato_editado)
        id_provincia = _resolver_id_provincia(db, fila["provincia"])

        if not id_contrato:
            errores.append({"fila": i, "mensaje": f"Contrato {fila['contrato']} no encontrado"})
            omitidas += 1
            continue

        if not id_item:
            errores.append({"fila": i, "mensaje": f"Ítem {fila['item_codigo']} para {fila['contrato']} no encontrado"})
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
            "ptos_gasnor":     fila.get("ptos_gasnor"),
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