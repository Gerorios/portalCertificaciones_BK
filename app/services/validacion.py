"""
Reglas de cargabilidad de filas de certificación (ver CONTEXT.md).

La cargabilidad nunca es un veredicto congelado del parser: se recalcula
acá cada vez que hace falta (preview, edición, confirmación), ignorando
el flag `tiene_error` que traiga la fila.

Una fila es "de plantilla" (ruido, se oculta del preview) cuando no tiene
cantidad ni total con plata. El unitario solo no cuenta como contenido:
los archivos de Naturgy traen el catálogo completo de ítems con precio
unitario y cantidad 0, y eso no es nada certificado.
"""
from typing import Optional


def _num(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def es_fila_plantilla(fila: dict) -> bool:
    """
    Sin cantidad y sin total con plata: ruido de plantilla, no se muestra.
    El unitario solo NO cuenta — los archivos de Naturgy traen el catálogo
    completo con unitario y cantidad 0, y eso no es contenido certificado.
    """
    cant  = _num(fila.get("cantidades"))
    total = _num(fila.get("total_mes"))
    return (cant is None or cant == 0) and (total is None or total == 0)


def revalidar_fila(
    fila: dict,
    item_existe: bool = True,
    provincias_validas: Optional[list[str]] = None,
) -> tuple[bool, Optional[str]]:
    """
    Devuelve (tiene_error, detalle) según la regla de fila cargable:
    ítem en maestro + contrato + provincia válida + cantidad != 0 + total.
    El unitario puede faltar si el total está.
    """
    faltas = []

    if not item_existe:
        faltas.append(f"Ítem {fila.get('item_codigo', '?')} no encontrado en el maestro")

    if not (fila.get("contrato") or "").strip():
        faltas.append("Falta contrato K")

    provincia = (fila.get("provincia") or "").strip()
    if not provincia:
        faltas.append("Falta provincia")
    elif provincias_validas is not None:
        validas_upper = {p.strip().upper() for p in provincias_validas}
        if provincia.upper() not in validas_upper:
            faltas.append(f"Provincia '{provincia}' inválida")

    cant = _num(fila.get("cantidades"))
    if cant is None or cant == 0:
        faltas.append("Falta cantidad")

    if _num(fila.get("total_mes")) is None:
        faltas.append("Falta total mes")

    if faltas:
        return True, "; ".join(faltas)
    return False, None


def filtrar_visibles_preview(filas: list[dict]) -> list[dict]:
    """Todo se muestra salvo las filas de plantilla."""
    return [f for f in filas if not es_fila_plantilla(f)]


def filtrar_cargables(
    filas: list[dict],
    provincias_validas: Optional[list[str]] = None,
) -> list[dict]:
    """
    Filas que van a la base: no excluidas por el usuario y cargables
    según revalidación — el flag `tiene_error` del frontend se ignora.
    """
    resultado = []
    for f in filas:
        if f.get("excluida"):
            continue
        err, _ = revalidar_fila(f, provincias_validas=provincias_validas)
        if not err:
            resultado.append(f)
    return resultado
