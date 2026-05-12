"""
cache.py
========
Caché en memoria para guardar resultados de parseo de Excel.
Evita reparsear el archivo en el paso de confirmación.

Cada entrada expira a los 30 minutos — suficiente para que
el usuario revise y confirme sin que se pierda la sesión.
"""
import uuid
from datetime import datetime, timedelta
from typing import Optional

_cache: dict[str, dict] = {}

EXPIRACION_MINUTOS = 30


def guardar(datos: dict) -> str:
    """Guarda el resultado del parseo y devuelve un ID único."""
    # Limpiar entradas expiradas antes de guardar
    _limpiar_expirados()

    id_cache = str(uuid.uuid4())
    _cache[id_cache] = {
        "datos":     datos,
        "expira_en": datetime.now() + timedelta(minutes=EXPIRACION_MINUTOS),
    }
    return id_cache


def recuperar(id_cache: str) -> Optional[dict]:
    """Recupera los datos del caché. Devuelve None si no existe o expiró."""
    entry = _cache.get(id_cache)
    if not entry:
        return None
    if datetime.now() > entry["expira_en"]:
        del _cache[id_cache]
        return None
    return entry["datos"]


def limpiar(id_cache: str):
    """Elimina una entrada del caché (después de confirmar)."""
    _cache.pop(id_cache, None)


def _limpiar_expirados():
    """Elimina entradas expiradas para no acumular memoria."""
    ahora = datetime.now()
    expirados = [k for k, v in _cache.items() if ahora > v["expira_en"]]
    for k in expirados:
        del _cache[k]