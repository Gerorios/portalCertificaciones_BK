"""
Tests del fallback de ptos_gasnor en la carga (backlog CONTEXTO §13):

- Si el archivo trae ptos_gasnor → se usa el del archivo (coincide con Power BI).
- Si el archivo NO lo trae (None o "") → se usa el del maestro (dim_item).
- Si el maestro tampoco lo tiene → queda None (como hoy).
"""
from app.services.carga import _ptos_gasnor_con_fallback


class _FakeDB:
    """Simula db.execute(...).fetchone() devolviendo una fila fija."""

    def __init__(self, fila):
        self._fila = fila
        self.consultas = 0

    def execute(self, *_args, **_kw):
        self.consultas += 1
        return self

    def fetchone(self):
        return self._fila


def test_valor_del_archivo_tiene_prioridad_y_no_consulta_maestro():
    db = _FakeDB(fila=(9999.0,))
    assert _ptos_gasnor_con_fallback(db, "7500.00", id_item=1) == "7500.00"
    assert db.consultas == 0


def test_sin_valor_en_archivo_usa_el_maestro():
    db = _FakeDB(fila=(7500.0,))
    assert _ptos_gasnor_con_fallback(db, None, id_item=1) == 7500.0


def test_string_vacio_cuenta_como_sin_valor():
    db = _FakeDB(fila=(7500.0,))
    assert _ptos_gasnor_con_fallback(db, "", id_item=1) == 7500.0


def test_sin_valor_en_archivo_ni_maestro_queda_none():
    db = _FakeDB(fila=None)
    assert _ptos_gasnor_con_fallback(db, None, id_item=1) is None


def test_maestro_con_null_explicito_queda_none():
    db = _FakeDB(fila=(None,))
    assert _ptos_gasnor_con_fallback(db, None, id_item=1) is None
