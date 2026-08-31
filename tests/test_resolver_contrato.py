"""
Tests de la resolución única de contrato (sesión 2026-08-31):

Prioridad: contrato editado por el usuario > contrato del maestro (dim_item)
> contrato del archivo (solo si el ítem no está en el maestro).

Si el ítem existe en VARIOS contratos del maestro, se prefiere el del archivo
si está entre ellos; si no, el primero en orden determinista (ORDER BY id_item).
"""
from app.services.carga import resolver_contrato_final, anotar_contrato_final


class _FakeDB:
    """db.execute(...).fetchall() devuelve los contratos del maestro para el ítem."""

    def __init__(self, contratos_maestro):
        self._rows = [(k,) for k in contratos_maestro]
        self.consultas = 0

    def execute(self, *_args, **_kw):
        self.consultas += 1
        return self

    def fetchall(self):
        return self._rows


def test_contrato_editado_gana_siempre_y_no_consulta_maestro():
    db = _FakeDB(["K6"])
    k, fuente = resolver_contrato_final(db, "431,2", "K11", contrato_editado="K12")
    assert (k, fuente) == ("K12", "editado")
    assert db.consultas == 0


def test_maestro_gana_sobre_el_archivo():
    db = _FakeDB(["K6"])
    k, fuente = resolver_contrato_final(db, "431,2", "K11")
    assert (k, fuente) == ("K6", "maestro")


def test_item_en_varios_contratos_prefiere_el_del_archivo():
    db = _FakeDB(["K6", "K11"])
    k, fuente = resolver_contrato_final(db, "431,2", "K11")
    assert (k, fuente) == ("K11", "maestro")


def test_item_en_varios_contratos_sin_match_toma_el_primero():
    db = _FakeDB(["K6", "K11"])
    k, fuente = resolver_contrato_final(db, "431,2", "K99")
    assert (k, fuente) == ("K6", "maestro")


def test_item_fuera_del_maestro_usa_el_del_archivo():
    db = _FakeDB([])
    k, fuente = resolver_contrato_final(db, "999,9", "K11")
    assert (k, fuente) == ("K11", "archivo")


def test_item_fuera_del_maestro_y_sin_archivo_queda_none():
    db = _FakeDB([])
    k, fuente = resolver_contrato_final(db, "999,9", "")
    assert (k, fuente) == (None, "archivo")


def test_item_vacio_no_consulta_maestro():
    db = _FakeDB(["K6"])
    k, fuente = resolver_contrato_final(db, "", "K11")
    assert (k, fuente) == ("K11", "archivo")
    assert db.consultas == 0


# ── anotar_contrato_final ────────────────────────────────────

def test_anotar_reasignacion_del_maestro():
    fila = {"item_codigo": "431,2", "contrato": "K11"}
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    assert fila["contrato"] == "K6"
    assert fila["contrato_archivo"] == "K11"
    assert fila["contrato_fuente"] == "maestro"
    assert fila["contrato_del_maestro"] == "K6"


def test_anotar_preserva_contrato_archivo_en_pasadas_sucesivas():
    # El preview puede anotar y el confirmar volver a anotar: el original
    # del archivo no debe pisarse con el contrato ya resuelto.
    fila = {"item_codigo": "431,2", "contrato": "K11"}
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    assert fila["contrato_archivo"] == "K11"
    assert fila["contrato"] == "K6"


def test_anotar_item_fuera_del_maestro_deja_el_del_archivo():
    fila = {"item_codigo": "999,9", "contrato": "K11"}
    anotar_contrato_final(_FakeDB([]), fila)
    assert fila["contrato"] == "K11"
    assert fila["contrato_fuente"] == "archivo"
    assert fila["contrato_del_maestro"] is None


def test_anotar_respeta_contrato_editado():
    fila = {"item_codigo": "431,2", "contrato": "K6", "contrato_editado": "K12"}
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    assert fila["contrato"] == "K12"
    assert fila["contrato_fuente"] == "editado"
