"""El estado de cargas se abre al jefe, acotado a sus contratos (mockup 2026-08-31)."""
from types import SimpleNamespace
from app.routers.analytics import contratos_visibles

TODOS = ["K2", "K5", "K6", "K8", "K9", "K10", "K11", "K12"]

def _u(rol, ks=None):
    return SimpleNamespace(rol=rol, contratos_list=ks or [])

def test_admin_y_gerente_ven_todos():
    assert contratos_visibles(_u("admin"), TODOS) == TODOS
    assert contratos_visibles(_u("gerente"), TODOS) == TODOS

def test_jefe_ve_solo_sus_k_en_el_orden_del_maestro():
    assert contratos_visibles(_u("jefe", ["K11", "K6"]), TODOS) == ["K6", "K11"]

def test_jefe_sin_contratos_ve_lista_vacia():
    assert contratos_visibles(_u("jefe"), TODOS) == []

def test_k_asignado_que_no_existe_en_maestro_se_ignora():
    assert contratos_visibles(_u("jefe", ["K6", "K99"]), TODOS) == ["K6"]
