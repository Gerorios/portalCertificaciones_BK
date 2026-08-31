"""
Tests de las reglas de validación de filas (CONTEXT.md):

- Fila cargable: ítem en maestro + contrato K + provincia válida
  + cantidad != 0 + total presente. El unitario puede faltar.
- Fila incompleta: tiene total o unitario pero no es cargable → se muestra con error.
- Fila de plantilla: sin cantidad y sin ningún monto → se oculta.
- Fila excluida: el usuario la destildó → no se carga.
"""
from app.services.validacion import (
    es_fila_plantilla,
    revalidar_fila,
    filtrar_visibles_preview,
    filtrar_cargables,
)


def fila_base(**kw):
    f = {
        "item_codigo":     "431,2",
        "contrato":        "K6",
        "provincia":       "Jujuy",
        "cantidades":      "3",
        "precio_unitario": "4437360.00",
        "total_mes":       "13312080.00",
        "tiene_error":     False,
    }
    f.update(kw)
    return f


# ── contenido monetario / plantilla ──────────────────────────

def test_fila_sin_montos_ni_cantidad_es_plantilla():
    f = fila_base(cantidades=None, precio_unitario=None, total_mes=None)
    assert es_fila_plantilla(f)

def test_fila_con_total_no_es_plantilla_aunque_falte_cantidad():
    assert not es_fila_plantilla(fila_base(cantidades=None))

def test_fila_cantidad_cero_sin_montos_es_plantilla():
    f = fila_base(cantidades="0", precio_unitario=None, total_mes=None)
    assert es_fila_plantilla(f)

def test_fila_cantidad_cero_con_solo_unitario_es_plantilla():
    # Ruido de catálogo Naturgy: unitario cargado, nada certificado → se oculta
    f = fila_base(cantidades="0", total_mes=None)
    assert es_fila_plantilla(f)

def test_fila_sin_cantidad_con_solo_unitario_es_plantilla():
    f = fila_base(cantidades=None, total_mes=None)
    assert es_fila_plantilla(f)

def test_fila_cantidad_cero_con_total_cero_es_plantilla():
    f = fila_base(cantidades="0", total_mes="0.00")
    assert es_fila_plantilla(f)

def test_fila_cantidad_cero_con_total_con_plata_no_es_plantilla():
    # Anomalía real: hay plata declarada sin cantidad → debe verse (con error)
    f = fila_base(cantidades="0")
    assert not es_fila_plantilla(f)

def test_fila_con_cantidad_y_total_no_es_plantilla():
    assert not es_fila_plantilla(fila_base())


# ── revalidación (el flag nunca se congela) ──────────────────

def test_fila_completa_es_cargable():
    err, detalle = revalidar_fila(fila_base())
    assert err is False

def test_sin_unitario_pero_con_total_es_cargable():
    err, _ = revalidar_fila(fila_base(precio_unitario=None))
    assert err is False

def test_sin_cantidad_no_es_cargable():
    err, detalle = revalidar_fila(fila_base(cantidades=None))
    assert err is True
    assert "cantidad" in detalle.lower()

def test_sin_total_no_es_cargable():
    err, detalle = revalidar_fila(fila_base(total_mes=None))
    assert err is True

def test_sin_provincia_no_es_cargable():
    err, detalle = revalidar_fila(fila_base(provincia=""))
    assert err is True

def test_sin_contrato_no_es_cargable():
    err, _ = revalidar_fila(fila_base(contrato=""))
    assert err is True

def test_item_inexistente_no_es_cargable():
    err, detalle = revalidar_fila(fila_base(), item_existe=False)
    assert err is True
    assert "maestro" in detalle.lower()

def test_flag_viejo_del_parser_se_ignora():
    # La fila fue marcada con error por el parser pero el usuario la corrigió:
    # la revalidación debe darla por buena sin importar el flag congelado.
    err, _ = revalidar_fila(fila_base(tiene_error=True))
    assert err is False

def test_provincia_invalida_no_es_cargable():
    err, detalle = revalidar_fila(
        fila_base(provincia="Jujui"),
        provincias_validas=["Jujuy", "Salta"],
    )
    assert err is True


# ── visibilidad en preview ───────────────────────────────────

def test_preview_muestra_incompleta_y_oculta_plantilla():
    incompleta = fila_base(cantidades=None)                        # total con plata → se ve
    catalogo   = fila_base(cantidades="0", total_mes=None)         # solo unitario → se oculta
    plantilla  = fila_base(cantidades=None, precio_unitario=None,
                           total_mes=None)                         # ruido → se oculta
    normal     = fila_base()
    visibles = filtrar_visibles_preview([incompleta, catalogo, plantilla, normal])
    assert incompleta in visibles
    assert normal in visibles
    assert catalogo not in visibles
    assert plantilla not in visibles


# ── filtrado al confirmar ────────────────────────────────────

def test_confirmar_ignora_flag_y_revalida():
    corregida = fila_base(tiene_error=True)   # el usuario la arregló
    cargables = filtrar_cargables([corregida])
    assert len(cargables) == 1

def test_confirmar_descarta_excluidas():
    excluida = fila_base(excluida=True)
    assert filtrar_cargables([excluida]) == []

def test_confirmar_descarta_no_cargables():
    rota = fila_base(cantidades=None)
    assert filtrar_cargables([rota]) == []
