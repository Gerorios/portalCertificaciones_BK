"""
Tests del parser PDF contra el certificado real SERYTEC FEB-25 K6.

Verdad de base (leída del PDF a ojo):
- 12 filas simples de ítems 431/432/433 (Jujuy y Salta) — ya salían bien.
- 431,2 Jujuy:  cant 3,   unit $4.437.360,00, total $13.312.080,00 (multilínea)
- 431,3 Jujuy:  cant 112, unit $7.888,64,     total $883.527,68
- 431,2 Salta:  cant 1,   unit $4.437.360,00, total $4.437.360,00 (multilínea)
- TOTAL MES declarado en encabezado: $39.072.433,92
"""
import pytest
from pathlib import Path

from app.services.parser_pdf import parsear_pdf_bytes

FIXTURE = Path(__file__).parent / "fixtures" / "SERYTEC FEB-25 K6.pdf"


@pytest.fixture(scope="module")
def resultado():
    contenido = FIXTURE.read_bytes()
    return parsear_pdf_bytes(contenido, "SERYTEC FEB-25 K6.pdf", 2025, 2)


def _filas(resultado, item):
    return [f for f in resultado["filas"] if f["item_codigo"] == item]


def test_431_2_jujuy_sale_completa(resultado):
    filas = [f for f in _filas(resultado, "431,2") if f["provincia"] == "Jujuy"]
    assert len(filas) == 1
    f = filas[0]
    assert float(f["cantidades"]) == 3
    assert float(f["total_mes"]) == 13312080.00
    assert float(f["precio_unitario"]) == 4437360.00
    assert f["tiene_error"] is False


def test_431_2_salta_sale_completa(resultado):
    filas = [f for f in _filas(resultado, "431,2") if f["provincia"] == "Salta"]
    assert len(filas) == 1
    f = filas[0]
    assert float(f["cantidades"]) == 1
    assert float(f["total_mes"]) == 4437360.00
    assert f["tiene_error"] is False


def test_431_2_solo_dos_filas(resultado):
    # Una por Jujuy y una por Salta — sin fragmentos partidos
    assert len(_filas(resultado, "431,2")) == 2


def test_431_3_sin_fila_fantasma(resultado):
    # El PDF tiene UNA sola fila 431,3 (Jujuy, cant 112). El parser viejo
    # inventaba una segunda con la cantidad 3 robada del 431,2.
    filas = _filas(resultado, "431,3")
    assert len(filas) == 1
    f = filas[0]
    assert float(f["cantidades"]) == 112
    assert float(f["total_mes"]) == 883527.68


def test_filas_simples_intactas(resultado):
    # Las 12 filas de 431/432/433 no deben romperse con el cambio
    simples = [f for f in resultado["filas"] if f["item_codigo"] in ("431", "432", "433")]
    assert len(simples) == 12
    assert all(f["provincia"] in ("Jujuy", "Salta") for f in simples)
    assert all(f["cantidades"] and f["total_mes"] for f in simples)


def test_suma_total_coincide_con_declarado(resultado):
    suma = sum(float(f["total_mes"] or 0) for f in resultado["filas"])
    assert suma == pytest.approx(39072433.92, abs=0.01)


def test_total_declarado_extraido(resultado):
    assert resultado["total_declarado"] == pytest.approx(39072433.92, abs=0.01)
