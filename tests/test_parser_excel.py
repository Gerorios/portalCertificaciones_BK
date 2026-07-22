"""
Tests del parser de Excel: extracción del total declarado del encabezado.
"""
import io
import openpyxl

from app.services.parser import parsear_bytes


def _xlsx_bytes():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "CERTIFICADO K6"
    ws.append(["CONTRATISTA", "SER&TEC"])
    ws.append(["K", "K6"])
    ws.append(["TOTAL MES", "$ 39.072.433,92"])
    ws.append([])
    ws.append(["ÍTEMS", "TAREA", "K GASNOR", "PROVINCIA",
               "CANTIDADES", "$ UNITARIO MES", "$ TOTAL MES"])
    ws.append(["431", "Atención de urgencias P1", "K6", "Jujuy",
               15, 59164.80, 887472.00])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_total_declarado_desde_excel():
    r = parsear_bytes(_xlsx_bytes(), "test.xlsx", 2025, 2)
    assert r["total_declarado"] == 39072433.92


def test_fila_simple_se_sigue_parseando():
    r = parsear_bytes(_xlsx_bytes(), "test.xlsx", 2025, 2)
    assert len(r["filas"]) == 1
    f = r["filas"][0]
    assert f["item_codigo"] == "431"
    assert float(f["total_mes"]) == 887472.00


def test_total_declarado_numerico_directo():
    # El Excel puede tener el total como número, no como texto con $
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["TOTAL MES", 1234567.89])
    ws.append(["ÍTEMS", "CANTIDADES"])
    ws.append(["431", 5])
    buf = io.BytesIO()
    wb.save(buf)
    r = parsear_bytes(buf.getvalue(), "test.xlsx", 2025, 2)
    assert r["total_declarado"] == 1234567.89
