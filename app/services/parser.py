"""
Servicio de parseo de certificaciones Excel.
Recibe el archivo en bytes, lo parsea y devuelve filas + errores.
"""
import io
import re
import math
from datetime import datetime
from typing import Any

import pandas as pd

COLS_QUERER = [
    "Item", "Nombre_Contrato", "Tarea", "Contrato", "UM",
    "Puntos_Gasnor", "Tipo", "Contratista", "Provincia", "Región",
    "Cantidades", "$ Unitario mes", "$ Total mes", "Observaciones", "Fecha",
]
COLS_BD = [
    "item_codigo", "nombre_contrato", "tarea", "contrato", "unidad_medida",
    "ptos_gasnor", "tipo", "contratista", "provincia", "region",
    "cantidades", "precio_unitario", "total_mes", "observaciones", "fecha",
]


def fmt_item(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    s = str(v).strip()
    try:
        f = float(s.replace(",", "."))
        return str(int(f)) if f == int(f) else str(round(f, 4))
    except (ValueError, TypeError):
        return s


def fmt_num(v) -> str | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, (int, float)):
        return str(v)
    s = re.sub(r"[\$\s]", "", str(v).strip())
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        float(s)
        return s
    except (ValueError, TypeError):
        return None


def fmt_fecha(v) -> str | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    except Exception:
        return None


def parsear_bytes(
    contenido: bytes,
    nombre_archivo: str,
    periodo_anio: int,
    periodo_mes: int,
) -> dict:
    """
    Parsea un archivo Excel en bytes.

    Retorna:
        {
            "hojas": [...],
            "filas": [...],
            "errores": [...],
            "periodo": "2026-03",
        }
    """
    resultado: dict[str, Any] = {
        "archivo": nombre_archivo,
        "hojas": [],
        "filas": [],
        "errores": [],
        "periodo": f"{periodo_anio}-{periodo_mes:02d}",
    }

    try:
        xl = pd.ExcelFile(io.BytesIO(contenido), engine="openpyxl")
    except Exception:
        try:
            xl = pd.ExcelFile(io.BytesIO(contenido), engine="calamine")
        except Exception as e:
            resultado["errores"].append({
                "hoja": "—", "fila": 0, "campo": "archivo",
                "mensaje": f"No se pudo abrir el archivo: {e}",
            })
            return resultado

    hojas_cert = [s for s in xl.sheet_names if s.strip().upper().startswith("CERTIF")]
    if not hojas_cert:
        hojas_cert = xl.sheet_names

    resultado["hojas"] = hojas_cert

    for hoja in hojas_cert:
        _procesar_hoja(xl, hoja, nombre_archivo, periodo_anio, periodo_mes, resultado)

    return resultado


def _procesar_hoja(xl, nombre_hoja, nombre_archivo, anio, mes, resultado):
    try:
        df = pd.read_excel(xl, sheet_name=nombre_hoja, engine=getattr(xl, "engine", None))
    except Exception as e:
        resultado["errores"].append({
            "hoja": nombre_hoja, "fila": 0, "campo": "hoja",
            "mensaje": f"No se pudo leer la hoja: {e}",
        })
        return

    cols_faltantes = [c for c in COLS_QUERER if c not in df.columns]
    if cols_faltantes:
        resultado["errores"].append({
            "hoja": nombre_hoja, "fila": 0, "campo": "columnas",
            "mensaje": f"Columnas no encontradas: {cols_faltantes}",
        })
        return

    df = df[COLS_QUERER].copy()
    df.columns = COLS_BD
    df = df[df["item_codigo"].notna() & df["contrato"].notna() & df["provincia"].notna()]

    for idx, (_, row) in enumerate(df.iterrows(), start=2):
        fila, errores_fila = _procesar_fila(row, nombre_hoja, idx, nombre_archivo, anio, mes)
        resultado["filas"].append(fila)
        resultado["errores"].extend(errores_fila)


def _procesar_fila(row, hoja, num_fila, archivo, anio, mes):
    errores = []

    item_codigo    = fmt_item(row.get("item_codigo"))
    contrato       = str(row.get("contrato") or "").strip().upper()
    provincia      = str(row.get("provincia") or "").strip().title()
    region         = str(row.get("region") or "").strip()
    nombre_cont    = str(row.get("nombre_contrato") or "").strip() or None
    tarea          = str(row.get("tarea") or "").strip() or None
    unidad_medida  = str(row.get("unidad_medida") or "").strip() or None
    tipo           = str(row.get("tipo") or "").strip() or None
    contratista    = str(row.get("contratista") or "").strip() or None
    observaciones  = str(row.get("observaciones") or "").strip() or None
    ptos_gasnor    = fmt_num(row.get("ptos_gasnor"))
    cantidades     = fmt_num(row.get("cantidades"))
    precio_unit    = fmt_num(row.get("precio_unitario"))
    total_mes      = fmt_num(row.get("total_mes"))

    if not provincia or provincia.lower() == "nan":
        errores.append({"hoja": hoja, "fila": num_fila, "campo": "provincia",
                         "mensaje": "Provincia vacía."})

    if cantidades and float(cantidades) == 0:
        errores.append({"hoja": hoja, "fila": num_fila, "campo": "cantidades",
                         "mensaje": "Cantidad es 0."})

    if cantidades and precio_unit and total_mes:
        calc = round(float(cantidades) * float(precio_unit), 2)
        if abs(calc - round(float(total_mes), 2)) > 2:
            errores.append({"hoja": hoja, "fila": num_fila, "campo": "total_mes",
                             "mensaje": f"Total ({total_mes}) ≠ cant × precio ({calc:.2f})"})

    tiene_error = any(
        e["hoja"] == hoja and e["fila"] == num_fila and e["campo"] == "provincia"
        for e in errores
    )

    fila = {
        "hoja_origen":     hoja,
        "archivo_origen":  archivo,
        "item_codigo":     item_codigo,
        "nombre_contrato": nombre_cont,
        "tarea":           tarea,
        "contrato":        contrato,
        "unidad_medida":   unidad_medida,
        "ptos_gasnor":     ptos_gasnor,
        "tipo":            tipo,
        "contratista":     contratista,
        "provincia":       provincia,
        "region":          region if region != "nan" else "",
        "cantidades":      cantidades,
        "precio_unitario": precio_unit,
        "total_mes":       total_mes,
        "observaciones":   observaciones,
        "fecha":           f"{anio}-{mes:02d}-01",
        "tiene_error":     tiene_error,
    }
    return fila, errores
