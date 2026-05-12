"""
parser.py
=========
Lee archivos Excel de certificaciones de Naturgy en formato crudo
(tal como los envía Naturgy, con encabezado administrativo y tabla
de datos que empieza con la fila que contiene 'ÍTEMS').

Funciona con todos los contratos K y sus variantes de formato.
"""
import io
import re
import math
from typing import Any

import pandas as pd


# Columnas que buscamos en el header real de la tabla
# Mapeamos nombre en el Excel → nombre en la BD
COL_MAP = {
    "ÍTEMS":           "item_codigo",
    "ITEMS":           "item_codigo",
    "NOMBRE CONTRATO": "nombre_contrato",
    "TAREA":           "tarea",
    "K GASNOR":        "contrato",
    "UM":              "unidad_medida",
    "PTOS. GASNOR":    "ptos_gasnor",
    "TIPO":            "tipo",
    "CONTRATISTA":     "contratista",
    "PROVINCIA":       "provincia",
    "CANTIDADES":      "cantidades",
    "$ UNITARIO MES":  "precio_unitario",
    "$ TOTAL MES":     "total_mes",
    "OBSERVACIONES":   "observaciones",
}

# Columnas que descartamos aunque estén en el header
COLS_DESCARTAR = {
    "OG", "BORRAR DESPUES DE PONER LAS CUENTAS",
    "AUX", "TEXTO", "CUENTA",
}


def parsear_bytes(
    contenido: bytes,
    nombre_archivo: str,
    periodo_anio: int,
    periodo_mes: int,
) -> dict:
    resultado: dict[str, Any] = {
        "archivo":  nombre_archivo,
        "hojas":    [],
        "filas":    [],
        "errores":  [],
        "periodo":  f"{periodo_anio}-{periodo_mes:02d}",
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
        df_raw = pd.read_excel(xl, sheet_name=nombre_hoja, header=None,
                               engine=getattr(xl, "engine", None))
    except Exception as e:
        resultado["errores"].append({
            "hoja": nombre_hoja, "fila": 0, "campo": "hoja",
            "mensaje": f"No se pudo leer: {e}",
        })
        return

    # Buscar la fila del header (la que contiene ÍTEMS o ITEMS)
    header_idx, col_offset = _encontrar_header(df_raw)
    if header_idx is None:
        resultado["errores"].append({
            "hoja": nombre_hoja, "fila": 0, "campo": "header",
            "mensaje": "No se encontró la fila de encabezado (ÍTEMS).",
        })
        return

    # Extraer metadatos del encabezado (K, NP, fechas)
    meta = _extraer_meta(df_raw, nombre_hoja, anio, mes)

    # Construir DataFrame con el header correcto
    header_vals = [
        str(v).strip().upper() if pd.notna(v) else ""
        for v in df_raw.iloc[header_idx]
    ]

    df_datos = df_raw.iloc[header_idx + 1:].copy()
    df_datos.columns = header_vals
    df_datos = df_datos.reset_index(drop=True)

    # Descartar filas vacías o de totales
    col_item = "ÍTEMS" if "ÍTEMS" in header_vals else "ITEMS"
    if col_item not in df_datos.columns:
        resultado["errores"].append({
            "hoja": nombre_hoja, "fila": 0, "campo": "header",
            "mensaje": f"Columna ÍTEMS no encontrada. Columnas: {header_vals[:10]}",
        })
        return

    df_datos = df_datos[df_datos[col_item].apply(_es_item_valido)]

    for idx, (_, row) in enumerate(df_datos.iterrows(), start=header_idx + 2):
        fila, errores_fila = _procesar_fila(
            row, header_vals, col_item, nombre_hoja,
            idx, nombre_archivo, meta
        )
        resultado["filas"].append(fila)
        resultado["errores"].extend(errores_fila)


def _encontrar_header(df: pd.DataFrame):
    """Busca la fila que contiene ÍTEMS o ITEMS."""
    for i, row in df.iterrows():
        for j, val in enumerate(row):
            if isinstance(val, str) and val.strip().upper() in ("ÍTEMS", "ITEMS"):
                return i, j
    return None, 0


def _extraer_meta(df: pd.DataFrame, nombre_hoja: str, anio: int, mes: int) -> dict:
    """Extrae K, NRO NP y otros metadatos del encabezado."""
    meta = {
        "k_gasnor": None,
        "nro_np":   None,
        "fecha":    f"{anio}-{mes:02d}-01",
    }

    # Intentar detectar el K desde el nombre de la hoja
    m = re.search(r'K\d+', nombre_hoja.upper())
    if m:
        meta["k_gasnor"] = m.group(0)

    for _, row in df.iloc[:13].iterrows():
        valores = [str(v).strip() for v in row if pd.notna(v) and str(v).strip() not in ("", "nan")]
        fila_str = " ".join(valores).upper()

        # Detectar K
        if not meta["k_gasnor"]:
            for v in valores:
                if re.match(r"^K\d+$", v.upper()):
                    meta["k_gasnor"] = v.upper()

        # Detectar NRO NP
        if "NRO. DE NP" in fila_str or "NRO DE NP" in fila_str:
            for i, v in enumerate(valores):
                if "NP" in v.upper() and i + 1 < len(valores):
                    meta["nro_np"] = valores[i + 1]

    return meta


def _procesar_fila(row, header_vals, col_item, hoja, num_fila, archivo, meta):
    errores = []

    def get_col(*nombres):
        """Busca la primera columna que coincida (insensible a mayúsculas)."""
        for n in nombres:
            nu = n.upper()
            if nu in header_vals:
                v = row.get(nu)
                # Extraer valor escalar si es Series
                if isinstance(v, pd.Series):
                    v = v.iloc[0] if len(v) > 0 else None
                if pd.notna(v) and str(v).strip() not in ("", "nan", "NaT"):
                    return v
        return None

    def fmt_str(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        s = str(v).strip()
        return s if s and s.upper() not in ("NAN", "NAT", "NONE", "#N/A") else None

    def fmt_num(v):
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
        except:
            return None

    def fmt_item(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ""
        s = str(v).strip()
        try:
            f = float(s.replace(",", "."))
            return str(int(f)) if f == int(f) else str(round(f, 4))
        except:
            return s

    # Extraer valores
    item_codigo     = fmt_item(get_col("ÍTEMS", "ITEMS"))
    nombre_contrato = fmt_str(get_col("NOMBRE CONTRATO"))
    tarea           = fmt_str(get_col("TAREA"))
    contrato_raw    = fmt_str(get_col("K GASNOR"))
    contrato        = (contrato_raw or "").strip().upper() or meta.get("k_gasnor", "")
    unidad_medida   = fmt_str(get_col("UM"))
    ptos_gasnor     = fmt_num(get_col("PTOS. GASNOR", "PTOS GASNOR"))
    tipo            = fmt_str(get_col("TIPO"))
    contratista     = fmt_str(get_col("CONTRATISTA"))
    provincia       = fmt_str(get_col("PROVINCIA"))
    cantidades      = fmt_num(get_col("CANTIDADES"))
    precio_unit     = fmt_num(get_col("$ UNITARIO MES"))
    total_mes       = fmt_num(get_col("$ TOTAL MES"))
    observaciones   = fmt_str(get_col("OBSERVACIONES"))

    # Normalizar provincia
    if provincia:
        provincia = provincia.strip().title()

    # Normalizar contrato
    if contrato and not contrato.startswith("K"):
        contrato = "K" + contrato.lstrip("kK")

    # ── Validaciones ──
    tiene_error = False

    if not provincia:
        errores.append({"hoja": hoja, "fila": num_fila, "campo": "provincia",
                        "mensaje": "Provincia vacía."})
        tiene_error = True

    if not contrato:
        errores.append({"hoja": hoja, "fila": num_fila, "campo": "contrato",
                        "mensaje": "Contrato K no detectado."})
        tiene_error = True

    if cantidades and float(cantidades) == 0:
        errores.append({"hoja": hoja, "fila": num_fila, "campo": "cantidades",
                        "mensaje": "Cantidad es 0."})

    if cantidades and precio_unit and total_mes:
        calc = round(float(cantidades) * float(precio_unit), 2)
        real = round(float(total_mes), 2)
        if abs(calc - real) > 2:
            errores.append({"hoja": hoja, "fila": num_fila, "campo": "total_mes",
                            "mensaje": f"Total ({real}) ≠ cant × precio ({calc:.2f})"})

    fila = {
        "hoja_origen":     hoja,
        "archivo_origen":  archivo,
        "item_codigo":     item_codigo,
        "nombre_contrato": nombre_contrato,
        "tarea":           tarea,
        "contrato":        contrato,
        "unidad_medida":   unidad_medida,
        "ptos_gasnor":     ptos_gasnor,
        "tipo":            tipo,
        "contratista":     contratista,
        "provincia":       provincia or "",
        "region":          _extraer_region(hoja),
        "cantidades":      cantidades,
        "precio_unitario": precio_unit,
        "total_mes":       total_mes,
        "observaciones":   observaciones,
        "fecha":           meta["fecha"],
        "nro_np":          meta.get("nro_np"),
        "tiene_error":     tiene_error,
    }
    return fila, errores


def _extraer_region(nombre_hoja: str) -> str:
    """Extrae Zona Norte/Sur del nombre de la hoja si existe."""
    h = nombre_hoja.upper()
    if "NORTE" in h:
        return "Norte"
    if "SUR" in h:
        return "Sur"
    return ""


def _es_item_valido(v) -> bool:
    """Devuelve True si el valor puede ser un código de ítem válido."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return False
    s = str(v).strip()
    if not s or s.upper() in ("NAN", "NAT", "NONE", "ÍTEMS", "ITEMS"):
        return False
    # Acepta enteros, decimales con punto o coma, y alfanuméricos tipo D858
    try:
        float(s.replace(",", "."))
        return True
    except (ValueError, TypeError):
        # Alfanumérico tipo D858
        return bool(re.match(r"^[A-Za-z]?\d+[,.]?\d*$", s))