"""
parser.py — busca columnas por nombre, no por posición.
Robusto ante cualquier variación de formato del Excel de Naturgy.
"""
import io, re, math
from typing import Any
import pandas as pd

# Mapeo flexible: variantes de nombre → nombre canónico
COL_ALIAS = {
    "item_codigo":    ["ÍTEMS", "ITEMS", "ÍTEM", "ITEM"],
    "nombre_contrato":["NOMBRE CONTRATO", "NOMBRE_CONTRATO"],
    "tarea":          ["TAREA"],
    "contrato":       ["K GASNOR", "K_GASNOR", "K GASNOR "],
    "unidad_medida":  ["UM", "UNIDAD", "UNIDAD MEDIDA"],
    "ptos_gasnor":    ["PTOS. GASNOR", "PTOS GASNOR", "PUNTOS GASNOR"],
    "tipo":           ["TIPO"],
    "contratista":    ["CONTRATISTA"],
    "provincia":      ["PROVINCIA"],
    "cantidades":     ["CANTIDADES", "CANTIDAD"],
    "precio_unitario":["$ UNITARIO MES", "UNITARIO MES", "$ UNITARIO", "PRECIO UNITARIO"],
    "total_mes":      ["$ TOTAL MES", "TOTAL MES", "$ TOTAL", "TOTAL"],
    "observaciones":  ["OBSERVACIONES", "OBS", "OBSERVACION"],
}


def _mapear_columnas(header_vals: list[str]) -> dict[str, str]:
    """
    Dado el header, devuelve un dict {nombre_canonico: nombre_en_header}
    para cada columna que encontremos.
    """
    header_upper = [str(v).strip().upper() for v in header_vals]
    mapa = {}
    for canon, aliases in COL_ALIAS.items():
        for alias in aliases:
            if alias in header_upper:
                # Usar el nombre real del header (con mayúsculas originales)
                idx = header_upper.index(alias)
                mapa[canon] = header_vals[idx]
                break
    return mapa


def parsear_bytes(contenido: bytes, nombre_archivo: str,
                  periodo_anio: int, periodo_mes: int) -> dict:
    resultado: dict[str, Any] = {
        "archivo": nombre_archivo, "hojas": [],
        "filas": [], "errores": [],
        "periodo": f"{periodo_anio}-{periodo_mes:02d}",
    }
    try:
        xl = pd.ExcelFile(io.BytesIO(contenido), engine="openpyxl")
    except Exception:
        try:
            xl = pd.ExcelFile(io.BytesIO(contenido), engine="calamine")
        except Exception as e:
            resultado["errores"].append({"hoja":"—","fila":0,"campo":"archivo",
                                          "mensaje":f"No se pudo abrir: {e}"})
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
        resultado["errores"].append({"hoja":nombre_hoja,"fila":0,"campo":"hoja",
                                      "mensaje":f"No se pudo leer: {e}"})
        return

    # Buscar fila que contenga ÍTEMS/ITEMS
    header_idx = _encontrar_header_idx(df_raw)
    if header_idx is None:
        resultado["errores"].append({"hoja":nombre_hoja,"fila":0,"campo":"header",
                                      "mensaje":"No se encontró la fila de encabezado (ÍTEMS)."})
        return

    meta = _extraer_meta(df_raw, nombre_hoja, anio, mes)

    # Header como lista de strings (upper para comparar)
    header_raw  = list(df_raw.iloc[header_idx])
    header_upper= [str(v).strip().upper() if pd.notna(v) else "" for v in header_raw]

    # Mapear columnas por nombre
    col_map = _mapear_columnas(header_upper)

    if "item_codigo" not in col_map:
        resultado["errores"].append({"hoja":nombre_hoja,"fila":0,"campo":"header",
                                      "mensaje":f"Columna ÍTEMS no encontrada. Header: {header_upper[:12]}"})
        return

    # Construir df con header
    df_datos = df_raw.iloc[header_idx + 1:].copy()
    df_datos.columns = header_upper          # nombres en UPPER para buscar
    df_datos = df_datos.reset_index(drop=True)

    # Columna de ítem
    col_item = col_map["item_codigo"]        # nombre upper de la col
    df_datos = df_datos[df_datos[col_item].apply(_es_item_valido)]

    for idx, (_, row) in enumerate(df_datos.iterrows(), start=header_idx + 2):
        fila, errores = _procesar_fila(row, col_map, nombre_hoja, idx, nombre_archivo, meta)
        resultado["filas"].append(fila)
        resultado["errores"].extend(errores)


def _encontrar_header_idx(df: pd.DataFrame):
    """Busca la fila que contiene ÍTEMS o ITEMS en cualquier columna."""
    for i, row in df.iterrows():
        for val in row:
            if isinstance(val, str) and val.strip().upper() in ("ÍTEMS", "ITEMS"):
                return i
    return None


def _extraer_meta(df: pd.DataFrame, nombre_hoja: str, anio: int, mes: int) -> dict:
    meta = {"k_gasnor": None, "nro_np": None,
            "fecha": f"{anio}-{mes:02d}-01"}
    m = re.search(r'K\d+', nombre_hoja.upper())
    if m:
        meta["k_gasnor"] = m.group(0)
    for _, row in df.iloc[:13].iterrows():
        vals = [str(v).strip() for v in row if pd.notna(v) and str(v).strip() not in ("","nan")]
        for v in vals:
            if re.match(r"^K\d+$", v.upper()) and not meta["k_gasnor"]:
                meta["k_gasnor"] = v.upper()
        fila_str = " ".join(vals).upper()
        if ("NRO. DE NP" in fila_str or "NRO DE NP" in fila_str) and not meta["nro_np"]:
            for i, v in enumerate(vals):
                if "NP" in v.upper() and i+1 < len(vals):
                    meta["nro_np"] = vals[i+1]
    return meta


def _procesar_fila(row, col_map: dict, hoja, num_fila, archivo, meta):
    """
    col_map: {nombre_canonico: nombre_upper_en_header}
    row: Series con index = nombres upper del header
    """
    errores = []

    def get(campo):
        """Obtiene el valor de la columna canónica, o None si no existe."""
        col = col_map.get(campo)
        if col is None:
            return None
        v = row.get(col)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        s = str(v).strip()
        return s if s and s.upper() not in ("NAN", "NAT", "NONE", "#N/A", "") else None

    def fmt_num(v):
        if v is None: return None
        s = re.sub(r"[\$\s]", "", v)
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        try: float(s); return s
        except: return None

    def fmt_item(v):
        if v is None: return ""
        try:
            f = float(v.replace(",", "."))
            return str(int(f)) if f == int(f) else str(round(f, 4))
        except: return v

    item_codigo    = fmt_item(get("item_codigo"))
    nombre_contrato= get("nombre_contrato")
    tarea          = get("tarea")
    contrato       = (get("contrato") or "").strip().upper() or meta.get("k_gasnor","")
    unidad_medida  = get("unidad_medida")
    ptos_gasnor    = fmt_num(get("ptos_gasnor"))
    tipo           = get("tipo")
    contratista    = get("contratista")
    provincia      = (get("provincia") or "").strip().title() or None
    cantidades     = fmt_num(get("cantidades"))
    precio_unit    = fmt_num(get("precio_unitario"))
    total_mes      = fmt_num(get("total_mes"))
    observaciones  = get("observaciones")

    # Normalizar contrato
    if contrato and not contrato.startswith("K"):
        contrato = "K" + contrato.lstrip("kK")

    tiene_error = False
    if not provincia:
        errores.append({"hoja":hoja,"fila":num_fila,"campo":"provincia",
                         "mensaje":"Provincia vacía."})
        tiene_error = True
    if not contrato:
        errores.append({"hoja":hoja,"fila":num_fila,"campo":"contrato",
                         "mensaje":"Contrato K no detectado."})
        tiene_error = True
    if cantidades and float(cantidades) == 0:
        errores.append({"hoja":hoja,"fila":num_fila,"campo":"cantidades",
                         "mensaje":"Cantidad es 0."})

    return {
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
    }, errores


def _extraer_region(nombre_hoja: str) -> str:
    h = nombre_hoja.upper()
    if "NORTE" in h: return "Norte"
    if "SUR"   in h: return "Sur"
    return ""


def _es_item_valido(v) -> bool:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return False
    s = str(v).strip()
    if not s or s.upper() in ("NAN","NAT","NONE","ÍTEMS","ITEMS",""):
        return False
    try:
        float(s.replace(",","."))
        return True
    except:
        return bool(re.match(r"^[A-Za-z0-9][A-Za-z0-9\s\-_,\.]*$", s))