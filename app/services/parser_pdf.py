"""
parser_pdf.py
=============
Lee PDFs de certificaciones de Naturgy.
Detecta los límites de columna desde el x0 real de cada palabra del header.
Pega números partidos (problema de pdfplumber con PDFs A3).
"""
import io
import re
from typing import Any, Optional

import pdfplumber

# Palabras del header → nombre canónico del campo
HEADER_PALABRAS = {
    "ÍTEMS":        "item_codigo",
    "ITEMS":        "item_codigo",
    "TAREA":        "tarea",
    "K":            "contrato",
    "UM":           "unidad_medida",
    "PTOS.":        "ptos_gasnor",
    "TIPO":         "tipo",
    "CONTRATISTA":  "contratista",
    "PROVINCIA":    "provincia",
    "CANTIDADES":   "cantidades",
    "CANTIDADES":   "cantidades",
    "UNITARIO":     "precio_unitario",
    "TOTAL":        "total_mes",
    "OBSERVACIONES":"observaciones",
}

PROVINCIAS = {
    "salta":    "Salta",
    "jujuy":    "Jujuy",
    "tucumán":  "Tucumán",
    "tucuman":  "Tucumán",
    "santiago": "Santiago del Estero",
    "catamarca":"Catamarca",
}


def parsear_pdf_bytes(
    contenido: bytes,
    nombre_archivo: str,
    periodo_anio: int,
    periodo_mes: int,
) -> dict:
    resultado: dict[str, Any] = {
        "archivo": nombre_archivo,
        "hojas":   [nombre_archivo],
        "filas":   [],
        "errores": [],
        "periodo": f"{periodo_anio}-{periodo_mes:02d}",
    }
    try:
        with pdfplumber.open(io.BytesIO(contenido)) as pdf:
            for num_pagina, page in enumerate(pdf.pages, start=1):
                _procesar_pagina(page, num_pagina, nombre_archivo,
                                 periodo_anio, periodo_mes, resultado)
    except Exception as e:
        resultado["errores"].append({
            "hoja": nombre_archivo, "fila": 0, "campo": "archivo",
            "mensaje": f"No se pudo abrir el PDF: {e}",
        })
    return resultado


def _procesar_pagina(page, num_pagina, nombre_archivo, anio, mes, resultado):
    words = page.extract_words()
    if not words:
        return

    meta = _extraer_meta(words)

    # Agrupar por línea
    lineas: dict[int, list] = {}
    for w in words:
        top = round(w["top"] / 4) * 4
        lineas.setdefault(top, []).append(w)

    # Encontrar header → detectar columnas por x0 real
    header_top = None
    col_map    = None  # dict {campo: (x_inicio, x_fin)}

    for top in sorted(lineas.keys()):
        ws = sorted(lineas[top], key=lambda w: w["x0"])
        textos = [w["text"].strip().upper() for w in ws]
        if "ÍTEMS" in textos or "ITEMS" in textos:
            header_top = top
            col_map    = _construir_col_map(ws)
            break

    if header_top is None or not col_map:
        return

    # Procesar filas de datos
    num_fila = 0
    for top in sorted(k for k in lineas.keys() if k > header_top):
        ws = sorted(lineas[top], key=lambda w: w["x0"])
        if not ws:
            continue
        primer = ws[0]["text"].strip()
        if not _es_item_valido(primer):
            continue
        num_fila += 1
        fila, errores = _procesar_fila(
            ws, col_map, nombre_archivo, num_fila, anio, mes, meta
        )
        if fila:
            resultado["filas"].append(fila)
            resultado["errores"].extend(errores)


def _construir_col_map(header_ws: list) -> dict:
    """
    Construye {campo: (x_inicio, x_fin)} usando los x0 reales del header.
    x_fin de cada campo = x0 del siguiente campo detectado.
    """
    # Detectar qué palabras del header corresponden a qué campo
    detectados = []  # lista de (x0, campo)
    for w in sorted(header_ws, key=lambda w: w["x0"]):
        texto = w["text"].strip().upper()
        if texto in HEADER_PALABRAS:
            campo = HEADER_PALABRAS[texto]
            # No duplicar (GASNOR aparece dos veces, NOMBRE/CONTRATO son ruido)
            if not any(c == campo for _, c in detectados):
                detectados.append((w["x0"], campo))

    if not detectados:
        return {}

    # Construir rangos: cada campo va desde su x0 hasta el x0 del siguiente
    col_map = {}
    for i, (x0, campo) in enumerate(detectados):
        x_fin = detectados[i + 1][0] if i + 1 < len(detectados) else 99999
        col_map[campo] = (x0 - 15, x_fin)  # tolerancia 15px

    return col_map


def _get_texto(ws: list, col_map: dict, campo: str) -> str:
    """Extrae y pega palabras de un campo según su rango x."""
    if campo not in col_map:
        return ""
    x_min, x_max = col_map[campo]
    palabras = [w for w in ws if x_min <= w["x0"] < x_max]
    if not palabras:
        return ""
    # item_codigo: tomar solo la primera palabra (el código numérico/alfanumérico)
    if campo == "item_codigo":
        return palabras[0]["text"].strip()
    return _pegar_palabras(palabras)


def _pegar_palabras(ws: list) -> str:
    """
    Junta palabras. Si dos palabras numéricas consecutivas están
    muy cerca (dígito partido), las pega sin espacio.
    """
    if not ws:
        return ""
    resultado = [ws[0]["text"].strip()]
    for i in range(1, len(ws)):
        prev = ws[i - 1]
        curr = ws[i]
        prev_texto = prev["text"].strip()
        curr_texto = curr["text"].strip()
        gap = curr["x0"] - (prev["x0"] + prev.get("width", len(prev_texto) * 4))

        # Pegar si son partes de un número (gap muy pequeño)
        if (gap < 5 and
                re.match(r'^\d+$', prev_texto) and
                re.match(r'^\d', curr_texto)):
            resultado[-1] = resultado[-1] + curr_texto
        else:
            resultado.append(curr_texto)
    return " ".join(resultado).strip()


def _limpiar_num(s: str) -> Optional[str]:
    if not s:
        return None
    # Quitar $ y tomar solo la primera secuencia numérica (con puntos/comas)
    s = re.sub(r"\$", "", s).strip()
    m = re.match(r'^[\d\.,]+', s)
    if not m:
        return None
    s = m.group(0).strip(".,")
    # Formato argentino: puntos de miles + coma decimal → float
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        float(s)
        return s
    except (ValueError, TypeError):
        return None


def _extraer_meta(words: list) -> dict:
    meta = {"k_gasnor": None, "nro_np": None}
    full = " ".join(w["text"] for w in words).upper()
    m = re.search(r'\bK(\d+)\b', full)
    if m:
        meta["k_gasnor"] = "K" + m.group(1)
    return meta


def _procesar_fila(ws, col_map, nombre_archivo, num_fila, anio, mes, meta):
    errores = []

    def get(campo):
        return _get_texto(ws, col_map, campo)

    def num(campo):
        return _limpiar_num(get(campo))

    item_codigo   = get("item_codigo").strip()
    tarea         = get("tarea").strip() or None
    contrato_raw  = get("contrato").strip().upper()
    unidad_medida = get("unidad_medida").strip() or None
    ptos_gasnor   = num("ptos_gasnor")
    tipo          = get("tipo").strip() or None
    contratista   = get("contratista").strip() or None
    provincia_raw = get("provincia").strip()
    cantidades    = num("cantidades")
    precio_unit   = num("precio_unitario")
    total_mes     = num("total_mes")
    observaciones = get("observaciones").strip() or None

    # Normalizar contrato — extraer solo KN
    m = re.match(r'K\d+', contrato_raw)
    contrato = m.group(0) if m else meta.get("k_gasnor", "") or ""

    # Normalizar provincia
    provincia = ""
    prov_key  = provincia_raw.lower().replace("á","a").replace("é","e")
    for key, val in PROVINCIAS.items():
        if key in prov_key:
            provincia = val
            break
    if not provincia and provincia_raw:
        provincia = provincia_raw.title()

    # Limpiar contratista si tiene provincia pegada
    if contratista and provincia:
        contratista = re.sub(re.escape(provincia), "", contratista, flags=re.IGNORECASE).strip()

    tiene_error = False
    if not provincia:
        errores.append({"hoja": nombre_archivo, "fila": num_fila,
                        "campo": "provincia", "mensaje": "Provincia vacía."})
        tiene_error = True
    if not contrato:
        errores.append({"hoja": nombre_archivo, "fila": num_fila,
                        "campo": "contrato", "mensaje": "Contrato K no detectado."})
        tiene_error = True

    return {
        "hoja_origen":     nombre_archivo,
        "archivo_origen":  nombre_archivo,
        "item_codigo":     item_codigo,
        "nombre_contrato": None,
        "tarea":           tarea,
        "contrato":        contrato,
        "unidad_medida":   unidad_medida,
        "ptos_gasnor":     ptos_gasnor,
        "tipo":            tipo,
        "contratista":     contratista,
        "provincia":       provincia,
        "region":          "",
        "cantidades":      cantidades,
        "precio_unitario": precio_unit,
        "total_mes":       total_mes,
        "observaciones":   observaciones,
        "fecha":           f"{anio}-{mes:02d}-01",
        "nro_np":          meta.get("nro_np"),
        "tiene_error":     tiene_error,
    }, errores


def _es_item_valido(s: str) -> bool:
    if not s or s.upper() in ("ÍTEMS", "ITEMS", "ÍTEM", "ITEM", ""):
        return False
    return bool(re.match(r'^[A-Za-z]?\d{2,}', s))