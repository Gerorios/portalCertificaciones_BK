"""
parser_pdf.py
=============
Lee PDFs de certificaciones de Naturgy.
Detecta los límites de columna desde el x0 real de cada palabra del header.
Pega números partidos (problema de pdfplumber con PDFs A3).
Las filas lógicas pueden ocupar varias líneas del PDF (cantidad u
observaciones en línea aparte): toda línea sin código de ítem se fusiona
con la fila del ítem anterior.
"""
import io
import re
from typing import Any, Optional

import pdfplumber

# Palabras del header → nombre canónico del campo
HEADER_PALABRAS = {
    "ÍTEMS":         "item_codigo",
    "ITEMS":         "item_codigo",
    "NOMBRE":        "nombre_contrato",
    "TAREA":         "tarea",
    "K":             "contrato",
    "UM":            "unidad_medida",
    "PTOS.":         "ptos_gasnor",
    "TIPO":          "tipo",
    "CONTRATISTA":   "contratista",
    "PROVINCIA":     "provincia",
    "CANTIDADES":    "cantidades",
    "UNITARIO":      "precio_unitario",
    "TOTAL":         "total_mes",
    "OBSERVACIONES": "observaciones",
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
        "archivo":         nombre_archivo,
        "hojas":           [nombre_archivo],
        "filas":           [],
        "errores":         [],
        "periodo":         f"{periodo_anio}-{periodo_mes:02d}",
        "total_declarado": None,
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


# Al detectar cualquiera de estas palabras, terminó la zona de datos de la página
FOOTER_PALABRAS = ("FIRMA", "ACLARACIÓN", "ACLARACION",
                   "TOTAL A CERTIFICAR", "PERIODO A CERTIFICAR")


def _procesar_pagina(page, num_pagina, nombre_archivo, anio, mes, resultado):
    words = page.extract_words()
    if not words:
        return

    meta = _extraer_meta(words)
    if resultado.get("total_declarado") is None:
        resultado["total_declarado"] = meta.get("total_declarado")

    # Agrupar por línea
    lineas: dict[int, list] = {}
    for w in words:
        top = round(w["top"] / 4) * 4
        lineas.setdefault(top, []).append(w)

    # Encontrar header → detectar columnas por x0 real
    header_top = None
    col_map    = None

    for top in sorted(lineas.keys()):
        ws = sorted(lineas[top], key=lambda w: w["x0"])
        textos = [w["text"].strip().upper() for w in ws]
        if "ÍTEMS" in textos or "ITEMS" in textos:
            header_top = top
            col_map    = _construir_col_map(ws)
            break

    if header_top is None or not col_map:
        return

    # Agrupar líneas en filas lógicas: una línea que empieza con código de
    # ítem abre una fila nueva; las demás son continuación de la anterior
    item_x_max = col_map.get("item_codigo", (0, 384))[1]
    grupos: list[list[list]] = []
    grupo_actual = None

    for top in sorted(k for k in lineas.keys() if k > header_top):
        ws = sorted(lineas[top], key=lambda w: w["x0"])
        if not ws:
            continue

        texto_linea = " ".join(w["text"] for w in ws).upper()
        if any(p in texto_linea for p in FOOTER_PALABRAS):
            break  # empezó el pie de firmas/totales: no hay más datos

        primer  = ws[0]
        es_item = _es_item_valido(primer["text"].strip()) and primer["x0"] < item_x_max

        if es_item:
            grupo_actual = [ws]
            grupos.append(grupo_actual)
        elif grupo_actual is not None:
            grupo_actual.append(ws)
        # líneas antes de la primera fila con ítem: ruido, se ignoran

    for num_fila, grupo in enumerate(grupos, start=1):
        fila, errores = _procesar_fila(
            grupo, col_map, nombre_archivo, num_fila, anio, mes, meta
        )
        if fila:
            resultado["filas"].append(fila)
            resultado["errores"].extend(errores)


def _construir_col_map(header_ws: list) -> dict:
    """
    Construye {campo: (x_inicio, x_fin)} usando los x0 reales del header.
    """
    detectados = []
    for w in sorted(header_ws, key=lambda w: w["x0"]):
        texto = w["text"].strip().upper()
        if texto in HEADER_PALABRAS:
            campo = HEADER_PALABRAS[texto]
            if not any(c == campo for _, c in detectados):
                detectados.append((w["x0"], campo))

    if not detectados:
        return {}

    col_map = {}
    n = len(detectados)
    for i, (x0, campo) in enumerate(detectados):
        # x_fin: punto medio entre este header y el siguiente
        if i + 1 < n:
            x_sig = detectados[i + 1][0]
            x_fin = (x0 + x_sig) / 2 + 5  # punto medio + un poco más para que el dato del campo actual caiga acá
        else:
            x_fin = 99999

        # x_inicio: punto medio entre el header anterior y este
        if i > 0:
            x_prev = detectados[i - 1][0]
            x_ini  = (x_prev + x0) / 2 + 5
        else:
            x_ini  = 0

        col_map[campo] = (x_ini, x_fin)

    return col_map


def _get_texto(ws: list, col_map: dict, campo: str) -> str:
    """Extrae y pega palabras de un campo según su rango x (una línea)."""
    if campo not in col_map:
        return ""
    x_min, x_max = col_map[campo]
    palabras = [w for w in ws if x_min <= w["x0"] < x_max]
    if not palabras:
        return ""
    # item_codigo: solo la primera palabra
    if campo == "item_codigo":
        return palabras[0]["text"].strip()
    return _pegar_palabras(palabras)


def _get_texto_grupo(grupo: list, col_map: dict, campo: str, modo: str) -> str:
    """
    Extrae un campo de una fila lógica multilínea.
    modo "primero": primera línea con valor (campos cortos y numéricos —
                    los valores de líneas siguientes son de otra cosa).
    modo "juntar":  concatena todas las líneas (textos largos que el PDF
                    parte: tarea, nombre de contrato, observaciones).
    """
    chunks = [t for ws in grupo if (t := _get_texto(ws, col_map, campo))]
    if not chunks:
        return ""
    if modo == "juntar":
        return " ".join(chunks)
    return chunks[0]


def _get_num_grupo(grupo: list, col_map: dict, campo: str) -> Optional[str]:
    """Primer valor numérico válido del campo en las líneas del grupo."""
    for ws in grupo:
        n = _limpiar_num(_get_texto(ws, col_map, campo))
        if n is not None:
            return n
    return None


def _pegar_palabras(ws: list) -> str:
    """
    Junta palabras pegando dígitos partidos por pdfplumber.
    Ej: '9' + '.338,22' → '9.338,22'  |  '8' + '3.006,40' → '83.006,40'
    """
    if not ws:
        return ""
    resultado = [ws[0]["text"].strip()]
    for i in range(1, len(ws)):
        prev      = ws[i - 1]
        curr      = ws[i]
        prev_txt  = prev["text"].strip()
        curr_txt  = curr["text"].strip()
        gap       = curr["x0"] - (prev["x0"] + prev.get("width", len(prev_txt) * 4))

        # Pegar si gap muy pequeño y parecen partes de un número
        es_numero_partido = (
            gap < 8 and
            re.match(r'^\d+$', prev_txt) and
            re.match(r'^[\d\.,]', curr_txt)
        )
        if es_numero_partido:
            resultado[-1] = resultado[-1] + curr_txt
        else:
            resultado.append(curr_txt)
    return " ".join(resultado).strip()


def _limpiar_num(s: str) -> Optional[str]:
    if not s:
        return None
    # Quitar $ y tomar solo la primera secuencia numérica
    s = s.replace("$", "").strip()
    m = re.match(r'^[\d\.,]+', s)
    if not m:
        return None
    s = m.group(0).strip(".,")
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
    meta = {"k_gasnor": None, "nro_np": None, "total_declarado": None}
    full = " ".join(w["text"] for w in words).upper()
    m = re.search(r'\bK(\d+)\b', full)
    if m:
        meta["k_gasnor"] = "K" + m.group(1)
    # El monto puede venir partido en varias palabras ("3 9.072.433,92")
    m = re.search(r'TOTAL MES\s*\$?\s*((?:[\d\.,]+\s*)+)', full)
    if m:
        n = _limpiar_num(m.group(1).replace(" ", ""))
        if n is not None:
            meta["total_declarado"] = float(n)
    return meta


def _procesar_fila(grupo, col_map, nombre_archivo, num_fila, anio, mes, meta):
    """grupo: lista de líneas (cada una, lista de words) de una fila lógica."""
    errores = []

    def get(campo, modo="primero"):
        return _get_texto_grupo(grupo, col_map, campo, modo)

    def num(campo):
        return _get_num_grupo(grupo, col_map, campo)

    item_codigo   = get("item_codigo").strip()
    tarea         = get("tarea", "juntar").strip() or None
    contrato_raw  = get("contrato").strip().upper()
    unidad_medida = get("unidad_medida").strip() or None
    ptos_gasnor   = num("ptos_gasnor")
    tipo          = get("tipo").strip() or None
    contratista   = get("contratista").strip() or None
    provincia_raw = get("provincia").strip()
    cantidades    = num("cantidades")
    precio_unit   = num("precio_unitario")
    total_mes     = num("total_mes")
    observaciones = get("observaciones", "juntar").strip() or None

    # Normalizar contrato
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
    return bool(re.match(r'^[A-Za-z]?\d{3,}', s))