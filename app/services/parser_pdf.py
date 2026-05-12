"""
parser_pdf.py
=============
Lee PDFs de certificaciones de Naturgy y devuelve el mismo
formato de filas que parser.py (Excel), para que el resto
del sistema los trate de forma idéntica.

Estructura del PDF:
- Tabla 1: encabezado administrativo (contratista, fechas, etc.)
- Tabla 2: totales OPEX/CAPEX
- Tabla 3: filas de ítems certificados (la que nos interesa)
"""
import io
import re
import math
from typing import Any

import pdfplumber

# Índices de columnas en la Tabla 3
COL_ITEM        = 0
COL_TAREA       = 1
COL_K           = 2
COL_UM          = 3
COL_PTOS        = 4
COL_TIPO        = 5
COL_CONTRATISTA = 6
COL_PROVINCIA   = 7
COL_CANTIDADES  = 8
COL_UNITARIO    = 9
COL_TOTAL       = 10
COL_OBS         = 11


def parsear_pdf_bytes(
    contenido: bytes,
    nombre_archivo: str,
    periodo_anio: int,
    periodo_mes: int,
) -> dict:
    """
    Parsea un PDF de certificación de Naturgy.
    Devuelve el mismo formato que parser.parsear_bytes().
    """
    resultado: dict[str, Any] = {
        "archivo":  nombre_archivo,
        "hojas":    [nombre_archivo],   # un PDF = una "hoja"
        "filas":    [],
        "errores":  [],
        "periodo":  f"{periodo_anio}-{periodo_mes:02d}",
    }

    try:
        with pdfplumber.open(io.BytesIO(contenido)) as pdf:
            for num_pagina, page in enumerate(pdf.pages, start=1):
                tablas = page.extract_tables()
                _procesar_pagina(
                    tablas, num_pagina, nombre_archivo,
                    periodo_anio, periodo_mes, resultado
                )
    except Exception as e:
        resultado["errores"].append({
            "hoja": nombre_archivo, "fila": 0, "campo": "archivo",
            "mensaje": f"No se pudo abrir el PDF: {e}",
        })

    return resultado


def _procesar_pagina(tablas, num_pagina, nombre_archivo, anio, mes, resultado):
    """Procesa las tablas de una página del PDF."""

    # La tabla de ítems es la Tabla 3 (índice 2) — la que tiene más columnas
    tabla_datos = None
    for tabla in tablas:
        if tabla and len(tabla) > 1 and len(tabla[0]) >= 10:
            # Verificar que tenga el header de ítems
            header = [str(c or "").strip().upper() for c in tabla[1]]
            if "ÍTEMS" in header or "ITEMS" in header:
                tabla_datos = tabla
                break
        # También buscar por la fila con ÍTEMS como primer valor
        if tabla:
            for fila in tabla[:3]:
                if fila and str(fila[0] or "").strip().upper() in ("ÍTEMS", "ITEMS"):
                    tabla_datos = tabla
                    break

    if not tabla_datos:
        # Intentar con la tabla de más columnas
        if tablas:
            tabla_datos = max(tablas, key=lambda t: len(t[0]) if t else 0)
            if len(tabla_datos[0]) < 10:
                resultado["errores"].append({
                    "hoja": nombre_archivo, "fila": num_pagina,
                    "campo": "tabla", "mensaje": f"No se encontró tabla de ítems en página {num_pagina}",
                })
                return

    # Encontrar la fila del header (la que tiene ÍTEMS)
    header_idx = None
    for i, fila in enumerate(tabla_datos):
        if fila and str(fila[0] or "").strip().upper() in ("ÍTEMS", "ITEMS"):
            header_idx = i
            break

    if header_idx is None:
        resultado["errores"].append({
            "hoja": nombre_archivo, "fila": num_pagina,
            "campo": "header", "mensaje": "No se encontró el header ÍTEMS en la tabla",
        })
        return

    # Procesar filas de datos (después del header)
    for i, fila in enumerate(tabla_datos[header_idx + 1:], start=header_idx + 2):
        if not fila or not fila[COL_ITEM]:
            continue
        item_val = str(fila[COL_ITEM] or "").strip()
        if not item_val or item_val.upper() in ("ÍTEMS", "ITEMS", ""):
            continue

        fila_proc, errores = _procesar_fila(
            fila, nombre_archivo, i, anio, mes
        )
        if fila_proc:
            resultado["filas"].append(fila_proc)
            resultado["errores"].extend(errores)


def _procesar_fila(fila, nombre_archivo, num_fila, anio, mes):
    """Convierte una fila cruda del PDF en un dict limpio."""
    errores = []

    def limpiar_str(v):
        if v is None: return None
        s = str(v).strip()
        return s if s and s.upper() not in ("NONE", "NULL", "") else None

    def limpiar_num(v):
        """Limpia números con formato argentino y espacios en medio."""
        if v is None: return None
        s = str(v).strip()
        # Quitar $ y espacios
        s = re.sub(r"[\$\s]", "", s)
        # Quitar puntos de miles, convertir coma decimal a punto
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            float(s)
            return s
        except (ValueError, TypeError):
            return None

    def limpiar_item(v):
        if v is None: return ""
        s = str(v).strip()
        # Puede ser alfanumérico: 106-a, 116-a, D858
        return s

    # Extraer valores — col 6 es contratista, col 7 es provincia (vienen fusionadas a veces)
    item_codigo    = limpiar_item(fila[COL_ITEM]        if len(fila) > COL_ITEM        else None)
    tarea          = limpiar_str(fila[COL_TAREA]        if len(fila) > COL_TAREA       else None)
    contrato       = limpiar_str(fila[COL_K]            if len(fila) > COL_K           else None)
    unidad_medida  = limpiar_str(fila[COL_UM]           if len(fila) > COL_UM          else None)
    ptos_gasnor    = limpiar_num(fila[COL_PTOS]         if len(fila) > COL_PTOS        else None)
    tipo           = limpiar_str(fila[COL_TIPO]         if len(fila) > COL_TIPO        else None)
    contratista    = limpiar_str(fila[COL_CONTRATISTA]  if len(fila) > COL_CONTRATISTA else None)
    provincia      = limpiar_str(fila[COL_PROVINCIA]    if len(fila) > COL_PROVINCIA   else None)
    cantidades     = limpiar_num(fila[COL_CANTIDADES]   if len(fila) > COL_CANTIDADES  else None)
    precio_unit    = limpiar_num(fila[COL_UNITARIO]     if len(fila) > COL_UNITARIO    else None)
    total_mes      = limpiar_num(fila[COL_TOTAL]        if len(fila) > COL_TOTAL       else None)
    observaciones  = limpiar_str(fila[COL_OBS]          if len(fila) > COL_OBS         else None)

    # Normalizar contrato
    if contrato:
        contrato = contrato.strip().upper()
        if not contrato.startswith("K"):
            contrato = "K" + contrato.lstrip("kK")

    # Normalizar provincia
    if provincia:
        provincia = provincia.strip().title()

    # Si CONTRATISTA y PROVINCIA vienen en la misma celda (col 6)
    # pdfplumber a veces los fusiona: "SER&TEC Salta"
    if contratista and not provincia:
        partes = contratista.rsplit(" ", 1)
        if len(partes) == 2:
            contratista = partes[0].strip()
            provincia   = partes[1].strip().title()

    # Validaciones
    tiene_error = False

    if not item_codigo:
        return None, []  # fila vacía, ignorar

    if not provincia:
        errores.append({"hoja": nombre_archivo, "fila": num_fila,
                         "campo": "provincia", "mensaje": "Provincia vacía."})
        tiene_error = True

    if not contrato:
        errores.append({"hoja": nombre_archivo, "fila": num_fila,
                         "campo": "contrato", "mensaje": "Contrato K no detectado."})
        tiene_error = True

    if cantidades and float(cantidades) == 0:
        errores.append({"hoja": nombre_archivo, "fila": num_fila,
                         "campo": "cantidades", "mensaje": "Cantidad es 0."})

    fila_limpia = {
        "hoja_origen":     nombre_archivo,
        "archivo_origen":  nombre_archivo,
        "item_codigo":     item_codigo,
        "nombre_contrato": None,
        "tarea":           tarea,
        "contrato":        contrato or "",
        "unidad_medida":   unidad_medida,
        "ptos_gasnor":     ptos_gasnor,
        "tipo":            tipo,
        "contratista":     contratista,
        "provincia":       provincia or "",
        "region":          "",
        "cantidades":      cantidades,
        "precio_unitario": precio_unit,
        "total_mes":       total_mes,
        "observaciones":   observaciones,
        "fecha":           f"{anio}-{mes:02d}-01",
        "nro_np":          None,
        "tiene_error":     tiene_error,
    }
    return fila_limpia, errores