"""
parser_pdf_ia.py
================
Extrae datos de certificaciones desde PDFs usando GPT-4o vision.
Convierte cada página a imagen y le pide al modelo que extraiga
los ítems en formato JSON estructurado.

Ventaja: funciona con cualquier formato de PDF sin calibración.
"""
import io
import os
import re
import json
import base64
from typing import Any

from openai import OpenAI

# pdf2image convierte páginas PDF a imágenes PIL
try:
    from pdf2image import convert_from_bytes
    PDF2IMAGE_OK = True
except ImportError:
    PDF2IMAGE_OK = False


PROMPT_EXTRACCION = """
Sos un asistente especializado en leer certificaciones de gas de Naturgy Argentina.

Analizá esta imagen de una certificación y extraé TODOS los ítems de la tabla principal.
La tabla tiene columnas: ÍTEMS, TAREA, K GASNOR, UM, PTOS. GASNOR, TIPO, CONTRATISTA, PROVINCIA, Cantidades, $ Unitario mes, $ Total mes, Observaciones.

Devolvé ÚNICAMENTE un JSON válido con esta estructura, sin texto adicional, sin markdown:
{
  "contrato": "K9",
  "items": [
    {
      "item_codigo": "693",
      "tarea": "Construcción de cámara de 6 a 8",
      "contrato": "K9",
      "unidad_medida": "N°",
      "ptos_gasnor": "7500",
      "tipo": "CAPEX",
      "contratista": "SER&TEC",
      "provincia": "Tucumán",
      "cantidades": "1",
      "precio_unitario": "5187900",
      "total_mes": "5187900",
      "observaciones": "Construcción de camara para inyección"
    }
  ]
}

Reglas importantes:
- Incluí TODOS los ítems de la tabla, incluso los repetidos
- Los números deben ser solo dígitos y punto decimal (sin $, sin comas de miles)
- Si un campo está vacío, poné null
- El campo "contrato" debe ser el código K (ej: "K9", "K2")
- La provincia debe ser el nombre completo (ej: "Tucumán", "Salta", "Jujuy")
- Si cantidades es 0 o vacío, igual incluí el ítem con cantidades: "0"
- NO incluyas filas de totales ni encabezados, solo filas de ítems reales
"""


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

    if not PDF2IMAGE_OK:
        resultado["errores"].append({
            "hoja": nombre_archivo, "fila": 0, "campo": "archivo",
            "mensaje": "pdf2image no está instalado. Agregá 'pdf2image' al requirements.txt",
        })
        return resultado

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        resultado["errores"].append({
            "hoja": nombre_archivo, "fila": 0, "campo": "archivo",
            "mensaje": "OPENAI_API_KEY no configurada",
        })
        return resultado

    try:
        # Convertir PDF a imágenes (una por página)
        paginas = convert_from_bytes(contenido, dpi=150)
    except Exception as e:
        resultado["errores"].append({
            "hoja": nombre_archivo, "fila": 0, "campo": "archivo",
            "mensaje": f"No se pudo convertir el PDF a imagen: {e}",
        })
        return resultado

    client = OpenAI(api_key=api_key)

    num_fila_global = 0
    for num_pagina, pagina in enumerate(paginas, start=1):
        try:
            filas, errores = _procesar_pagina_ia(
                client, pagina, nombre_archivo,
                num_pagina, num_fila_global,
                periodo_anio, periodo_mes
            )
            resultado["filas"].extend(filas)
            resultado["errores"].extend(errores)
            num_fila_global += len(filas)
        except Exception as e:
            resultado["errores"].append({
                "hoja": nombre_archivo, "fila": num_pagina, "campo": "pagina",
                "mensaje": f"Error procesando página {num_pagina}: {e}",
            })

    return resultado


def _procesar_pagina_ia(client, pagina, nombre_archivo, num_pagina, offset, anio, mes):
    """Manda una página (imagen PIL) a GPT-4o y parsea la respuesta."""
    filas   = []
    errores = []

    # Convertir imagen PIL a base64
    buf = io.BytesIO()
    pagina.save(buf, format="PNG")
    img_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

    # Llamar a GPT-4o
    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=4000,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{img_b64}",
                            "detail": "high",
                        },
                    },
                    {
                        "type": "text",
                        "text": PROMPT_EXTRACCION,
                    },
                ],
            }
        ],
    )

    texto = response.choices[0].message.content.strip()

    # Limpiar markdown si viene con ```json
    texto = re.sub(r"^```json\s*", "", texto)
    texto = re.sub(r"\s*```$", "", texto)
    texto = texto.strip()

    try:
        data = json.loads(texto)
    except json.JSONDecodeError as e:
        errores.append({
            "hoja": nombre_archivo, "fila": num_pagina, "campo": "json",
            "mensaje": f"GPT-4o devolvió JSON inválido: {e}. Respuesta: {texto[:200]}",
        })
        return filas, errores

    contrato_default = (data.get("contrato") or "").strip().upper()
    if contrato_default and not contrato_default.startswith("K"):
        contrato_default = "K" + contrato_default.lstrip("kK")

    for i, item in enumerate(data.get("items", []), start=1):
        num_fila = offset + i
        fila, errs = _normalizar_item(
            item, contrato_default, nombre_archivo, num_fila, anio, mes
        )
        if fila:
            filas.append(fila)
            errores.extend(errs)

    return filas, errores


def _normalizar_item(item: dict, contrato_default: str, nombre_archivo, num_fila, anio, mes):
    errores = []

    def s(campo):
        v = item.get(campo)
        if v is None or str(v).strip().lower() in ("null", "none", ""):
            return None
        return str(v).strip()

    def n(campo):
        v = s(campo)
        if v is None:
            return None
        # Limpiar formato numérico
        v = re.sub(r"[\$\s,]", "", v)
        v = re.sub(r"(\d)\s+(\d)", r"\1\2", v)
        if "," in v and "." in v:
            v = v.replace(".", "").replace(",", ".")
        elif "," in v:
            v = v.replace(",", ".")
        try:
            float(v)
            return v
        except (ValueError, TypeError):
            return None

    item_codigo   = s("item_codigo") or ""
    tarea         = s("tarea")
    contrato      = (s("contrato") or contrato_default or "").upper()
    unidad_medida = s("unidad_medida")
    ptos_gasnor   = n("ptos_gasnor")
    tipo          = s("tipo")
    contratista   = s("contratista")
    provincia     = (s("provincia") or "").title()
    cantidades    = n("cantidades")
    precio_unit   = n("precio_unitario")
    total_mes     = n("total_mes")
    observaciones = s("observaciones")

    if not contrato.startswith("K") and contrato:
        contrato = "K" + contrato.lstrip("kK")

    tiene_error = False
    if not item_codigo:
        return None, []
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
        "nro_np":          None,
        "tiene_error":     tiene_error,
    }, errores