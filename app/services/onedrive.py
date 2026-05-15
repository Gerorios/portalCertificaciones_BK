"""
onedrive.py
===========
Sube archivos al OneDrive de la empresa usando Microsoft Graph API.
Crea automáticamente la estructura de carpetas:
    Certificaciones / K8 / 2026-05 / archivo.xlsx
"""
import requests
from typing import Optional
import os

TENANT_ID     = os.environ.get("AZURE_TENANT_ID", "")
CLIENT_ID     = os.environ.get("AZURE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "")
ONEDRIVE_USER = os.environ.get("ONEDRIVE_USER", "")
CARPETA_RAIZ  = "Certificaciones"

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_URL = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_USER}/drive"


def _get_token() -> Optional[str]:
    """Obtiene un token de acceso de Microsoft Graph."""
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://graph.microsoft.com/.default",
    }, timeout=10)
    if resp.status_code == 200:
        return resp.json().get("access_token")
    print(f"Error obteniendo token OneDrive: {resp.status_code} {resp.text}")
    return None


def _crear_carpeta_si_no_existe(token: str, ruta_padre: str, nombre: str) -> Optional[str]:
    """
    Crea una carpeta dentro de ruta_padre si no existe.
    ruta_padre: ruta relativa desde la raíz del drive, ej: 'Certificaciones/K8'
    Retorna el ID de la carpeta.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Verificar si ya existe
    check = requests.get(
        f"{GRAPH_URL}/root:/{ruta_padre}/{nombre}",
        headers=headers, timeout=10
    )
    if check.status_code == 200:
        return check.json().get("id")

    # Crear la carpeta
    parent_url = f"{GRAPH_URL}/root:/{ruta_padre}:/children" if ruta_padre else f"{GRAPH_URL}/root/children"
    resp = requests.post(parent_url, headers=headers, json={
        "name":   nombre,
        "folder": {},
        "@microsoft.graph.conflictBehavior": "rename",
    }, timeout=10)

    if resp.status_code in (200, 201):
        return resp.json().get("id")

    print(f"Error creando carpeta '{nombre}': {resp.status_code} {resp.text}")
    return None


def subir_certificacion(
    contenido: bytes,
    nombre_archivo: str,
    contrato: str,
    periodo: str,
) -> Optional[str]:
    """
    Sube un archivo de certificación a OneDrive.

    Estructura:
        Certificaciones / K8 / 2026-05 / CERTIFICADOS_K8_MAYO.xlsx

    Retorna la URL del archivo subido o None si falló.
    """
    try:
        token = _get_token()
        if not token:
            return None

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/octet-stream",
        }

        # Asegurar que las carpetas existen
        _crear_carpeta_si_no_existe(token, "",             CARPETA_RAIZ)
        _crear_carpeta_si_no_existe(token, CARPETA_RAIZ,   contrato)
        _crear_carpeta_si_no_existe(token, f"{CARPETA_RAIZ}/{contrato}", periodo)

        # Ruta completa del archivo
        ruta = f"{CARPETA_RAIZ}/{contrato}/{periodo}/{nombre_archivo}"

        # Subir archivo (hasta 4MB con upload simple)
        if len(contenido) <= 4 * 1024 * 1024:
            resp = requests.put(
                f"{GRAPH_URL}/root:/{ruta}:/content",
                headers=headers,
                data=contenido,
                timeout=30,
            )
        else:
            # Upload en sesión para archivos grandes
            resp = _upload_grande(token, ruta, contenido)
            return resp

        if resp.status_code in (200, 201):
            web_url = resp.json().get("webUrl")
            print(f"✓ Archivo subido a OneDrive: {ruta}")
            return web_url
        else:
            print(f"Error subiendo archivo: {resp.status_code} {resp.text}")
            return None

    except Exception as e:
        print(f"Error en subida a OneDrive: {e}")
        return None


def _upload_grande(token: str, ruta: str, contenido: bytes) -> Optional[str]:
    """Upload en sesión para archivos mayores a 4MB."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Crear sesión de upload
    session = requests.post(
        f"{GRAPH_URL}/root:/{ruta}:/createUploadSession",
        headers=headers,
        json={"item": {"@microsoft.graph.conflictBehavior": "rename"}},
        timeout=10,
    )
    if session.status_code != 200:
        return None

    upload_url = session.json().get("uploadUrl")
    chunk_size  = 3 * 1024 * 1024  # 3MB por chunk
    total       = len(contenido)
    offset      = 0
    web_url     = None

    while offset < total:
        chunk = contenido[offset: offset + chunk_size]
        end   = offset + len(chunk) - 1
        resp  = requests.put(
            upload_url,
            headers={"Content-Range": f"bytes {offset}-{end}/{total}"},
            data=chunk,
            timeout=60,
        )
        if resp.status_code in (200, 201):
            web_url = resp.json().get("webUrl")
        offset += len(chunk)

    return web_url