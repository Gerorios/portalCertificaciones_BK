"""
Tests del mensaje de archivo duplicado según rol (backlog CONTEXTO §13):
solo el admin puede eliminar cargas del historial, así que a los demás
roles hay que decirles que se lo pidan a un admin, no sugerirles una
acción que no pueden hacer.
"""
from app.routers.certificaciones import mensaje_archivo_duplicado


def test_admin_puede_eliminar_del_historial():
    msg = mensaje_archivo_duplicado("cert_K9.xlsx", rol="admin")
    assert "cert_K9.xlsx" in msg
    assert "eliminá la carga anterior desde el historial" in msg


def test_jefe_debe_pedirselo_a_un_admin():
    msg = mensaje_archivo_duplicado("cert_K9.xlsx", rol="jefe")
    assert "cert_K9.xlsx" in msg
    assert "administrador" in msg
    assert "eliminá" not in msg


def test_gerente_debe_pedirselo_a_un_admin():
    msg = mensaje_archivo_duplicado("cert_K9.xlsx", rol="gerente")
    assert "administrador" in msg
