from jose import jwt
import pytest
from fastapi import HTTPException
from app.services import auth as auth_mod

SECRET_HORAS = "secret-horas-test"

def token_horas(cert):
    return jwt.encode(
        {"cuil": "20-1", "email": "jefe@serytec.com", "rol": "JefeContrato", "cert": cert},
        SECRET_HORAS, algorithm="HS256",
    )

@pytest.fixture(autouse=True)
def _settings(monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "horas_jwt_secret", SECRET_HORAS, raising=False)

def test_token_horas_nivel_carga_mapea_a_jefe():
    p = auth_mod.principal_desde_token_horas(
        jwt.decode(token_horas({"nivel": "carga", "ks": ["K6"], "inc": True}),
                   SECRET_HORAS, algorithms=["HS256"]))
    assert p.rol == "jefe" and p.contratos_list == ["K6"] and p.ver_incidencia is True
    assert p.email == "jefe@serytec.com"

def test_nivel_lectura_mapea_a_gerente_y_admin_a_admin():
    g = auth_mod.principal_desde_token_horas({"cuil": "1", "email": "g@s", "rol": "Admin",
                                              "cert": {"nivel": "lectura", "ks": [], "inc": False}})
    a = auth_mod.principal_desde_token_horas({"cuil": "1", "email": "a@s", "rol": "Admin",
                                              "cert": {"nivel": "admin", "ks": [], "inc": True}})
    assert g.rol == "gerente" and a.rol == "admin"

def test_sin_claim_cert_es_403():
    with pytest.raises(HTTPException) as e:
        auth_mod.principal_desde_token_horas({"cuil": "1", "email": "x@s", "rol": "Supervisor", "cert": None})
    assert e.value.status_code == 403

def test_decode_any_prueba_portal_y_luego_horas():
    payload = auth_mod.decode_any_token(token_horas({"nivel": "admin", "ks": [], "inc": True}))
    assert payload["cuil"] == "20-1"

def test_horas_jwt_secret_vacio_rechaza_token_forjado(monkeypatch):
    """Con HORAS_JWT_SECRET sin configurar, un token firmado con clave vacía
    (forjable por cualquiera) NO debe ser aceptado, aunque declare cert admin."""
    monkeypatch.setattr(auth_mod.settings, "horas_jwt_secret", "", raising=False)
    token_forjado = jwt.encode(
        {"cuil": "1", "email": "atacante@x.com", "rol": "Admin",
         "cert": {"nivel": "admin", "ks": [], "inc": True}},
        "", algorithm="HS256",
    )
    with pytest.raises(HTTPException) as e:
        auth_mod.decode_any_token(token_forjado)
    assert e.value.status_code == 401

def test_check_contrato_access_sigue_estricto_para_gerente_sin_contratos():
    """check_contrato_access no cambia: un gerente sin el contrato en su lista
    sigue recibiendo 403 (la excepción para /detalle vive en el endpoint,
    que salta este check cuando current.rol == 'gerente', no acá)."""
    gerente = auth_mod.PrincipalHoras(nombre="g", rol="gerente", contratos_list=[])
    with pytest.raises(HTTPException) as e:
        auth_mod.check_contrato_access(gerente, "K6")
    assert e.value.status_code == 403

def test_decode_any_con_ambos_secrets_configurados_rechaza_tercer_secret():
    """Con secret del portal y de Horas configurados, un token firmado con un
    tercer secret desconocido debe ser rechazado (no falsear ninguno de los dos)."""
    token_otro = jwt.encode(
        {"cuil": "1", "email": "x@x.com", "rol": "Admin",
         "cert": {"nivel": "admin", "ks": [], "inc": True}},
        "otro-secret-desconocido", algorithm="HS256",
    )
    with pytest.raises(HTTPException) as e:
        auth_mod.decode_any_token(token_otro)
    assert e.value.status_code == 401
