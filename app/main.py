from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import Base, engine, check_connection
from app.routers import auth, certificaciones, admin, items, analytics



settings = get_settings()

app = FastAPI(
    title="Serytec — Sistema de certificaciones",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,
)

# Orígenes de la config (.env / ALLOWED_ORIGINS) + los del módulo Certificaciones
# dentro de la app unificada de Horas (Etapa 1 de la unificación ERP): el dominio
# de producción de esa app y el dev server de Next. No reemplazan lo que ya
# permite ALLOWED_ORIGINS, se suman.
_erp_horas_origins = [
    "https://misregistros.serytec.com.ar",
    "http://localhost:3000",
]
allow_origins = list(dict.fromkeys(settings.origins_list + _erp_horas_origins))

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(certificaciones.router)
app.include_router(admin.router)
app.include_router(items.router)
app.include_router(analytics.router)


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
    if check_connection():
        print("✓ Conexión a la base de datos OK")
    else:
        print("✗ ERROR: no se pudo conectar a la base de datos")


@app.get("/health")
@app.head("/health")
def health():
    return {"status": "ok", "db": check_connection()}
