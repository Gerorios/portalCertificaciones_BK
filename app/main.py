from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import Base, engine, check_connection
from app.routers import auth, certificaciones, admin

settings = get_settings()

app = FastAPI(
    title="Serytec — Sistema de certificaciones",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(certificaciones.router)
app.include_router(admin.router)


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
    if check_connection():
        print("✓ Conexión a la base de datos OK")
    else:
        print("✗ ERROR: no se pudo conectar a la base de datos")


@app.get("/health")
def health():
    return {"status": "ok", "db": check_connection()}
