from sqlalchemy import Column, Integer, String, DateTime, Enum, Boolean, Text, DECIMAL, Date, SmallInteger
from sqlalchemy.sql import func
from app.database import Base

class Usuario(Base):
    __tablename__ = "usuarios"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    nombre      = Column(String(100), nullable=False)
    email       = Column(String(100), unique=True, nullable=False)
    password    = Column(String(255), nullable=False, comment="hash bcrypt")
    rol         = Column(Enum("admin", "jefe", "gerente"), nullable=False, default="jefe")
    contratos   = Column(String(50), nullable=True, comment="ej: K2,K6")
    activo      = Column(Boolean, nullable=False, default=True)
    creado_en   = Column(DateTime, server_default=func.now())

    @property
    def contratos_list(self) -> list[str]:
        if not self.contratos:
            return []
        return [k.strip().upper() for k in self.contratos.split(",")]


class CargaLog(Base):
    """Registro de cada archivo cargado — auditoría completa."""
    __tablename__ = "carga_log"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    usuario_id      = Column(Integer, nullable=False)
    usuario_nombre  = Column(String(100), nullable=False)
    archivo_nombre  = Column(String(200), nullable=False)
    contrato        = Column(String(10), nullable=True)
    periodo         = Column(String(10), nullable=True, comment="ej: 2026-03")
    filas_cargadas  = Column(Integer, default=0)
    filas_error     = Column(Integer, default=0)
    estado          = Column(Enum("ok", "parcial", "error"), default="ok")
    detalle_errores = Column(Text, nullable=True)
    cargado_en      = Column(DateTime, server_default=func.now())
