from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    db_host: str
    db_port: int = 3306
    db_name: str
    db_user: str
    db_password: str
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 480
    allowed_origins: str = "http://localhost:5500"
    # Secret HS256 del sistema de Horas (login unificado) — token con claims
    # cuil/email/rol/cert. Vacío por defecto: sin valor, no se acepta ese token.
    horas_jwt_secret: str = ""

    @property
    def database_url(self) -> str:
        return (
            f"mysql+pymysql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}?charset=utf8mb4"
        )

    @property
    def origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",")]

    class Config:
        env_file = ".env"
        # El .env también guarda credenciales de OneDrive/Azure que lee
        # onedrive.py por os.environ — no deben romper la carga de Settings
        extra = "ignore"

@lru_cache
def get_settings() -> Settings:
    return Settings()
