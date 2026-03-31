# app/Astrology/config.py
from pydantic_settings import BaseSettings
import os


class Settings(BaseSettings):
    VEDIC_API_KEY:  str = os.getenv("VEDIC_API_KEY", "")
    VEDIC_BASE_URL: str = os.getenv("VEDIC_BASE_URL", "https://json.vedicastroapi.com/v3-json")
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    REDIS_URL:      str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    CACHE_TTL:      int = int(os.getenv("CACHE_TTL", "3600"))
    DRISHTII_HTTP_TIMEOUT: int = int(os.getenv("DRISHTII_HTTP_TIMEOUT", "20"))

    class Config:
        env_file    = ".env"
        case_sensitive = True


settings = Settings()