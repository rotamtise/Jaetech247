"""
app/core/config.py
Application configuration via pydantic-settings
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path
from functools import lru_cache
import secrets


class Settings(BaseSettings):
    # App
    APP_NAME: str = "JaeTech247 AutoTrading Platform"
    APP_ENV: str = "production"
    DEBUG: bool = False
    BASE_URL: str = "https://jaetech247.pro"

    # Security
    SECRET_KEY: str = Field(default_factory=lambda: secrets.token_hex(32))
    AES_KEY: str = "jaetech247_aes_key_32bytes_pad!!"  # must be 32 chars
    AES_IV: str = "jaetech247_iv16!"                    # must be 16 chars
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 480
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # Database
    DATABASE_URL: str = "sqlite+aiosqlite:///./jaetech247.db"

    # Upload
    UPLOAD_DIR: Path = Path("./uploads")
    MAX_UPLOAD_SIZE_MB: int = 10

    # Trading config
    TOTAL_CHANNELS: int = 35
    PREMIUM_CH_START: int = 1
    PREMIUM_CH_END: int = 6
    CRYPTO_CH_START: int = 7
    CRYPTO_CH_END: int = 30
    STOCK_CH_START: int = 31
    STOCK_CH_END: int = 35
    CHANNEL_CYCLE_SECONDS: int = 6

    # Grace & Cleanup
    GRACE_PERIOD_HOURS: int = 24
    UNVERIFIED_ACCOUNT_TTL_DAYS: int = 7

    # External APIs
    BITHUMB_API_URL: str = "https://api.bithumb.com"
    KIS_API_URL: str = "https://openapi.koreainvestment.com:9443"

    # Admin init
    ADMIN_ID: str = "admin"
    ADMIN_PASSWORD: str = "JaeTech247Admin!!"

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
