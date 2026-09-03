"""
app/core/security.py
Password hashing, JWT tokens, AES bidirectional encryption for API keys
"""
import base64
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import settings

# ── Password Hashing ──────────────────────────────────────────────────────────
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(plain: str) -> str:
    import hashlib
    return hashlib.sha256(plain.encode()).hexdigest()

def verify_password(plain: str, hashed: str) -> bool:
    import hashlib
    return hashlib.sha256(plain.encode()).hexdigest() == hashed


# ── JWT ───────────────────────────────────────────────────────────────────────
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError:
        return None


# ── AES-256-CBC Bidirectional Encryption (for API keys) ──────────────────────
def _get_aes_key_iv() -> tuple[bytes, bytes]:
    key = settings.AES_KEY.encode("utf-8")[:32].ljust(32, b"\x00")
    iv = settings.AES_IV.encode("utf-8")[:16].ljust(16, b"\x00")
    return key, iv


def _pad(data: bytes) -> bytes:
    """PKCS7 padding to 16-byte blocks"""
    pad_len = 16 - (len(data) % 16)
    return data + bytes([pad_len] * pad_len)


def _unpad(data: bytes) -> bytes:
    """Remove PKCS7 padding"""
    pad_len = data[-1]
    return data[:-pad_len]


def encrypt_api_key(plaintext: str) -> str:
    """Encrypt API key/secret for DB storage. Returns base64 string."""
    if not plaintext:
        return ""
    key, iv = _get_aes_key_iv()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    enc = cipher.encryptor()
    padded = _pad(plaintext.encode("utf-8"))
    ct = enc.update(padded) + enc.finalize()
    return base64.b64encode(ct).decode("utf-8")


def decrypt_api_key(ciphertext_b64: str) -> str:
    """Decrypt API key/secret from DB. Returns plaintext string."""
    if not ciphertext_b64:
        return ""
    key, iv = _get_aes_key_iv()
    ct = base64.b64decode(ciphertext_b64)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    dec = cipher.decryptor()
    padded = dec.update(ct) + dec.finalize()
    return _unpad(padded).decode("utf-8")
