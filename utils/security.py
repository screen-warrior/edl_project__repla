from __future__ import annotations

import secrets
from types import SimpleNamespace

# Passlib's bcrypt backend expects bcrypt._bcrypt.about on some platforms. bcrypt 5.x
# dropped that attribute, so we recreate a minimal shim before importing passlib.
try:  # pragma: no cover - defensive patch for Windows/py3.12 envs
    import bcrypt  # type: ignore[import-not-found]

    backend = getattr(bcrypt, "_bcrypt", None)
    if backend is not None and not hasattr(backend, "about"):
        version = getattr(bcrypt, "__version__", "unknown")
        backend.about = SimpleNamespace(version=version)
except Exception:  # fall back silently if bcrypt is missing
    bcrypt = None  # type: ignore[assignment]

from passlib.context import CryptContext

# Use PBKDF2-SHA256 to avoid bcrypt length limitations and backend quirks.
_pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def generate_api_key(length: int = 32) -> str:
    """Generate a URL-safe random API key."""
    return secrets.token_urlsafe(length)


def hash_token(token: str) -> str:
    return _pwd_context.hash(token)


def verify_token(token: str, token_hash: str) -> bool:
    try:
        return _pwd_context.verify(token, token_hash)
    except ValueError:
        return False
