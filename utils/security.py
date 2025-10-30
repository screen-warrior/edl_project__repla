from __future__ import annotations

import secrets
from passlib.context import CryptContext

# Use PBKDF2-SHA256. It has no 72-byte limit and avoids bcrypt backend quirks.
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
