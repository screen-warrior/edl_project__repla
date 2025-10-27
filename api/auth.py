from __future__ import annotations

import os
from typing import Optional, Set

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from .schemas import APIKeySettings
from pydantic import BaseModel

_settings = APIKeySettings()
_api_key_header = APIKeyHeader(name=_settings.header_name, auto_error=False)


class AuthContext(BaseModel):
    api_key: str
    role: str  # "reader" or "operator"


def _load_keys(settings: APIKeySettings) -> tuple[Set[str], Set[str]]:
    operator_keys = set(filter(None, os.getenv("EDL_OPERATOR_KEYS", "").split(",")))
    reader_keys = set(filter(None, os.getenv("EDL_READER_KEYS", "").split(",")))
    legacy_key = os.getenv(settings.env_var)
    if legacy_key:
        operator_keys.add(legacy_key)
    return operator_keys, reader_keys


def _resolve_role(api_key: str, settings: APIKeySettings) -> Optional[str]:
    operator_keys, reader_keys = _load_keys(settings)
    if not operator_keys and not reader_keys:
        return None
    if api_key in operator_keys:
        return "operator"
    if api_key in reader_keys:
        return "reader"
    return None


async def require_api_key(
    api_key: Optional[str] = Security(_api_key_header),
    settings: APIKeySettings = _settings,
) -> AuthContext:
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )
    role = _resolve_role(api_key, settings)
    if role is None:
        if not any(_load_keys(settings)):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="API key not configured.",
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )
    return AuthContext(api_key=api_key, role=role)


async def require_reader(
    context: AuthContext = Security(require_api_key),
) -> AuthContext:
    return context


async def require_operator(
    context: AuthContext = Security(require_api_key),
) -> AuthContext:
    if context.role != "operator":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operator role required.",
        )
    return context


__all__ = ["require_api_key", "require_reader", "require_operator", "AuthContext"]
