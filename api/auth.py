from __future__ import annotations

import os
from typing import Optional, Set

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from .schemas import APIKeySettings
from pydantic import BaseModel
from db.persistence import get_profile_by_api_key

_settings = APIKeySettings()
_api_key_header = APIKeyHeader(name=_settings.header_name, auto_error=False)


class AuthContext(BaseModel):
    api_key: str
    role: str  # "admin-operator", "admin-reader", or "profile"
    profile_id: Optional[str] = None

    @property
    def is_admin(self) -> bool:
        return self.profile_id is None and self.role.startswith("admin")

    @property
    def is_profile(self) -> bool:
        return self.profile_id is not None


def _load_keys(settings: APIKeySettings) -> tuple[Set[str], Set[str]]:
    operator_keys = set(filter(None, os.getenv("EDL_OPERATOR_KEYS", "").split(",")))
    reader_keys = set(filter(None, os.getenv("EDL_READER_KEYS", "").split(",")))
    legacy_key = os.getenv(settings.env_var)
    if legacy_key:
        operator_keys.add(legacy_key)
    return operator_keys, reader_keys


def _resolve_admin_role(api_key: str, settings: APIKeySettings) -> Optional[str]:
    operator_keys, reader_keys = _load_keys(settings)
    if api_key in operator_keys:
        return "admin-operator"
    if api_key in reader_keys:
        return "admin-reader"
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

    # First see if this is a system/admin key.
    role = _resolve_admin_role(api_key, settings)
    if role:
        return AuthContext(api_key=api_key, role=role)

    # Otherwise fall back to per-profile API keys.
    profile = get_profile_by_api_key(api_key)
    if profile:
        return AuthContext(api_key=api_key, role="profile", profile_id=profile.id)

    operator_keys, reader_keys = _load_keys(settings)
    if not operator_keys and not reader_keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API key not configured.",
        )

    raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )


async def require_reader(
    context: AuthContext = Security(require_api_key),
) -> AuthContext:
    return context


async def require_operator(
    context: AuthContext = Security(require_api_key),
) -> AuthContext:
    # Profile-scoped keys have full operator capabilities for their own tenant.
    if context.is_profile:
        return context
    if context.role == "admin-operator":
        return context
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Operator role required.",
    )


async def require_profile(
    context: AuthContext = Security(require_api_key),
) -> AuthContext:
    if context.profile_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Profile API key required.",
        )
    return context


__all__ = ["require_api_key", "require_reader", "require_operator", "require_profile", "AuthContext"]
