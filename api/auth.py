from __future__ import annotations

import os
from typing import Optional, Set, Tuple

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from .schemas import APIKeySettings
from pydantic import BaseModel
from db.persistence import get_profile_by_api_key, get_admin_account_by_api_key

_settings = APIKeySettings()
_api_key_header = APIKeyHeader(name=_settings.header_name, auto_error=False)


class AuthContext(BaseModel):
    api_key: str
    role: str  # "admin-operator", "admin-reader", "admin-super", or "profile"
    profile_id: Optional[str] = None
    admin_id: Optional[str] = None
    is_super_admin: bool = False

    @property
    def is_admin(self) -> bool:
        return self.admin_id is not None or self.is_super_admin or self.role.startswith("admin")

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


def _resolve_env_admin_role(api_key: str, settings: APIKeySettings) -> Optional[Tuple[str, bool]]:
    operator_keys, reader_keys = _load_keys(settings)
    if api_key in operator_keys:
        return ("admin-operator", True)
    if api_key in reader_keys:
        return ("admin-reader", True)
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

    # First see if this is a managed admin key stored in the database.
    account = get_admin_account_by_api_key(api_key)
    if account:
        role = "admin-operator" if account.role == "operator" else "admin-reader"
        return AuthContext(
            api_key=api_key,
            role=role,
            admin_id=account.id,
            is_super_admin=account.is_super_admin,
        )

    # Next check if this is a legacy env-configured admin key (treated as super-admin).
    env_admin = _resolve_env_admin_role(api_key, settings)
    if env_admin:
        role, is_super = env_admin
        return AuthContext(api_key=api_key, role="admin-super" if is_super else role, is_super_admin=True)

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
    if context.is_super_admin or context.role in {"admin-operator", "admin-super"}:
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


async def require_super_admin(
    context: AuthContext = Security(require_api_key),
) -> AuthContext:
    if context.is_super_admin:
        return context
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Super admin privileges required.",
    )


__all__ = [
    "require_api_key",
    "require_reader",
    "require_operator",
    "require_profile",
    "require_super_admin",
    "AuthContext",
]
