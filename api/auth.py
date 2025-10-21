from __future__ import annotations

import os
from typing import Optional

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from .schemas import APIKeySettings

_settings = APIKeySettings()
_api_key_header = APIKeyHeader(name=_settings.header_name, auto_error=False)


async def require_api_key(
    api_key: Optional[str] = Security(_api_key_header),
    settings: APIKeySettings = _settings,
) -> str:
    configured_key = os.getenv(settings.env_var)
    if not configured_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API key not configured.",
        )
    if not api_key or api_key != configured_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )
    return api_key


__all__ = ["require_api_key"]
