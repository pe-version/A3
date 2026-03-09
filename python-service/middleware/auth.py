"""Bearer token authentication middleware."""

from typing import Optional

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import get_settings

# auto_error=False so we handle missing/malformed tokens ourselves with 401
security = HTTPBearer(auto_error=False)


def verify_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> str:
    """
    Dependency that validates the Bearer token.

    Args:
        credentials: The Authorization header credentials extracted by HTTPBearer.

    Returns:
        The validated token string.

    Raises:
        HTTPException: 401 if the token is invalid or missing.
    """
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    settings = get_settings()

    if credentials.credentials != settings.api_token:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return credentials.credentials
