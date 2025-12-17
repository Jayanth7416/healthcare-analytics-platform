"""Authentication Utilities"""

from fastapi import HTTPException, Header, Depends
from typing import Optional
import hashlib
import hmac
import structlog

from src.utils.config import settings
from src.services.cache import CacheService

logger = structlog.get_logger()

# In production, these would be stored in a database or secrets manager
VALID_API_KEYS = {
    "dev-api-key-12345": {
        "name": "Development Key",
        "provider_id": "DEV-001",
        "scopes": ["read", "write", "admin"]
    }
}


async def verify_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
) -> str:
    """
    Verify API key from request header

    Args:
        x_api_key: API key from X-API-Key header

    Returns:
        Validated API key

    Raises:
        HTTPException: If API key is invalid
    """
    if not x_api_key:
        logger.warning("missing_api_key")
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide X-API-Key header."
        )

    # Check if key exists and is valid
    key_info = VALID_API_KEYS.get(x_api_key)

    if not key_info:
        # Also check cache for dynamically issued keys
        cache = CacheService()
        cached_key = await cache.get(f"api_key:{hash_key(x_api_key)}")
        if not cached_key:
            logger.warning("invalid_api_key", key_prefix=x_api_key[:8])
            raise HTTPException(
                status_code=401,
                detail="Invalid API key"
            )
        key_info = cached_key

    logger.info(
        "api_key_verified",
        provider_id=key_info.get("provider_id"),
        name=key_info.get("name")
    )

    return x_api_key


def hash_key(api_key: str) -> str:
    """Hash API key for storage"""
    return hashlib.sha256(api_key.encode()).hexdigest()


def generate_api_key(provider_id: str, name: str) -> str:
    """
    Generate a new API key

    Args:
        provider_id: Provider identifier
        name: Key name/description

    Returns:
        Generated API key
    """
    import secrets
    return f"hap_{secrets.token_urlsafe(32)}"


async def check_scope(required_scope: str, api_key: str) -> bool:
    """
    Check if API key has required scope

    Args:
        required_scope: Required scope (read, write, admin)
        api_key: API key to check

    Returns:
        True if key has required scope
    """
    key_info = VALID_API_KEYS.get(api_key, {})
    scopes = key_info.get("scopes", [])
    return required_scope in scopes or "admin" in scopes


def require_scope(scope: str):
    """
    Dependency to require specific scope

    Usage:
        @router.post("/admin/action")
        async def admin_action(
            api_key: str = Depends(verify_api_key),
            _: bool = Depends(require_scope("admin"))
        ):
            ...
    """
    async def check(api_key: str = Depends(verify_api_key)):
        if not await check_scope(scope, api_key):
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient permissions. Required scope: {scope}"
            )
        return True
    return check
