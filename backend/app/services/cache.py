import json
import redis.asyncio as redis
from typing import Dict, Any, Optional
import os
import logging

# Initialize Redis client (typically configured centrally).
redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
logger = logging.getLogger(__name__)

def _revenue_cache_key(
    property_id: str,
    tenant_id: str,
    month: Optional[int],
    year: Optional[int]
) -> str:
    period = f"{year}-{month}" if year is not None and month is not None else "latest"
    return f"revenue:v3:{tenant_id}:{property_id}:{period}"


async def get_revenue_summary(
    property_id: str,
    tenant_id: str,
    month: Optional[int] = None,
    year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Fetches revenue summary, utilizing caching to improve performance.
    """
    if not tenant_id:
        raise ValueError("tenant_id is required for revenue summary cache isolation")

    cache_key = _revenue_cache_key(property_id, tenant_id, month, year)

    # Try to get from cache (best effort).
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception as exc:
        logger.warning("Redis GET failed for key %s: %s", cache_key, exc)

    # Revenue calculation is delegated to the reservation service.
    from app.services.reservations import calculate_total_revenue

    # Calculate revenue
    result = await calculate_total_revenue(
        property_id=property_id,
        tenant_id=tenant_id,
        month=month,
        year=year,
    )

    # Cache the result for 5 minutes (best effort).
    try:
        await redis_client.setex(cache_key, 300, json.dumps(result))
    except Exception as exc:
        logger.warning("Redis SETEX failed for key %s: %s", cache_key, exc)

    return result
