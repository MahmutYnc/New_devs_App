from decimal import Decimal, ROUND_HALF_UP
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any
from app.services.cache import get_revenue_summary
from app.core.auth import authenticate_request as get_current_user

router = APIRouter()

@router.get("/dashboard/summary")
async def get_dashboard_summary(
    property_id: str,
    month: int | None = Query(default=None, ge=1, le=12),
    year: int | None = Query(default=None, ge=2000, le=2100),
    current_user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    tenant_id = getattr(current_user, "tenant_id", None)
    if not tenant_id:
        raise HTTPException(status_code=403, detail="Tenant context is required")

    revenue_data = await get_revenue_summary(
        property_id=property_id,
        tenant_id=tenant_id,
        month=month,
        year=year,
    )

    # Round currency using decimal first to avoid floating point drift in API output.
    total_revenue = Decimal(str(revenue_data["total"])).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    return {
        "property_id": revenue_data["property_id"],
        "total_revenue": float(total_revenue),
        "currency": revenue_data["currency"],
        "reservations_count": revenue_data["count"],
        "reporting_month": revenue_data.get("month"),
        "reporting_year": revenue_data.get("year"),
    }
