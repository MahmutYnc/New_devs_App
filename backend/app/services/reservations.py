from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional
import logging

from sqlalchemy import text

from app.core.database_pool import db_pool

logger = logging.getLogger(__name__)

async def calculate_monthly_revenue(property_id: str, month: int, year: int, db_session=None) -> Decimal:
    """
    Calculates revenue for a specific month.
    """

    start_date = datetime(year, month, 1)
    if month < 12:
        end_date = datetime(year, month + 1, 1)
    else:
        end_date = datetime(year + 1, 1, 1)
        
    print(f"DEBUG: Querying revenue for {property_id} from {start_date} to {end_date}")

    # SQL Simulation (This would be executed against the actual DB)
    query = """
        SELECT SUM(total_amount) as total
        FROM reservations
        WHERE property_id = $1
        AND tenant_id = $2
        AND check_in_date >= $3
        AND check_in_date < $4
    """
    
    # In production this query executes against a database session.
    # result = await db.fetch_val(query, property_id, tenant_id, start_date, end_date)
    # return result or Decimal('0')
    
    return Decimal('0') # Placeholder for now until DB connection is finalized

async def _resolve_reporting_period(session, property_id: str, tenant_id: str) -> tuple[int, int]:
    """Resolve the latest available reporting period for a property in its local timezone."""
    period_query = text("""
        SELECT
            EXTRACT(YEAR FROM (r.check_in_date AT TIME ZONE p.timezone))::int AS year,
            EXTRACT(MONTH FROM (r.check_in_date AT TIME ZONE p.timezone))::int AS month
        FROM reservations r
        JOIN properties p
            ON p.id = r.property_id
            AND p.tenant_id = r.tenant_id
        WHERE r.property_id = :property_id
          AND r.tenant_id = :tenant_id
        ORDER BY year DESC, month DESC
        LIMIT 1
    """)

    period_row = (await session.execute(period_query, {
        "property_id": property_id,
        "tenant_id": tenant_id,
    })).fetchone()

    if period_row:
        return int(period_row.year), int(period_row.month)

    # Fallback: use latest period for the tenant across all properties.
    tenant_period_query = text("""
        SELECT
            EXTRACT(YEAR FROM (r.check_in_date AT TIME ZONE p.timezone))::int AS year,
            EXTRACT(MONTH FROM (r.check_in_date AT TIME ZONE p.timezone))::int AS month
        FROM reservations r
        JOIN properties p
            ON p.id = r.property_id
            AND p.tenant_id = r.tenant_id
        WHERE r.tenant_id = :tenant_id
        ORDER BY year DESC, month DESC
        LIMIT 1
    """)

    tenant_period_row = (await session.execute(tenant_period_query, {
        "tenant_id": tenant_id,
    })).fetchone()

    if tenant_period_row:
        return int(tenant_period_row.year), int(tenant_period_row.month)

    now = datetime.now(timezone.utc)
    return now.year, now.month


async def calculate_total_revenue(
    property_id: str,
    tenant_id: str,
    month: Optional[int] = None,
    year: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Aggregates monthly revenue from database.
    Revenue is grouped by property+tenant and filtered by check-in month in property's local timezone.
    """
    if db_pool.session_factory is None:
        await db_pool.initialize()

    if db_pool.session_factory is None:
        raise RuntimeError("Database pool not available")

    async with db_pool.get_session() as session:
        if month is None or year is None:
            resolved_year, resolved_month = await _resolve_reporting_period(session, property_id, tenant_id)
            year = year or resolved_year
            month = month or resolved_month

        query = text("""
            SELECT
                r.property_id,
                COALESCE(SUM(r.total_amount), 0) AS total_revenue,
                COUNT(*) AS reservation_count,
                COALESCE(MIN(r.currency), 'USD') AS currency
            FROM reservations r
            JOIN properties p
                ON p.id = r.property_id
                AND p.tenant_id = r.tenant_id
            WHERE r.property_id = :property_id
              AND r.tenant_id = :tenant_id
              AND EXTRACT(YEAR FROM (r.check_in_date AT TIME ZONE p.timezone))::int = :year
              AND EXTRACT(MONTH FROM (r.check_in_date AT TIME ZONE p.timezone))::int = :month
            GROUP BY r.property_id
        """)

        result = await session.execute(query, {
            "property_id": property_id,
            "tenant_id": tenant_id,
            "year": year,
            "month": month,
        })
        row = result.fetchone()

        if row:
            # Monetary values are rounded to cents deterministically to avoid float drift.
            total_revenue = Decimal(str(row.total_revenue)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            return {
                "property_id": property_id,
                "tenant_id": tenant_id,
                "year": year,
                "month": month,
                "total": str(total_revenue),
                "currency": row.currency,
                "count": int(row.reservation_count),
            }

        return {
            "property_id": property_id,
            "tenant_id": tenant_id,
            "year": year,
            "month": month,
            "total": "0.00",
            "currency": "USD",
            "count": 0,
        }
