"""Stripe billing endpoints and webhook handler."""

import stripe
import logging
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone

from app.config import settings
from app.database import get_db, async_session_maker
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan, PLAN_MIGRATION
from app.services.stripe_service import (
    create_customer,
    create_checkout_session,
    get_daily_limit,
    get_freshness_limit,
    get_alert_limit,
    get_enrichment_cost,
    ENRICHMENT_COSTS,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Billing"])

# Valid plan names for the subscribe endpoint (new + legacy)
_VALID_SUBSCRIBE_PLANS = {"explorer", "pro_leads", "realtime", "enterprise", "starter", "pro"}


@router.get("/usage")
async def get_usage(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get current billing period usage stats including freshness tier info."""
    today = datetime.now(timezone.utc).date()
    start_of_day = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)

    plan = resolve_plan(user.plan)

    # Today's usage
    daily_result = await db.execute(
        select(func.coalesce(func.sum(UsageLog.lookup_count), 0))
        .where(UsageLog.user_id == user.id, UsageLog.created_at >= start_of_day)
    )
    daily_used = daily_result.scalar()

    # This month's usage
    start_of_month = today.replace(day=1)
    start_of_month_dt = datetime.combine(start_of_month, datetime.min.time(), tzinfo=timezone.utc)
    monthly_result = await db.execute(
        select(func.coalesce(func.sum(UsageLog.lookup_count), 0))
        .where(UsageLog.user_id == user.id, UsageLog.created_at >= start_of_month_dt)
    )
    monthly_used = monthly_result.scalar()

    # Enrichment usage this month (tracked via endpoint pattern)
    enrichment_result = await db.execute(
        select(func.coalesce(func.sum(UsageLog.lookup_count), 0))
        .where(
            UsageLog.user_id == user.id,
            UsageLog.created_at >= start_of_month_dt,
            UsageLog.endpoint.like("%enrichment=%"),
        )
    )
    enrichment_used = enrichment_result.scalar()

    daily_limit = get_daily_limit(plan)
    freshness_days = get_freshness_limit(plan)
    alert_limit = get_alert_limit(plan)

    return {
        "plan": plan.value,
        "daily_used": daily_used,
        "daily_limit": daily_limit,
        "daily_remaining": max(0, daily_limit - daily_used),
        "monthly_used": monthly_used,
        "monthly_enrichments": enrichment_used,
        "billing_period_start": start_of_month.isoformat(),
        "alert_limit": alert_limit,
        "freshness": {
            "limit_days": freshness_days,
            "cutoff_date": (date.today() - timedelta(days=freshness_days)).isoformat() if freshness_days > 0 else None,
            "can_access_hot": freshness_days == 0,
            "can_access_warm": freshness_days <= 30,
            "can_access_mild": freshness_days <= 90,
            "can_access_cold": True,
        },
        "enrichment_pricing": {
            k: f"${v / 100:.2f}/lead" for k, v in ENRICHMENT_COSTS.items()
        },
    }


@router.post("/subscribe")
async def subscribe(
    request: Request,
    plan: str,
    success_url: str = None,
    cancel_url: str = None,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a Stripe checkout session to subscribe to a paid plan."""
    if plan not in _VALID_SUBSCRIBE_PLANS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid plan: {plan}. Use: explorer, pro_leads, realtime, enterprise",
        )

    try:
        tier = resolve_plan(plan)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid plan: {plan}. Use: explorer, pro_leads, realtime, enterprise",
        )

    if tier == PlanTier.FREE:
        raise HTTPException(status_code=400, detail="You're already on the free plan.")

    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Billing not configured.")

    # Create Stripe customer if needed
    if not user.stripe_customer_id:
        customer_id = await create_customer(user.email, user.company_name)
        user.stripe_customer_id = customer_id
        await db.commit()

    checkout_url = await create_checkout_session(
        customer_id=user.stripe_customer_id,
        plan=tier,
        success_url=success_url or f"{settings.FRONTEND_URL}/dashboard?subscribed=true",
        cancel_url=cancel_url or f"{settings.FRONTEND_URL}/pricing",
    )

    return {"checkout_url": checkout_url}


@router.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events (subscription changes, payments)."""
    if not settings.STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="Webhooks not configured.")

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, settings.STRIPE_WEBHOOK_SECRET
        )
    except (ValueError, stripe.error.SignatureVerificationError):
        raise HTTPException(status_code=400, detail="Invalid webhook signature.")

    async with async_session_maker() as db:
        if event["type"] == "checkout.session.completed":
            session = event["data"]["object"]
            customer_id = session["customer"]
            subscription_id = session["subscription"]
            plan_str = session.get("metadata", {}).get("plan", "explorer")

            result = await db.execute(
                select(ApiUser).where(ApiUser.stripe_customer_id == customer_id)
            )
            user = result.scalar_one_or_none()
            if user:
                # Resolve plan name (handles legacy "starter"/"pro" values)
                resolved = resolve_plan(plan_str)
                user.plan = resolved
                user.stripe_subscription_id = subscription_id
                await db.commit()
                logger.info(f"User {user.email} subscribed to {resolved.value}")

        elif event["type"] == "customer.subscription.deleted":
            subscription = event["data"]["object"]
            customer_id = subscription["customer"]

            result = await db.execute(
                select(ApiUser).where(ApiUser.stripe_customer_id == customer_id)
            )
            user = result.scalar_one_or_none()
            if user:
                user.plan = PlanTier.FREE
                user.stripe_subscription_id = None
                await db.commit()
                logger.info(f"User {user.email} subscription cancelled, reverted to free")

        elif event["type"] == "invoice.payment_failed":
            invoice = event["data"]["object"]
            customer_id = invoice["customer"]
            logger.warning(f"Payment failed for Stripe customer {customer_id}")

    return {"status": "ok"}
