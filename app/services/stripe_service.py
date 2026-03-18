"""Stripe billing integration for subscription management."""

import stripe
from app.config import settings
from app.models.api_key import PlanTier, resolve_plan

if settings.STRIPE_SECRET_KEY:
    stripe.api_key = settings.STRIPE_SECRET_KEY

# Stripe price IDs — new tiers preferred, fall back to legacy env vars
PLAN_PRICE_MAP = {
    PlanTier.EXPLORER: settings.STRIPE_PRICE_EXPLORER or settings.STRIPE_PRICE_STARTER,
    PlanTier.PRO_LEADS: settings.STRIPE_PRICE_PRO_LEADS or settings.STRIPE_PRICE_PRO,
    PlanTier.REALTIME: settings.STRIPE_PRICE_REALTIME,
    PlanTier.ENTERPRISE: settings.STRIPE_PRICE_ENTERPRISE,
    # Legacy aliases point to the same prices
    PlanTier.STARTER: settings.STRIPE_PRICE_EXPLORER or settings.STRIPE_PRICE_STARTER,
    PlanTier.PRO: settings.STRIPE_PRICE_PRO_LEADS or settings.STRIPE_PRICE_PRO,
}

# Daily lookup limits per plan
PLAN_LIMITS = {
    PlanTier.FREE: settings.RATE_LIMIT_FREE,           # 100
    PlanTier.EXPLORER: settings.RATE_LIMIT_EXPLORER,    # 500
    PlanTier.PRO_LEADS: settings.RATE_LIMIT_PRO_LEADS,  # 2,000
    PlanTier.REALTIME: settings.RATE_LIMIT_REALTIME,    # 10,000
    PlanTier.ENTERPRISE: settings.RATE_LIMIT_ENTERPRISE,  # 1,000,000
    # Legacy aliases
    PlanTier.STARTER: settings.RATE_LIMIT_EXPLORER,
    PlanTier.PRO: settings.RATE_LIMIT_PRO_LEADS,
}

# Data freshness limits (days) — how old permits must be for each plan
# 0 = no restriction (sees all data including real-time)
FRESHNESS_LIMITS = {
    PlanTier.FREE: settings.DATA_FRESHNESS_FREE,              # 180
    PlanTier.EXPLORER: settings.DATA_FRESHNESS_EXPLORER,      # 90
    PlanTier.PRO_LEADS: settings.DATA_FRESHNESS_PRO_LEADS,    # 30
    PlanTier.REALTIME: settings.DATA_FRESHNESS_REALTIME,      # 0
    PlanTier.ENTERPRISE: settings.DATA_FRESHNESS_ENTERPRISE,  # 0
    # Legacy aliases
    PlanTier.STARTER: settings.DATA_FRESHNESS_EXPLORER,
    PlanTier.PRO: settings.DATA_FRESHNESS_PRO_LEADS,
}

# Alert limits per plan
ALERT_LIMITS = {
    PlanTier.FREE: 2,
    PlanTier.EXPLORER: 25,
    PlanTier.PRO_LEADS: 100,
    PlanTier.REALTIME: 500,
    PlanTier.ENTERPRISE: 10000,
    # Legacy aliases
    PlanTier.STARTER: 25,
    PlanTier.PRO: 100,
}

# Enrichment costs (cents) by enrichment type
ENRICHMENT_COSTS = {
    "phone": settings.ENRICHMENT_PHONE_CENTS,       # 200 ($2)
    "email": settings.ENRICHMENT_EMAIL_CENTS,        # 100 ($1)
    "property": settings.ENRICHMENT_PROPERTY_CENTS,  # 300 ($3)
    "full": settings.ENRICHMENT_FULL_CENTS,          # 500 ($5)
}

# Minimum plan required for enrichment
ENRICHMENT_MIN_PLAN = PlanTier.PRO_LEADS


async def create_customer(email: str, name: str | None = None) -> str:
    """Create a Stripe customer and return customer ID."""
    customer = stripe.Customer.create(
        email=email,
        name=name,
        metadata={"source": "permit_api"},
    )
    return customer.id


async def create_checkout_session(
    customer_id: str,
    plan: PlanTier,
    success_url: str,
    cancel_url: str,
) -> str:
    """Create a Stripe Checkout session for subscription signup."""
    resolved = resolve_plan(plan)
    price_id = PLAN_PRICE_MAP.get(resolved)
    if not price_id:
        raise ValueError(f"No Stripe price configured for plan: {resolved}")

    session = stripe.checkout.Session.create(
        customer=customer_id,
        payment_method_types=["card"],
        line_items=[{"price": price_id, "quantity": 1}],
        mode="subscription",
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"plan": resolved.value},
    )
    return session.url


async def create_usage_record(subscription_id: str, quantity: int) -> None:
    """Report metered usage to Stripe for overage billing."""
    if not subscription_id:
        return

    # Get the subscription's metered item
    subscription = stripe.Subscription.retrieve(subscription_id)
    for item in subscription["items"]["data"]:
        if item.get("price", {}).get("recurring", {}).get("usage_type") == "metered":
            stripe.SubscriptionItem.create_usage_record(
                item["id"],
                quantity=quantity,
                action="increment",
            )
            break


async def cancel_subscription(subscription_id: str) -> None:
    """Cancel a Stripe subscription at period end."""
    stripe.Subscription.modify(
        subscription_id,
        cancel_at_period_end=True,
    )


def get_daily_limit(plan: PlanTier) -> int:
    """Get daily lookup limit for a plan tier."""
    resolved = resolve_plan(plan)
    return PLAN_LIMITS.get(resolved, settings.RATE_LIMIT_FREE)


def get_freshness_limit(plan: PlanTier) -> int:
    """Get data freshness limit in days. 0 = no restriction (all data)."""
    resolved = resolve_plan(plan)
    return FRESHNESS_LIMITS.get(resolved, settings.DATA_FRESHNESS_FREE)


def get_alert_limit(plan: PlanTier) -> int:
    """Get maximum number of alerts for a plan tier."""
    resolved = resolve_plan(plan)
    return ALERT_LIMITS.get(resolved, 2)


def get_enrichment_cost(enrichment_type: str) -> int:
    """Get enrichment cost in cents. Returns 0 for unknown types."""
    return ENRICHMENT_COSTS.get(enrichment_type, 0)
