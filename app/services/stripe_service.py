"""Stripe billing integration for subscription management."""

import stripe
from app.config import settings
from app.models.api_key import PlanTier

if settings.STRIPE_SECRET_KEY:
    stripe.api_key = settings.STRIPE_SECRET_KEY

PLAN_PRICE_MAP = {
    PlanTier.STARTER: settings.STRIPE_PRICE_STARTER,
    PlanTier.PRO: settings.STRIPE_PRICE_PRO,
    PlanTier.ENTERPRISE: settings.STRIPE_PRICE_ENTERPRISE,
}

PLAN_LIMITS = {
    PlanTier.FREE: settings.RATE_LIMIT_FREE,
    PlanTier.STARTER: settings.RATE_LIMIT_STARTER,
    PlanTier.PRO: settings.RATE_LIMIT_PRO,
    PlanTier.ENTERPRISE: settings.RATE_LIMIT_ENTERPRISE,
}


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
    price_id = PLAN_PRICE_MAP.get(plan)
    if not price_id:
        raise ValueError(f"No Stripe price configured for plan: {plan}")

    session = stripe.checkout.Session.create(
        customer=customer_id,
        payment_method_types=["card"],
        line_items=[{"price": price_id, "quantity": 1}],
        mode="subscription",
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"plan": plan.value},
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
    return PLAN_LIMITS.get(plan, settings.RATE_LIMIT_FREE)
