"""
EligibilityCheck — domain logic for member policy and hospital eligibility.

Reads from Redis Hashes (member policy) and Redis Sets (empanelled hospitals).
All Redis keys match the seed data in docker-compose.yml → redis-init.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import redis

from app.config import config

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class EligibilityResult:
    claim_id:       str
    member_id:      str
    eligible:       bool
    reason:         str
    remaining_limit: Optional[float] = None
    policy_plan:    Optional[str]    = None

    def to_dict(self) -> dict:
        return {
            "claim_id":        self.claim_id,
            "member_id":       self.member_id,
            "eligible":        self.eligible,
            "reason":          self.reason,
            "remaining_limit": self.remaining_limit,
            "policy_plan":     self.policy_plan,
            "decision":        "ELIGIBLE" if self.eligible else "INELIGIBLE",
        }


# ──────────────────────────────────────────────────────────────────────────────
# EligibilityCheck
# ──────────────────────────────────────────────────────────────────────────────

class EligibilityCheck:
    """
    Performs eligibility check for a claim event using Redis:

    1. Check if hospital is empanelled in the claim city (Redis Set).
    2. Fetch member policy details (Redis Hash).
    3. Verify member has sufficient remaining limit.
    4. If eligible, update used amount (HINCRBY).
    5. Re-cache policy with TTL reset (EXPIRE).
    """

    def __init__(self, redis_client: redis.Redis) -> None:
        self._r = redis_client

    def check(self, claim: dict) -> EligibilityResult:
        claim_id     = claim["claim_id"]
        member_id    = claim["member_id"]
        hospital_name = claim["hospital_name"]
        city         = claim.get("city", "").lower()
        amount       = float(claim["amount"])
        claim_type   = claim.get("claim_type", "cashless")

        # ── Step 1: Hospital empanelment check (cashless only) ─────────────────
        if claim_type == "cashless":
            empanelled_key = f"empanelled:{city}"
            try:
                is_empanelled = self._r.sismember(empanelled_key, hospital_name)
            except redis.RedisError as exc:
                logger.error("Redis SISMEMBER failed | key=%s error=%s", empanelled_key, exc)
                is_empanelled = None  # treat as unknown — do not auto-reject

            if is_empanelled is False:
                logger.info(
                    "Hospital not empanelled | claim_id=%s hospital=%s city=%s",
                    claim_id, hospital_name, city,
                )
                return EligibilityResult(
                    claim_id=claim_id,
                    member_id=member_id,
                    eligible=False,
                    reason=f"Hospital '{hospital_name}' is not empanelled in {city} for cashless claims",
                )

        # ── Step 2: Fetch member policy from Redis Hash ────────────────────────
        policy_key = f"member:{member_id}:policy"
        try:
            policy = self._r.hgetall(policy_key)
        except redis.RedisError as exc:
            logger.error("Redis HGETALL failed | key=%s error=%s", policy_key, exc)
            return EligibilityResult(
                claim_id=claim_id,
                member_id=member_id,
                eligible=False,
                reason="Policy data unavailable — Redis error",
            )

        if not policy:
            logger.info("Policy not found | claim_id=%s member_id=%s", claim_id, member_id)
            return EligibilityResult(
                claim_id=claim_id,
                member_id=member_id,
                eligible=False,
                reason=f"No active policy found for member {member_id}",
            )

        limit = float(policy.get("limit", 0))
        used  = float(policy.get("used", 0))
        plan  = policy.get("plan", "Unknown")
        remaining = limit - used

        logger.info(
            "Policy loaded | member=%s plan=%s limit=%.0f used=%.0f remaining=%.0f",
            member_id, plan, limit, used, remaining,
        )

        # ── Step 3: Sufficient limit check ────────────────────────────────────
        if amount > remaining:
            return EligibilityResult(
                claim_id=claim_id,
                member_id=member_id,
                eligible=False,
                reason=(
                    f"Claim amount ₹{amount:,.0f} exceeds remaining policy limit "
                    f"₹{remaining:,.0f} (plan: {plan})"
                ),
                remaining_limit=remaining,
                policy_plan=plan,
            )

        # ── Step 4: Deduct from used amount (optimistic update) ────────────────
        try:
            new_used = self._r.hincrbyfloat(policy_key, "used", amount)
            self._r.expire(policy_key, config.POLICY_TTL)
            logger.info(
                "Policy updated | member=%s used_before=%.0f claim_amount=%.0f used_after=%.0f",
                member_id, used, amount, new_used,
            )
        except redis.RedisError as exc:
            logger.warning(
                "Failed to update policy used amount | member=%s error=%s (non-fatal)",
                member_id, exc,
            )

        return EligibilityResult(
            claim_id=claim_id,
            member_id=member_id,
            eligible=True,
            reason=f"Member has sufficient coverage under {plan}",
            remaining_limit=remaining - amount,
            policy_plan=plan,
        )
