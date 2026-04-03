"""
FraudRule — domain logic for real-time fraud detection.

Uses Redis:
  - Sorted Set  fraud:hospital:scores     — hospital risk leaderboard (ZINCRBY)
  - Counter     fraud:member:<id>:claims:<date> — daily claim count per member (INCR + EXPIRE)
  - String      claim:<id>:status          — update claim status on fraud flag
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import redis

from app.config import config

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Scoring weights — tune these to match MD India risk appetite
# ──────────────────────────────────────────────────────────────────────────────

SCORE_HIGH_AMOUNT     = 20   # claim > HIGH_AMOUNT_THRESHOLD
SCORE_DAILY_EXCESS    = 30   # member exceeds DAILY_CLAIM_LIMIT
SCORE_HOSPITAL_BASE   = 5    # every claim increments hospital score slightly
SCORE_ROUND_NUMBER    = 5    # suspiciously round amounts (100k, 500k)


# ──────────────────────────────────────────────────────────────────────────────
# Result
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class FraudAssessment:
    claim_id:        str
    member_id:       str
    hospital_id:     str
    risk_score:      float
    is_flagged:      bool
    reasons:         list[str] = field(default_factory=list)
    hospital_score:  Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "claim_id":       self.claim_id,
            "member_id":      self.member_id,
            "hospital_id":    self.hospital_id,
            "risk_score":     self.risk_score,
            "is_flagged":     self.is_flagged,
            "reasons":        self.reasons,
            "hospital_score": self.hospital_score,
            "verdict":        "FRAUD_REVIEW" if self.is_flagged else "CLEAR",
        }


# ──────────────────────────────────────────────────────────────────────────────
# FraudRule
# ──────────────────────────────────────────────────────────────────────────────

class FraudRule:
    """
    Applies heuristic fraud rules to a claim event.

    Rules applied (in order):
      1. High-amount claim flag
      2. Round-number amount flag
      3. Daily member claim volume check
      4. Aggregate hospital risk score update
    """

    def __init__(self, redis_client: redis.Redis) -> None:
        self._r = redis_client

    def assess(self, claim: dict) -> FraudAssessment:
        claim_id    = claim["claim_id"]
        member_id   = claim["member_id"]
        hospital_id = claim["hospital_id"]
        amount      = float(claim["amount"])

        risk_score = 0.0
        reasons: list[str] = []

        # ── Rule 1: High-amount claim ──────────────────────────────────────────
        if amount >= config.HIGH_AMOUNT_THRESHOLD:
            risk_score += SCORE_HIGH_AMOUNT
            reasons.append(f"High claim amount ₹{amount:,.0f} (≥ ₹{config.HIGH_AMOUNT_THRESHOLD:,.0f})")
            logger.debug("Rule 1 triggered | claim_id=%s amount=%.0f", claim_id, amount)

        # ── Rule 2: Round-number amount ────────────────────────────────────────
        if amount % 100000 == 0 and amount >= 100000:
            risk_score += SCORE_ROUND_NUMBER
            reasons.append(f"Round-number amount ₹{amount:,.0f} is suspicious")
            logger.debug("Rule 2 triggered | claim_id=%s amount=%.0f", claim_id, amount)

        # ── Rule 3: Daily member claim count ──────────────────────────────────
        today = date.today().isoformat()
        daily_key = f"fraud:member:{member_id}:claims:{today}"
        try:
            daily_count = self._r.incr(daily_key)
            self._r.expire(daily_key, 86400)  # reset at midnight + buffer

            if daily_count > config.DAILY_CLAIM_LIMIT:
                risk_score += SCORE_DAILY_EXCESS
                reasons.append(
                    f"Member {member_id} has submitted {daily_count} claims today "
                    f"(limit: {config.DAILY_CLAIM_LIMIT})"
                )
                logger.warning(
                    "Rule 3 triggered | member=%s daily_count=%d", member_id, daily_count
                )
        except redis.RedisError as exc:
            logger.error("Redis INCR failed | key=%s error=%s", daily_key, exc)

        # ── Rule 4: Hospital score update ──────────────────────────────────────
        hospital_scores_key = "fraud:hospital:scores"
        score_increment = SCORE_HOSPITAL_BASE + (risk_score * 0.5)
        hospital_total: Optional[float] = None

        try:
            hospital_total = self._r.zincrby(hospital_scores_key, score_increment, hospital_id)
            logger.debug(
                "Hospital score updated | hospital=%s increment=%.1f total=%.1f",
                hospital_id, score_increment, hospital_total,
            )
        except redis.RedisError as exc:
            logger.error("Redis ZINCRBY failed | key=%s error=%s", hospital_scores_key, exc)

        is_flagged = risk_score >= config.FRAUD_SCORE_THRESHOLD

        if is_flagged:
            logger.warning(
                "FRAUD FLAGGED | claim_id=%s member=%s hospital=%s score=%.1f reasons=%s",
                claim_id, member_id, hospital_id, risk_score, reasons,
            )
        else:
            logger.info(
                "Fraud check clear | claim_id=%s score=%.1f", claim_id, risk_score
            )

        return FraudAssessment(
            claim_id=claim_id,
            member_id=member_id,
            hospital_id=hospital_id,
            risk_score=risk_score,
            is_flagged=is_flagged,
            reasons=reasons,
            hospital_score=hospital_total,
        )

    def get_top_risk_hospitals(self, n: int = 5) -> list[tuple[str, float]]:
        """Returns top-N hospitals by fraud risk score (for ops dashboard)."""
        try:
            results = self._r.zrevrange("fraud:hospital:scores", 0, n - 1, withscores=True)
            return [(h, s) for h, s in results]
        except redis.RedisError:
            return []
