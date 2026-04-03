"""
Unit tests for FraudRule.
Redis is mocked — no live infrastructure required.
"""

from unittest.mock import MagicMock
import pytest
from app.fraud import FraudRule, FraudAssessment


@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.incr.return_value = 1        # first claim of the day by default
    r.zincrby.return_value = 5.0
    return r


@pytest.fixture
def fraud_rule(mock_redis):
    return FraudRule(redis_client=mock_redis)


@pytest.fixture
def normal_claim():
    return {
        "claim_id":     "C001TEST",
        "member_id":    "M1001",
        "hospital_id":  "H5501-Apollo",
        "hospital_name": "Apollo",
        "insurer":      "StarHealth",
        "amount":       50000,
        "city":         "mumbai",
        "claim_type":   "cashless",
    }


class TestFraudRule:

    def test_normal_claim_is_clear(self, fraud_rule, normal_claim):
        result = fraud_rule.assess(normal_claim)
        assert result.is_flagged is False
        assert result.risk_score < 50

    def test_high_amount_increases_score(self, fraud_rule, normal_claim):
        normal_claim["amount"] = 600000  # above HIGH_AMOUNT_THRESHOLD (500k)
        result = fraud_rule.assess(normal_claim)
        assert result.risk_score >= 20
        assert any("High claim amount" in r for r in result.reasons)

    def test_round_number_increases_score(self, fraud_rule, normal_claim):
        normal_claim["amount"] = 200000  # exactly 2 lakh — suspicious
        result = fraud_rule.assess(normal_claim)
        assert any("Round-number" in r for r in result.reasons)

    def test_daily_limit_excess_increases_score(self, fraud_rule, mock_redis, normal_claim):
        mock_redis.incr.return_value = 6  # 6 claims today, limit is 5
        result = fraud_rule.assess(normal_claim)
        assert any("claims today" in r for r in result.reasons)

    def test_high_amount_and_daily_excess_flags_claim(self, fraud_rule, mock_redis, normal_claim):
        mock_redis.incr.return_value = 6
        normal_claim["amount"] = 600000
        result = fraud_rule.assess(normal_claim)
        assert result.is_flagged is True
        assert result.risk_score >= 50

    def test_hospital_score_updated_on_every_claim(self, fraud_rule, mock_redis, normal_claim):
        fraud_rule.assess(normal_claim)
        mock_redis.zincrby.assert_called_once()
        call_args = mock_redis.zincrby.call_args
        assert call_args[0][0] == "fraud:hospital:scores"
        assert call_args[0][2] == normal_claim["hospital_id"]

    def test_assessment_to_dict_contains_verdict(self, fraud_rule, normal_claim):
        result = fraud_rule.assess(normal_claim)
        d = result.to_dict()
        assert "verdict" in d
        assert d["verdict"] in ("CLEAR", "FRAUD_REVIEW")

    def test_redis_error_on_incr_does_not_crash(self, fraud_rule, mock_redis, normal_claim):
        import redis
        mock_redis.incr.side_effect = redis.RedisError("timeout")
        result = fraud_rule.assess(normal_claim)
        # Should still return an assessment, not raise
        assert isinstance(result, FraudAssessment)

    def test_get_top_risk_hospitals(self, fraud_rule, mock_redis):
        mock_redis.zrevrange.return_value = [
            ("H5502-Unknown", 75.0),
            ("H5501-Apollo",  25.0),
        ]
        top = fraud_rule.get_top_risk_hospitals(n=2)
        assert len(top) == 2
        assert top[0][0] == "H5502-Unknown"
        assert top[0][1] == 75.0

    def test_clear_verdict_in_dict(self, fraud_rule, normal_claim):
        result = fraud_rule.assess(normal_claim)
        assert result.to_dict()["verdict"] == "CLEAR"

    def test_flagged_verdict_in_dict(self, fraud_rule, mock_redis, normal_claim):
        mock_redis.incr.return_value = 6
        normal_claim["amount"] = 600000
        result = fraud_rule.assess(normal_claim)
        assert result.to_dict()["verdict"] == "FRAUD_REVIEW"
