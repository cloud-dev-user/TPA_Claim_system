"""
Unit tests for EligibilityCheck.
Redis is mocked — no live infrastructure required.
"""

from unittest.mock import MagicMock, patch
import pytest
from app.eligibility import EligibilityCheck, EligibilityResult


@pytest.fixture
def mock_redis():
    return MagicMock()


@pytest.fixture
def checker(mock_redis):
    return EligibilityCheck(redis_client=mock_redis)


@pytest.fixture
def valid_claim():
    return {
        "claim_id":     "C001TEST",
        "member_id":    "M1001",
        "hospital_id":  "H5501-Apollo",
        "hospital_name": "Apollo",
        "insurer":      "StarHealth",
        "amount":       50000,
        "city":         "mumbai",
        "claim_type":   "cashless",
        "diagnosis_code": "Z51.1",
    }


class TestEligibilityCheck:

    def test_eligible_cashless_claim(self, checker, mock_redis, valid_claim):
        mock_redis.sismember.return_value = True
        mock_redis.hgetall.return_value = {
            "name": "Anita Sharma",
            "plan": "Gold Family Floater",
            "limit": "1000000",
            "used": "230000",
        }
        mock_redis.hincrbyfloat.return_value = 280000.0

        result = checker.check(valid_claim)

        assert result.eligible is True
        assert result.remaining_limit == 720000.0
        assert result.policy_plan == "Gold Family Floater"

    def test_hospital_not_empanelled(self, checker, mock_redis, valid_claim):
        mock_redis.sismember.return_value = False

        result = checker.check(valid_claim)

        assert result.eligible is False
        assert "not empanelled" in result.reason
        mock_redis.hgetall.assert_not_called()

    def test_member_policy_not_found(self, checker, mock_redis, valid_claim):
        mock_redis.sismember.return_value = True
        mock_redis.hgetall.return_value = {}

        result = checker.check(valid_claim)

        assert result.eligible is False
        assert "No active policy" in result.reason

    def test_insufficient_limit(self, checker, mock_redis, valid_claim):
        mock_redis.sismember.return_value = True
        mock_redis.hgetall.return_value = {
            "plan": "Silver", "limit": "100000", "used": "90000"
        }
        valid_claim["amount"] = 50000  # only 10k remaining

        result = checker.check(valid_claim)

        assert result.eligible is False
        assert "exceeds" in result.reason
        assert result.remaining_limit == 10000.0

    def test_exact_remaining_limit_is_eligible(self, checker, mock_redis, valid_claim):
        mock_redis.sismember.return_value = True
        mock_redis.hgetall.return_value = {
            "plan": "Gold", "limit": "100000", "used": "50000"
        }
        valid_claim["amount"] = 50000  # exactly equal to remaining

        result = checker.check(valid_claim)

        assert result.eligible is True

    def test_reimbursement_skips_empanelment_check(self, checker, mock_redis, valid_claim):
        valid_claim["claim_type"] = "reimbursement"
        mock_redis.hgetall.return_value = {
            "plan": "Gold", "limit": "1000000", "used": "0"
        }
        mock_redis.hincrbyfloat.return_value = 50000.0

        result = checker.check(valid_claim)

        mock_redis.sismember.assert_not_called()
        assert result.eligible is True

    def test_redis_error_on_policy_fetch(self, checker, mock_redis, valid_claim):
        import redis
        mock_redis.sismember.return_value = True
        mock_redis.hgetall.side_effect = redis.RedisError("connection refused")

        result = checker.check(valid_claim)

        assert result.eligible is False
        assert "Redis error" in result.reason

    def test_result_to_dict_contains_decision_key(self, checker, mock_redis, valid_claim):
        mock_redis.sismember.return_value = True
        mock_redis.hgetall.return_value = {
            "plan": "Gold", "limit": "1000000", "used": "0"
        }
        mock_redis.hincrbyfloat.return_value = 50000.0

        result = checker.check(valid_claim)
        d = result.to_dict()

        assert "decision" in d
        assert d["decision"] == "ELIGIBLE"
