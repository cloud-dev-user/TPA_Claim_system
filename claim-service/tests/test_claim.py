"""
Unit tests for ClaimSubmit and ClaimValidator.
Run: pytest tests/ -v
"""

import pytest
from app.claim import ClaimSubmit, ClaimStatus, ClaimType, ClaimValidator


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_claim() -> ClaimSubmit:
    return ClaimSubmit(
        member_id="M1001",
        hospital_id="H5501-Apollo",
        hospital_name="Apollo Hospital Mumbai",
        insurer="StarHealth",
        amount=85000.0,
        city="mumbai",
        claim_type="cashless",
        diagnosis_code="Z51.1",
    )


@pytest.fixture
def validator() -> ClaimValidator:
    return ClaimValidator()


# ── ClaimSubmit tests ──────────────────────────────────────────────────────────

class TestClaimSubmit:
    def test_claim_id_auto_generated(self, valid_claim):
        assert valid_claim.claim_id.startswith("C")
        assert len(valid_claim.claim_id) == 9  # 'C' + 8 hex chars

    def test_default_status_is_submitted(self, valid_claim):
        assert valid_claim.status == ClaimStatus.SUBMITTED

    def test_submitted_at_is_set(self, valid_claim):
        assert valid_claim.submitted_at is not None
        assert "T" in valid_claim.submitted_at  # ISO format

    def test_to_dict_round_trip(self, valid_claim):
        d = valid_claim.to_dict()
        restored = ClaimSubmit.from_dict(d)
        assert restored.claim_id == valid_claim.claim_id
        assert restored.amount == valid_claim.amount
        assert restored.insurer == valid_claim.insurer

    def test_to_dict_contains_all_required_keys(self, valid_claim):
        d = valid_claim.to_dict()
        required = {"claim_id", "member_id", "hospital_id", "insurer", "amount",
                    "city", "claim_type", "diagnosis_code", "submitted_at", "status"}
        assert required.issubset(d.keys())

    def test_two_claims_have_different_ids(self):
        c1 = ClaimSubmit(
            member_id="M1001", hospital_id="H001", hospital_name="H",
            insurer="Ins", amount=1000, city="mumbai",
            claim_type="cashless", diagnosis_code="A01",
        )
        c2 = ClaimSubmit(
            member_id="M1001", hospital_id="H001", hospital_name="H",
            insurer="Ins", amount=1000, city="mumbai",
            claim_type="cashless", diagnosis_code="A01",
        )
        assert c1.claim_id != c2.claim_id


# ── ClaimValidator tests ───────────────────────────────────────────────────────

class TestClaimValidator:
    def test_valid_claim_passes(self, valid_claim, validator):
        ok, errors = validator.validate(valid_claim)
        assert ok is True
        assert errors == []

    # ── member_id ──────────────────────────────────

    def test_member_id_must_start_with_M(self, valid_claim, validator):
        valid_claim.member_id = "X1001"
        ok, errors = validator.validate(valid_claim)
        assert ok is False
        assert any("member_id" in e for e in errors)

    def test_member_id_too_short(self, valid_claim, validator):
        valid_claim.member_id = "M1"
        ok, errors = validator.validate(valid_claim)
        assert ok is False

    def test_member_id_empty(self, valid_claim, validator):
        valid_claim.member_id = ""
        ok, errors = validator.validate(valid_claim)
        assert ok is False

    # ── amount ─────────────────────────────────────

    def test_amount_below_minimum_rejected(self, valid_claim, validator):
        valid_claim.amount = 50.0
        ok, errors = validator.validate(valid_claim)
        assert ok is False
        assert any("amount" in e for e in errors)

    def test_amount_above_maximum_rejected(self, valid_claim, validator):
        valid_claim.amount = 15_000_000.0
        ok, errors = validator.validate(valid_claim)
        assert ok is False

    def test_amount_at_minimum_accepted(self, valid_claim, validator):
        valid_claim.amount = 100.0
        ok, errors = validator.validate(valid_claim)
        assert ok is True

    def test_amount_at_maximum_accepted(self, valid_claim, validator):
        valid_claim.amount = 10_000_000.0
        ok, errors = validator.validate(valid_claim)
        assert ok is True

    # ── claim_type ─────────────────────────────────

    def test_invalid_claim_type_rejected(self, valid_claim, validator):
        valid_claim.claim_type = "emergency"
        ok, errors = validator.validate(valid_claim)
        assert ok is False
        assert any("claim_type" in e for e in errors)

    def test_reimbursement_accepted(self, valid_claim, validator):
        valid_claim.claim_type = "reimbursement"
        ok, errors = validator.validate(valid_claim)
        assert ok is True

    # ── hospital_id ────────────────────────────────

    def test_empty_hospital_id_rejected(self, valid_claim, validator):
        valid_claim.hospital_id = ""
        ok, errors = validator.validate(valid_claim)
        assert ok is False

    # ── diagnosis_code ─────────────────────────────

    def test_short_diagnosis_code_rejected(self, valid_claim, validator):
        valid_claim.diagnosis_code = "A"
        ok, errors = validator.validate(valid_claim)
        assert ok is False

    # ── multiple errors ────────────────────────────

    def test_multiple_errors_returned_at_once(self, validator):
        bad_claim = ClaimSubmit(
            member_id="X1",          # bad
            hospital_id="",          # bad
            hospital_name="H",
            insurer="",              # bad
            amount=50,               # bad
            city="mumbai",
            claim_type="wrong",      # bad
            diagnosis_code="AB",     # bad
        )
        ok, errors = validator.validate(bad_claim)
        assert ok is False
        assert len(errors) >= 5

    # ── deliberate bug test (Session 3 demo) ───────

    def test_wrong_amount_threshold_bug(self, validator):
        """
        Simulates the deliberate bug introduced in GitLab Session 3:
        a wrong threshold that would accept amounts above ₹1 crore.
        This test should FAIL if the bug is present.
        """
        claim = ClaimSubmit(
            member_id="M1001", hospital_id="H001", hospital_name="H",
            insurer="StarHealth", amount=12_000_000,  # ₹1.2 crore
            city="mumbai", claim_type="cashless", diagnosis_code="Z51.1",
        )
        ok, errors = validator.validate(claim)
        # Should be rejected — amount exceeds ₹1 crore cap
        assert ok is False, "BUG: amount above ₹1 crore was accepted"
