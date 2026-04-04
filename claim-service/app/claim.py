"""
Claim domain models — ClaimSubmit and ClaimValidator.
Used by both the REST API and the Kafka producer.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


# ──────────────────────────────────────────────────────────────────────────────
# Enumerations
# ──────────────────────────────────────────────────────────────────────────────

class ClaimStatus(str, Enum):
    SUBMITTED    = "SUBMITTED"
    VALIDATING   = "VALIDATING"
    ELIGIBLE     = "ELIGIBLE"
    INELIGIBLE   = "INELIGIBLE"
    APPROVED     = "APPROVED"
    REJECTED     = "REJECTED"
    FRAUD_REVIEW = "FRAUD_REVIEW"


class ClaimType(str, Enum):
    CASHLESS       = "cashless"
    REIMBURSEMENT  = "reimbursement"


# ──────────────────────────────────────────────────────────────────────────────
# ClaimSubmit — the data object that travels through the pipeline
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ClaimSubmit:
    member_id:      str
    hospital_id:    str
    hospital_name:  str
    insurer:        str
    amount:         float
    city:           str
    claim_type:     str
    diagnosis_code: str

    # Auto-generated fields
    claim_id:     str         = field(default_factory=lambda: f"C{uuid.uuid4().hex[:8].upper()}")
    submitted_at: str         = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status:       ClaimStatus = field(default=ClaimStatus.SUBMITTED)

    # Optional fields
    admission_date:   Optional[str] = None
    discharge_date:   Optional[str] = None
    pre_auth_number:  Optional[str] = None
    remarks:          Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "claim_id":       self.claim_id,
            "member_id":      self.member_id,
            "hospital_id":    self.hospital_id,
            "hospital_name":  self.hospital_name,
            "insurer":        self.insurer,
            "amount":         self.amount,
            "city":           self.city,
            "claim_type":     self.claim_type,
            "diagnosis_code": self.diagnosis_code,
            "submitted_at":   self.submitted_at,
            "status":         self.status.value,
            "admission_date": self.admission_date,
            "discharge_date": self.discharge_date,
            "pre_auth_number": self.pre_auth_number,
            "remarks":        self.remarks,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ClaimSubmit":
        return cls(
            member_id=data["member_id"],
            hospital_id=data["hospital_id"],
            hospital_name=data["hospital_name"],
            insurer=data["insurer"],
            amount=float(data["amount"]),
            city=data["city"],
            claim_type=data["claim_type"],
            diagnosis_code=data["diagnosis_code"],
            claim_id=data.get("claim_id", f"C{uuid.uuid4().hex[:8].upper()}"),
            submitted_at=data.get("submitted_at", datetime.now(timezone.utc).isoformat()),
            status=ClaimStatus(data.get("status", ClaimStatus.SUBMITTED.value)),
            admission_date=data.get("admission_date"),
            discharge_date=data.get("discharge_date"),
            pre_auth_number=data.get("pre_auth_number"),
            remarks=data.get("remarks"),
        )


# ──────────────────────────────────────────────────────────────────────────────
# ClaimValidator — synchronous validation rules applied before publishing
# ──────────────────────────────────────────────────────────────────────────────

class ClaimValidator:
    """Validates a ClaimSubmit before it enters the pipeline.

    Returns (is_valid: bool, errors: list[str]).
    Deliberately raises NO exceptions — callers check the bool.
    """

    # HealthOne TPA business rules
    MAX_AMOUNT: float = 10_000_000   # ₹1 crore
    MIN_AMOUNT: float = 100          # ₹100
    VALID_CLAIM_TYPES = frozenset(ct.value for ct in ClaimType)

    def validate(self, claim: ClaimSubmit) -> tuple[bool, list[str]]:
        errors: list[str] = []

        # Member ID format: starts with 'M', at least 5 chars
        if not claim.member_id or not claim.member_id.startswith("M") or len(claim.member_id) < 5:
            errors.append("member_id must start with 'M' and be at least 5 characters")

        # Hospital ID required
        if not claim.hospital_id or len(claim.hospital_id.strip()) == 0:
            errors.append("hospital_id is required")

        # Hospital name required
        if not claim.hospital_name or len(claim.hospital_name.strip()) == 0:
            errors.append("hospital_name is required")

        # Amount range
        if not (self.MIN_AMOUNT <= claim.amount <= self.MAX_AMOUNT):
            errors.append(
                f"amount must be between ₹{self.MIN_AMOUNT:,.0f} and ₹{self.MAX_AMOUNT:,.0f}"
            )

        # Claim type
        if claim.claim_type not in self.VALID_CLAIM_TYPES:
            errors.append(f"claim_type must be one of {sorted(self.VALID_CLAIM_TYPES)}")

        # Insurer
        if not claim.insurer or len(claim.insurer.strip()) == 0:
            errors.append("insurer is required")

        # City
        if not claim.city or len(claim.city.strip()) == 0:
            errors.append("city is required")

        # Diagnosis code — ICD-10 style: letter + digits
        if not claim.diagnosis_code or len(claim.diagnosis_code) < 3:
            errors.append("diagnosis_code must be at least 3 characters (ICD-10 format)")

        return len(errors) == 0, errors
