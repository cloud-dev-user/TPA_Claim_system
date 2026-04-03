package com.mdindia.claim.model;

/**
 * Represents the lifecycle states of an insurance claim.
 */
public enum ClaimStatus {
    SUBMITTED,
    VALIDATING,
    ELIGIBLE,
    INELIGIBLE,
    APPROVED,
    REJECTED,
    FRAUD_REVIEW
}
