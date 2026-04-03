package com.mdindia.claim.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Type of insurance claim settlement mechanism.
 */
public enum ClaimType {

    CASHLESS("cashless"),
    REIMBURSEMENT("reimbursement");

    private final String value;

    ClaimType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static ClaimType fromValue(String value) {
        for (ClaimType type : ClaimType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown claim type: " + value);
    }

    @Override
    public String toString() {
        return value;
    }
}
