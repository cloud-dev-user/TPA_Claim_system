package com.mdindia.eligibility.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Result of the eligibility check for a claim.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EligibilityResult {

    private String claimId;
    private String memberId;
    private boolean eligible;
    private String reason;
    private Double remainingLimit;
    private String policyPlan;

    /**
     * Returns a map representation for Kafka publishing and auditing.
     * Includes a human-readable "decision" field.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("claimId", claimId);
        map.put("memberId", memberId);
        map.put("eligible", eligible);
        map.put("decision", eligible ? "ELIGIBLE" : "INELIGIBLE");
        map.put("reason", reason);
        map.put("remainingLimit", remainingLimit);
        map.put("policyPlan", policyPlan);
        return map;
    }
}
