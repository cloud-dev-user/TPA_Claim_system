package com.mdindia.fraud.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of the fraud rule evaluation for a claim.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FraudAssessment {

    private String claimId;
    private String memberId;
    private String hospitalId;
    private double riskScore;
    private boolean isFlagged;
    private List<String> reasons = new ArrayList<>();
    private Double hospitalScore;

    /**
     * Returns a map representation for Kafka publishing.
     * Includes a "verdict" field: FRAUD_REVIEW when flagged, CLEAR otherwise.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("claimId", claimId);
        map.put("memberId", memberId);
        map.put("hospitalId", hospitalId);
        map.put("riskScore", riskScore);
        map.put("isFlagged", isFlagged);
        map.put("verdict", isFlagged ? "FRAUD_REVIEW" : "CLEAR");
        map.put("reasons", reasons);
        map.put("hospitalScore", hospitalScore);
        return map;
    }
}
