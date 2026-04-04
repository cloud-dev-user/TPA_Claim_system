package com.healthonetpa.claim.model;

import lombok.Getter;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a fully populated claim after submission.
 * Auto-generates claimId, submittedAt, and initial status on construction.
 */
@Getter
public class ClaimSubmit {

    private final String claimId;
    private final String memberId;
    private final String hospitalId;
    private final String hospitalName;
    private final String insurer;
    private final double amount;
    private final String city;
    private final String claimType;
    private final String diagnosisCode;
    private final String admissionDate;
    private final String dischargeDate;
    private final String preAuthNumber;
    private final String remarks;
    private final String submittedAt;
    private final String status;

    public ClaimSubmit(ClaimRequest request) {
        // Generate claimId: "C" + first 8 hex chars of UUID, uppercase
        String uuid = UUID.randomUUID().toString().replace("-", "").toUpperCase();
        this.claimId = "C" + uuid.substring(0, 8);

        this.memberId = request.getMemberId();
        this.hospitalId = request.getHospitalId();
        this.hospitalName = request.getHospitalName();
        this.insurer = request.getInsurer();
        this.amount = request.getAmount();
        this.city = request.getCity();
        this.claimType = request.getClaimType() != null ? request.getClaimType().getValue() : null;
        this.diagnosisCode = request.getDiagnosisCode();
        this.admissionDate = request.getAdmissionDate();
        this.dischargeDate = request.getDischargeDate();
        this.preAuthNumber = request.getPreAuthNumber();
        this.remarks = request.getRemarks();
        this.submittedAt = Instant.now().toString();
        this.status = ClaimStatus.SUBMITTED.name();
    }

    /**
     * Returns all fields as an ordered map for Redis HMSET or JSON serialization.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("claimId", claimId);
        map.put("memberId", memberId);
        map.put("hospitalId", hospitalId);
        map.put("hospitalName", hospitalName);
        map.put("insurer", insurer);
        map.put("amount", amount);
        map.put("city", city);
        map.put("claimType", claimType);
        map.put("diagnosisCode", diagnosisCode);
        map.put("admissionDate", admissionDate);
        map.put("dischargeDate", dischargeDate);
        map.put("preAuthNumber", preAuthNumber);
        map.put("remarks", remarks);
        map.put("submittedAt", submittedAt);
        map.put("status", status);
        return map;
    }
}
