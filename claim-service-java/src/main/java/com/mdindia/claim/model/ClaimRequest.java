package com.mdindia.claim.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;

/**
 * Incoming claim submission request payload.
 */
@Data
public class ClaimRequest {

    @NotBlank(message = "memberId is required")
    private String memberId;

    @NotBlank(message = "hospitalId is required")
    private String hospitalId;

    @NotBlank(message = "hospitalName is required")
    private String hospitalName;

    @NotBlank(message = "insurer is required")
    private String insurer;

    @NotNull(message = "amount is required")
    @Positive(message = "amount must be greater than 0")
    private Double amount;

    @NotBlank(message = "city is required")
    private String city;

    @NotNull(message = "claimType is required")
    private ClaimType claimType;

    @NotBlank(message = "diagnosisCode is required")
    private String diagnosisCode;

    // Optional fields
    private String admissionDate;
    private String dischargeDate;
    private String preAuthNumber;
    private String remarks;
}
