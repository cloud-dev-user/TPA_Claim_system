package com.healthonetpa.claim.service;

import com.healthonetpa.claim.model.ClaimRequest;
import com.healthonetpa.claim.model.ClaimType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Applies business validation rules to a ClaimRequest.
 */
@Slf4j
@Service
public class ClaimValidator {

    private static final double MIN_AMOUNT = 100.0;
    private static final double MAX_AMOUNT = 10_000_000.0;

    /**
     * Validates a claim request against all business rules.
     *
     * @param request the incoming claim request
     * @return a ValidationResult indicating pass/fail and any error messages
     */
    public ValidationResult validate(ClaimRequest request) {
        List<String> errors = new ArrayList<>();

        // Rule: memberId must start with 'M' and have length >= 5
        String memberId = request.getMemberId();
        if (memberId == null || !memberId.startsWith("M") || memberId.length() < 5) {
            errors.add("memberId must start with 'M' and be at least 5 characters long");
        }

        // Rule: hospitalId must not be blank
        if (isBlank(request.getHospitalId())) {
            errors.add("hospitalId must not be blank");
        }

        // Rule: hospitalName must not be blank
        if (isBlank(request.getHospitalName())) {
            errors.add("hospitalName must not be blank");
        }

        // Rule: insurer must not be blank
        if (isBlank(request.getInsurer())) {
            errors.add("insurer must not be blank");
        }

        // Rule: city must not be blank
        if (isBlank(request.getCity())) {
            errors.add("city must not be blank");
        }

        // Rule: amount must be between 100 and 10,000,000
        if (request.getAmount() == null) {
            errors.add("amount is required");
        } else {
            double amount = request.getAmount();
            if (amount < MIN_AMOUNT || amount > MAX_AMOUNT) {
                errors.add(String.format("amount must be between %.0f and %.0f", MIN_AMOUNT, MAX_AMOUNT));
            }
        }

        // Rule: claimType must be cashless or reimbursement
        if (request.getClaimType() == null) {
            errors.add("claimType must be one of: cashless, reimbursement");
        }

        // Rule: diagnosisCode length must be >= 3
        String diagnosisCode = request.getDiagnosisCode();
        if (diagnosisCode == null || diagnosisCode.trim().length() < 3) {
            errors.add("diagnosisCode must be at least 3 characters long");
        }

        boolean valid = errors.isEmpty();
        if (!valid) {
            log.warn("Claim validation failed with {} error(s): {}", errors.size(), errors);
        }

        return new ValidationResult(valid, errors);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * Encapsulates the result of claim validation.
     */
    public record ValidationResult(boolean valid, List<String> errors) {
    }
}
