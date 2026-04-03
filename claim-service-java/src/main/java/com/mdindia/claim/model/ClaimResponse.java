package com.mdindia.claim.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response returned to the client after claim submission or status lookup.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClaimResponse {

    private String claimId;
    private String status;
    private String memberId;
    private String insurer;
    private Double amount;
    private String submittedAt;
    private String message;
}
