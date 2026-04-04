package com.healthonetpa.claim.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "claims")
@Getter
@NoArgsConstructor
public class ClaimEntity {

    @Id
    @Column(name = "claim_id", nullable = false, length = 16)
    private String claimId;

    @Column(name = "member_id",     nullable = false, length = 20)  private String memberId;
    @Column(name = "hospital_id",   nullable = false, length = 50)  private String hospitalId;
    @Column(name = "hospital_name", nullable = false, length = 200) private String hospitalName;
    @Column(name = "insurer",       nullable = false, length = 100) private String insurer;
    @Column(name = "amount",        nullable = false)               private double amount;
    @Column(name = "city",          nullable = false, length = 100) private String city;
    @Column(name = "claim_type",    nullable = false, length = 20)  private String claimType;
    @Column(name = "diagnosis_code",nullable = false, length = 20)  private String diagnosisCode;
    @Column(name = "status",        nullable = false, length = 20)  private String status;
    @Column(name = "submitted_at",  nullable = false)               private Instant submittedAt;

    @Column(name = "admission_date",  length = 20)  private String admissionDate;
    @Column(name = "discharge_date",  length = 20)  private String dischargeDate;
    @Column(name = "pre_auth_number", length = 50)  private String preAuthNumber;
    @Column(name = "remarks",         length = 500) private String remarks;

    public ClaimEntity(ClaimSubmit claim) {
        this.claimId      = claim.getClaimId();
        this.memberId     = claim.getMemberId();
        this.hospitalId   = claim.getHospitalId();
        this.hospitalName = claim.getHospitalName();
        this.insurer      = claim.getInsurer();
        this.amount       = claim.getAmount();
        this.city         = claim.getCity();
        this.claimType    = claim.getClaimType();
        this.diagnosisCode= claim.getDiagnosisCode();
        this.status       = claim.getStatus();
        this.submittedAt  = Instant.parse(claim.getSubmittedAt());
        this.admissionDate  = claim.getAdmissionDate();
        this.dischargeDate  = claim.getDischargeDate();
        this.preAuthNumber  = claim.getPreAuthNumber();
        this.remarks        = claim.getRemarks();
    }
}
