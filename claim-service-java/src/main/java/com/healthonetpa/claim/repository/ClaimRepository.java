package com.healthonetpa.claim.repository;

import com.healthonetpa.claim.model.ClaimEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ClaimRepository extends JpaRepository<ClaimEntity, String> {}
