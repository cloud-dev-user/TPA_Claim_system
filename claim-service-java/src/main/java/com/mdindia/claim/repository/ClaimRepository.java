package com.mdindia.claim.repository;

import com.mdindia.claim.model.ClaimEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ClaimRepository extends JpaRepository<ClaimEntity, String> {}
