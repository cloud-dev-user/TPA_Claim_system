package com.healthonetpa.claim.controller;

import com.healthonetpa.claim.kafka.ClaimEventProducer;
import com.healthonetpa.claim.model.ClaimEntity;
import com.healthonetpa.claim.model.ClaimRequest;
import com.healthonetpa.claim.model.ClaimResponse;
import com.healthonetpa.claim.model.ClaimSubmit;
import com.healthonetpa.claim.repository.ClaimRepository;
import com.healthonetpa.claim.service.ClaimValidator;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;

/**
 * REST controller for claim submission and status retrieval.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class ClaimController {

    private static final String REDIS_STATUS_PREFIX = "claim:";
    private static final String REDIS_STATUS_SUFFIX = ":status";
    private static final long CLAIM_TTL_SECONDS = 86400L;

    private final StringRedisTemplate redisTemplate;
    private final ClaimValidator claimValidator;
    private final ClaimEventProducer claimEventProducer;
    private final ClaimRepository claimRepository;

    /**
     * POST /api/v1/claims
     * Validates, stores in Redis, saves to DB, and publishes to Kafka.
     * Returns HTTP 202 Accepted on success.
     */
    @PostMapping("/api/v1/claims")
    public ResponseEntity<?> submitClaim(@Valid @RequestBody ClaimRequest request) {

        // Business validation
        ClaimValidator.ValidationResult validation = claimValidator.validate(request);
        if (!validation.valid()) {
            log.warn("Claim validation failed: {}", validation.errors());
            return ResponseEntity
                    .status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(Map.of(
                            "error", "Validation failed",
                            "details", validation.errors()
                    ));
        }

        // Build the submitted claim (generates claimId, timestamps, status)
        ClaimSubmit claim = new ClaimSubmit(request);

        // Store status in Redis with TTL
        String redisKey = REDIS_STATUS_PREFIX + claim.getClaimId() + REDIS_STATUS_SUFFIX;
        redisTemplate.opsForValue().set(redisKey, claim.getStatus(), Duration.ofSeconds(CLAIM_TTL_SECONDS));
        log.info("Stored claim status in Redis: key={}, status={}", redisKey, claim.getStatus());

        // Persist claim to DB
        claimRepository.save(new ClaimEntity(claim));

        // Publish to Kafka
        try {
            claimEventProducer.publishClaimEvent(claim);
        } catch (Exception e) {
            log.error("Failed to publish claim event for claimId={}: {}", claim.getClaimId(), e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to publish claim event"));
        }

        ClaimResponse response = new ClaimResponse(
                claim.getClaimId(),
                claim.getStatus(),
                claim.getMemberId(),
                claim.getInsurer(),
                claim.getAmount(),
                claim.getSubmittedAt(),
                "Claim submitted successfully"
        );

        log.info("Claim accepted: claimId={}, memberId={}, insurer={}, amount={}",
                claim.getClaimId(), claim.getMemberId(), claim.getInsurer(), claim.getAmount());

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * GET /api/v1/claims/{claimId}
     * Returns the current status of a claim from Redis.
     */
    @GetMapping("/api/v1/claims/{claimId}")
    public ResponseEntity<?> getClaimStatus(@PathVariable String claimId) {
        String redisKey = REDIS_STATUS_PREFIX + claimId + REDIS_STATUS_SUFFIX;
        String status = redisTemplate.opsForValue().get(redisKey);

        if (status == null) {
            log.warn("Claim not found in Redis: claimId={}", claimId);
            return ResponseEntity
                    .status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Claim not found", "claimId", claimId));
        }

        log.debug("Fetched claim status from Redis: claimId={}, status={}", claimId, status);
        return ResponseEntity.ok(Map.of(
                "claimId", claimId,
                "status", status
        ));
    }

    /**
     * GET /health
     * Liveness probe.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "claim-service"));
    }

    /**
     * GET /ready
     * Readiness probe — verifies Redis connectivity.
     */
    @GetMapping("/ready")
    public ResponseEntity<Map<String, String>> ready() {
        try {
            String pong = redisTemplate.execute((RedisCallback<String>) conn -> conn.ping());
            if ("PONG".equalsIgnoreCase(pong)) {
                return ResponseEntity.ok(Map.of("status", "READY", "redis", "connected"));
            }
            return ResponseEntity
                    .status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("status", "NOT_READY", "redis", "unexpected response"));
        } catch (Exception e) {
            log.error("Redis readiness check failed: {}", e.getMessage());
            return ResponseEntity
                    .status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("status", "NOT_READY", "redis", "unreachable"));
        }
    }
}
