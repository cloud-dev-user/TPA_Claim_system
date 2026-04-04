package com.healthonetpa.eligibility.service;

import com.healthonetpa.eligibility.model.EligibilityResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;

/**
 * Applies eligibility rules to determine whether a claim can be processed.
 *
 * Rules (in order):
 *  1. Cashless only: hospital must be empanelled in the claim city.
 *  2. Member policy must exist in Redis.
 *  3. Claim amount must not exceed (policy limit - amount already used).
 *  4. On approval, increment the used amount and refresh policy TTL.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EligibilityCheckService {

    private final StringRedisTemplate redisTemplate;

    @Value("${app.policy-ttl:900}")
    private long policyTtlSeconds;

    /**
     * Checks eligibility for a claim event payload.
     *
     * @param claimData map of claim fields parsed from the Kafka message
     * @return EligibilityResult with decision, reason, and remaining limit
     */
    public EligibilityResult checkEligibility(Map<String, Object> claimData) {
        String claimId = String.valueOf(claimData.get("claimId"));
        String memberId = String.valueOf(claimData.get("memberId"));
        String hospitalId = String.valueOf(claimData.get("hospitalId"));
        String hospitalName = String.valueOf(claimData.get("hospitalName"));
        String city = String.valueOf(claimData.get("city"));
        String claimType = String.valueOf(claimData.get("claimType"));
        double amount = parseDouble(claimData.get("amount"));

        log.info("Checking eligibility: claimId={}, memberId={}, claimType={}, amount={}", claimId, memberId, claimType, amount);

        // Step 1: Cashless — check hospital empanelment
        if ("cashless".equalsIgnoreCase(claimType)) {
            String empanelledKey = "empanelled:" + city;
            Boolean isEmpanelled = redisTemplate.opsForSet().isMember(empanelledKey, hospitalName);
            if (Boolean.FALSE.equals(isEmpanelled)) {
                log.warn("Hospital not empanelled: claimId={}, hospital={}, city={}", claimId, hospitalName, city);
                return new EligibilityResult(claimId, memberId, false,
                        "Hospital '" + hospitalName + "' is not empanelled in city '" + city + "'",
                        null, null);
            }
        }

        // Step 2: Fetch member policy from Redis
        String policyKey = "member:" + memberId + ":policy";
        Map<Object, Object> policy = redisTemplate.opsForHash().entries(policyKey);
        if (policy == null || policy.isEmpty()) {
            log.warn("No policy found for member: claimId={}, memberId={}", claimId, memberId);
            return new EligibilityResult(claimId, memberId, false,
                    "No active policy found for member '" + memberId + "'",
                    null, null);
        }

        double limit = parseDouble(policy.get("limit"));
        double used = parseDouble(policy.get("used"));
        String policyPlan = policy.containsKey("plan") ? String.valueOf(policy.get("plan")) : "UNKNOWN";
        double available = limit - used;

        // Step 3: Check if amount exceeds available limit
        if (amount > available) {
            log.warn("Insufficient limit: claimId={}, memberId={}, required={}, available={}", claimId, memberId, amount, available);
            return new EligibilityResult(claimId, memberId, false,
                    String.format("Claim amount %.2f exceeds available limit %.2f", amount, available),
                    available, policyPlan);
        }

        // Step 4: Deduct amount and refresh TTL
        redisTemplate.opsForHash().increment(policyKey, "used", amount);
        redisTemplate.expire(policyKey, Duration.ofSeconds(policyTtlSeconds));

        double remainingAfter = available - amount;
        log.info("Eligibility approved: claimId={}, memberId={}, remainingLimit={}", claimId, memberId, remainingAfter);

        return new EligibilityResult(claimId, memberId, true,
                "Claim is eligible for processing",
                remainingAfter, policyPlan);
    }

    private double parseDouble(Object value) {
        if (value == null) return 0.0;
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
