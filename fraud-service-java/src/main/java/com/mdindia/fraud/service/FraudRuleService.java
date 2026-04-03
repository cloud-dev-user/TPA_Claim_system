package com.mdindia.fraud.service;

import com.mdindia.fraud.model.FraudAssessment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Applies a set of fraud detection rules to a claim event.
 *
 * Rule 1: High-amount flag — amount >= highAmountThreshold adds 20 to risk score.
 * Rule 2: Round-amount flag — amount is a multiple of 100,000 and >= 100,000 adds 5 to risk score.
 * Rule 3: Daily claim volume — member has submitted > dailyClaimLimit claims today adds 30 to risk score.
 * Rule 4: Hospital score update — hospital's cumulative fraud score is updated in a Redis sorted set.
 *
 * If total risk score >= scoreThreshold the claim is flagged for FRAUD_REVIEW.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FraudRuleService {

    private final StringRedisTemplate redisTemplate;

    @Value("${app.fraud.score-threshold:50}")
    private double scoreThreshold;

    @Value("${app.fraud.high-amount-threshold:500000}")
    private double highAmountThreshold;

    @Value("${app.fraud.daily-claim-limit:5}")
    private long dailyClaimLimit;

    private static final long DAILY_EXPIRY_SECONDS = 86400L;

    /**
     * Evaluates fraud rules for the given claim data.
     *
     * @param claimData map of claim fields parsed from Kafka message
     * @return FraudAssessment with risk score, flag status, reasons, and hospital score
     */
    public FraudAssessment assess(Map<String, Object> claimData) {
        String claimId = String.valueOf(claimData.get("claimId"));
        String memberId = String.valueOf(claimData.get("memberId"));
        String hospitalId = String.valueOf(claimData.get("hospitalId"));
        double amount = parseDouble(claimData.get("amount"));

        log.info("Assessing fraud: claimId={}, memberId={}, hospitalId={}, amount={}", claimId, memberId, hospitalId, amount);

        double score = 0.0;
        List<String> reasons = new ArrayList<>();

        // Rule 1: High amount threshold
        if (amount >= highAmountThreshold) {
            score += 20;
            reasons.add(String.format("High claim amount: %.2f >= %.0f", amount, highAmountThreshold));
            log.debug("Rule 1 triggered: high amount {} for claimId={}", amount, claimId);
        }

        // Rule 2: Suspiciously round amount (multiple of 100,000 and >= 100,000)
        if (amount >= 100_000 && amount % 100_000 == 0) {
            score += 5;
            reasons.add(String.format("Suspiciously round amount: %.0f", amount));
            log.debug("Rule 2 triggered: round amount {} for claimId={}", amount, claimId);
        }

        // Rule 3: Daily claim volume per member
        String today = LocalDate.now().toString(); // e.g. "2026-04-03"
        String dailyCountKey = "fraud:member:" + memberId + ":claims:" + today;
        Long dailyCount = redisTemplate.opsForValue().increment(dailyCountKey);
        redisTemplate.expire(dailyCountKey, Duration.ofSeconds(DAILY_EXPIRY_SECONDS));

        if (dailyCount != null && dailyCount > dailyClaimLimit) {
            score += 30;
            reasons.add(String.format("Excessive daily claims: %d today (limit: %d)", dailyCount, dailyClaimLimit));
            log.warn("Rule 3 triggered: member {} has {} claims today (limit {})", memberId, dailyCount, dailyClaimLimit);
        }

        // Rule 4: Update hospital fraud score in sorted set
        // base score = 0 initially; each assessment contributes (baseScore + score * 0.5)
        double scoreIncrement = score * 0.5;
        Double hospitalScore = redisTemplate.opsForZSet()
                .incrementScore("fraud:hospital:scores", hospitalId, scoreIncrement);
        log.debug("Updated hospital fraud score: hospitalId={}, increment={}, newScore={}", hospitalId, scoreIncrement, hospitalScore);

        boolean flagged = score >= scoreThreshold;
        if (flagged) {
            log.warn("Claim FLAGGED for fraud: claimId={}, riskScore={}, reasons={}", claimId, score, reasons);
        } else {
            log.info("Claim cleared: claimId={}, riskScore={}", claimId, score);
        }

        FraudAssessment assessment = new FraudAssessment();
        assessment.setClaimId(claimId);
        assessment.setMemberId(memberId);
        assessment.setHospitalId(hospitalId);
        assessment.setRiskScore(score);
        assessment.setFlagged(flagged);
        assessment.setReasons(reasons);
        assessment.setHospitalScore(hospitalScore);
        return assessment;
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
