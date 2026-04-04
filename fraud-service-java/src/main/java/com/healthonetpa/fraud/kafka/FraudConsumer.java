package com.healthonetpa.fraud.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.healthonetpa.fraud.model.FraudAssessment;
import com.healthonetpa.fraud.service.FraudRuleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * Kafka consumer for claim events that applies fraud detection rules.
 * Publishes to fraud-alerts topic when flagged, and always publishes to audit-log.
 * Uses MANUAL_IMMEDIATE acknowledgement for reliable at-least-once processing.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudConsumer {

    private final FraudRuleService fraudRuleService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.output-topic}")
    private String fraudAlertTopic;

    @Value("${app.kafka.audit-topic}")
    private String auditTopic;

    @KafkaListener(
            topics = "${app.kafka.input-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String claimId = "UNKNOWN";
        try {
            log.debug("Received claim event for fraud check: partition={}, offset={}, key={}",
                    record.partition(), record.offset(), record.key());

            // Parse JSON payload
            Map<String, Object> claimData = objectMapper.readValue(
                    record.value(), new TypeReference<Map<String, Object>>() {});

            claimId = String.valueOf(claimData.getOrDefault("claimId", "UNKNOWN"));

            // Apply fraud rules
            FraudAssessment assessment = fraudRuleService.assess(claimData);

            // Publish to fraud-alerts topic if score threshold exceeded
            if (assessment.isFlagged()) {
                String alertJson = objectMapper.writeValueAsString(assessment.toMap());
                kafkaTemplate.send(fraudAlertTopic, assessment.getClaimId(), alertJson);
                log.warn("Fraud alert published: claimId={}, riskScore={}, reasons={}",
                        assessment.getClaimId(), assessment.getRiskScore(), assessment.getReasons());
            }

            // Always publish audit entry
            publishAudit(claimId, assessment);

            // Manual offset commit
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing fraud assessment for claimId={}: {}", claimId, e.getMessage(), e);
            // Commit offset to avoid poison-pill loop; consider DLQ in production
            acknowledgment.acknowledge();
        }
    }

    private void publishAudit(String claimId, FraudAssessment assessment) {
        try {
            Map<String, Object> auditEntry = Map.of(
                    "eventType", "FRAUD_ASSESSMENT",
                    "claimId", claimId,
                    "memberId", assessment.getMemberId() != null ? assessment.getMemberId() : "",
                    "hospitalId", assessment.getHospitalId() != null ? assessment.getHospitalId() : "",
                    "verdict", assessment.isFlagged() ? "FRAUD_REVIEW" : "CLEAR",
                    "riskScore", assessment.getRiskScore(),
                    "timestamp", Instant.now().toString()
            );
            String auditJson = objectMapper.writeValueAsString(auditEntry);
            kafkaTemplate.send(auditTopic, claimId, auditJson);
            log.debug("Published fraud audit entry: claimId={}", claimId);
        } catch (Exception e) {
            log.error("Failed to publish fraud audit for claimId={}: {}", claimId, e.getMessage());
        }
    }
}
