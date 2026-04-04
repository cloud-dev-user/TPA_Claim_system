package com.healthonetpa.eligibility.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.healthonetpa.eligibility.model.EligibilityResult;
import com.healthonetpa.eligibility.service.EligibilityCheckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * Kafka consumer for claim events.
 * Performs eligibility checks and publishes results to eligibility-results and audit-log topics.
 * Uses manual offset commit (MANUAL_IMMEDIATE) to ensure at-least-once processing.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EligibilityConsumer {

    private final EligibilityCheckService eligibilityCheckService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.output-topic}")
    private String outputTopic;

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
            log.debug("Received claim event: partition={}, offset={}, key={}",
                    record.partition(), record.offset(), record.key());

            // Parse the incoming claim JSON
            Map<String, Object> claimData = objectMapper.readValue(
                    record.value(), new TypeReference<Map<String, Object>>() {});

            claimId = String.valueOf(claimData.getOrDefault("claimId", "UNKNOWN"));

            // Perform eligibility check
            EligibilityResult result = eligibilityCheckService.checkEligibility(claimData);

            // Publish eligibility result
            String resultJson = objectMapper.writeValueAsString(result.toMap());
            kafkaTemplate.send(outputTopic, result.getClaimId(), resultJson);
            log.info("Published eligibility result: claimId={}, decision={}",
                    result.getClaimId(), result.isEligible() ? "ELIGIBLE" : "INELIGIBLE");

            // Publish audit log entry
            publishAudit(claimId, result);

            // Manual offset commit after successful processing
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing eligibility for claimId={}: {}", claimId, e.getMessage(), e);
            // Commit offset anyway to avoid poison-pill loop; consider DLQ in production
            acknowledgment.acknowledge();
        }
    }

    private void publishAudit(String claimId, EligibilityResult result) {
        try {
            Map<String, Object> auditEntry = Map.of(
                    "eventType", "ELIGIBILITY_CHECK",
                    "claimId", claimId,
                    "memberId", result.getMemberId() != null ? result.getMemberId() : "",
                    "decision", result.isEligible() ? "ELIGIBLE" : "INELIGIBLE",
                    "reason", result.getReason() != null ? result.getReason() : "",
                    "timestamp", Instant.now().toString()
            );
            String auditJson = objectMapper.writeValueAsString(auditEntry);
            kafkaTemplate.send(auditTopic, claimId, auditJson);
            log.debug("Published audit entry: claimId={}", claimId);
        } catch (Exception e) {
            log.error("Failed to publish audit for claimId={}: {}", claimId, e.getMessage());
        }
    }
}
