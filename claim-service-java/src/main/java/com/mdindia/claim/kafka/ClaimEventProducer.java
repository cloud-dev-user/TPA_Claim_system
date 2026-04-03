package com.mdindia.claim.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdindia.claim.model.ClaimSubmit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Publishes claim events to Kafka.
 * Message key = insurer, value = full claim JSON.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ClaimEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.claim-topic}")
    private String claimTopic;

    /**
     * Publishes a ClaimSubmit event to the claim-events topic.
     * Key is the insurer name to enable partition-based routing by insurer.
     * Blocks up to 5 seconds waiting for Kafka broker acknowledgement.
     *
     * @param claim the submitted claim
     * @throws Exception if serialization fails or Kafka send times out / fails
     */
    public void publishClaimEvent(ClaimSubmit claim) throws Exception {
        String key = claim.getInsurer();
        String value = objectMapper.writeValueAsString(claim.toMap());

        SendResult<String, String> result = kafkaTemplate.send(claimTopic, key, value)
                .get(5, TimeUnit.SECONDS);

        log.info("Published claim event: claimId={}, insurer={}, topic={}, partition={}, offset={}",
                claim.getClaimId(),
                claim.getInsurer(),
                claimTopic,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }
}
