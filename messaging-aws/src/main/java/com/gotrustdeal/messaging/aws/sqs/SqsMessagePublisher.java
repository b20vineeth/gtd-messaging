package com.gotrustdeal.messaging.aws.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.MessagePublisher;
import com.gotrustdeal.messaging.aws.properties.AwsMessagingProperties;
import com.gotrustdeal.messaging.exception.DestinationResolutionException;
import com.gotrustdeal.messaging.exception.MessagingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AWS SQS Implementation of the MessagePublisher contract.
 * Resolves logical destinations to physical SQS queues and publishes JSON-serialized event envelopes.
 */
@Slf4j
@RequiredArgsConstructor
public class SqsMessagePublisher implements MessagePublisher {

    private final SqsClient sqsClient;
    private final AwsMessagingProperties properties;
    private final ObjectMapper objectMapper;
    private final Map<String, String> queueUrlCache = new ConcurrentHashMap<>();

    @Override
    public void publish(String destination, EventEnvelope<?> envelope) {
        if (destination == null || destination.trim().isEmpty()) {
            throw new IllegalArgumentException("Destination cannot be null or empty");
        }
        if (envelope == null) {
            throw new IllegalArgumentException("Event envelope cannot be null");
        }

        String queueUrl = resolveQueueUrl(destination);
        try {
            String jsonPayload = objectMapper.writeValueAsString(envelope);
            
            // Avoid logging sensitive payloads in production by using debug-only logging for payloads
            if (log.isDebugEnabled()) {
                log.debug("Publishing message to SQS queue [{}], payload: {}", queueUrl, jsonPayload);
            } else {
                log.info("Publishing message to SQS queue [{}], eventId: {}, eventType: {}, correlationId: {}",
                        queueUrl, envelope.eventId(), envelope.eventType(), envelope.correlationId());
            }

            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(jsonPayload)
                    .build();

            sqsClient.sendMessage(request);
        } catch (DestinationResolutionException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to publish message to SQS destination [{}]: {}", destination, e.getMessage(), e);
            throw new MessagingException("Failed to publish message to SQS queue: " + queueUrl, e);
        }
    }

    /**
     * Resolves the logical destination name to SQS Queue URL, utilizing a cache to minimize AWS API requests.
     */
    private String resolveQueueUrl(String destination) {
        return queueUrlCache.computeIfAbsent(destination, d -> {
            AwsMessagingProperties.DestinationProperties destProps = properties.getDestinations().get(d);
            if (destProps != null) {
                if (destProps.getQueueUrl() != null && !destProps.getQueueUrl().trim().isEmpty()) {
                    return destProps.getQueueUrl().trim();
                }
                if (destProps.getQueueName() != null && !destProps.getQueueName().trim().isEmpty()) {
                    return fetchQueueUrlFromSqs(destProps.getQueueName().trim());
                }
            }
            // Fallback: treat the destination parameter itself as the SQS queue name
            return fetchQueueUrlFromSqs(d.trim());
        });
    }

    private String fetchQueueUrlFromSqs(String queueName) {
        try {
            GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            return sqsClient.getQueueUrl(request).queueUrl();
        } catch (Exception e) {
            throw new DestinationResolutionException("Failed to resolve SQS queue URL for queue name: " + queueName, e);
        }
    }

    /**
     * Clears the resolved queue URL cache (e.g. for testing purposes).
     */
    public void clearCache() {
        queueUrlCache.clear();
    }
}
