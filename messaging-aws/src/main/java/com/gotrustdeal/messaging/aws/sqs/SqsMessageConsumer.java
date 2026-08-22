package com.gotrustdeal.messaging.aws.sqs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.aws.properties.AwsMessagingProperties;
import com.gotrustdeal.messaging.dispatcher.MessageDispatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Poll-based AWS SQS Consumer that polls configured queues, deserializes messages,
 * and routes them to MessageDispatcher.
 */
@Slf4j
@RequiredArgsConstructor
public class SqsMessageConsumer {

    private final SqsClient sqsClient;
    private final MessageDispatcher messageDispatcher;
    private final AwsMessagingProperties properties;
    private final ObjectMapper objectMapper;
    private final Map<String, String> queueUrlCache = new ConcurrentHashMap<>();
    private final Map<String, Long> lastErrorLogTime = new ConcurrentHashMap<>();
    private final Map<String, String> lastErrorMessage = new ConcurrentHashMap<>();
    private final Map<String, Long> nextAllowedPollTime = new ConcurrentHashMap<>();
    private final Map<String, Integer> consecutiveFailures = new ConcurrentHashMap<>();

    @Scheduled(fixedDelayString = "${gtd.messaging.consumer.poll-delay-ms:2000}")
    public void poll() {
        if (!properties.getConsumer().isEnabled()) {
            return;
        }

        // Poll all configured logical destinations that are not SNS topics
        for (Map.Entry<String, AwsMessagingProperties.DestinationProperties> entry : properties.getDestinations().entrySet()) {
            String destination = entry.getKey();
            AwsMessagingProperties.DestinationProperties destProps = entry.getValue();
            if (destProps != null && destProps.getTopicArn() != null && !destProps.getTopicArn().trim().isEmpty()) {
                continue;
            }
            if (destProps == null || !destProps.isConsumeEnabled()) {
                continue;
            }

            long now = System.currentTimeMillis();
            Long nextAllowed = nextAllowedPollTime.get(destination);
            if (nextAllowed != null && now < nextAllowed) {
                continue;
            }

            try {
                String queueUrl = resolveQueueUrl(destination);
                pollQueue(destination, queueUrl);

                // Polling succeeded
                if (consecutiveFailures.getOrDefault(destination, 0) > 0) {
                    log.info("SQS polling recovered for destination [{}]", destination);
                }
                consecutiveFailures.remove(destination);
                nextAllowedPollTime.remove(destination);
                lastErrorLogTime.remove(destination);
                lastErrorMessage.remove(destination);
            } catch (Exception e) {
                handleInfrastructureFailure(destination, e);
            }
        }
    }

    private void handleInfrastructureFailure(String destination, Exception e) {
        long now = System.currentTimeMillis();
        int failures = consecutiveFailures.getOrDefault(destination, 0) + 1;
        consecutiveFailures.put(destination, failures);

        // Exponential backoff: 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, max 300s (5 minutes)
        long backoffMs = Math.min(300, (long) Math.pow(2, failures)) * 1000;
        nextAllowedPollTime.put(destination, now + backoffMs);

        logPollError(destination, e, failures, backoffMs);
    }

    private void logPollError(String destination, Exception e, int failures, long backoffMs) {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
        long now = System.currentTimeMillis();
        Long lastTime = lastErrorLogTime.get(destination);
        String lastMsg = lastErrorMessage.get(destination);

        if (lastTime == null || !msg.equals(lastMsg) || (now - lastTime) > 60000) {
            log.error("Error polling SQS queue for destination [{}] (failure #{}): {}. Backing off for {}ms",
                    destination, failures, msg, backoffMs, e);
            lastErrorLogTime.put(destination, now);
            lastErrorMessage.put(destination, msg);
        } else {
            log.error("Error polling SQS queue for destination [{}]: {} (Stack trace suppressed, failure #{}, backing off for {}ms)",
                    destination, msg, failures, backoffMs);
        }
    }

    private void pollQueue(String destination, String queueUrl) {
        log.debug("Polling SQS queue [{}]", queueUrl);
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(properties.getConsumer().getMaxNumberOfMessages())
                .waitTimeSeconds(properties.getConsumer().getWaitTimeSeconds())
                .build();

        try {
            var messages = sqsClient.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
                processMessage(queueUrl, message);
            }
        } catch (Exception e) {
            log.error("Failed to receive messages from SQS queue [{}]: {}", queueUrl, e.getMessage(), e);
        }
    }

    private void processMessage(String queueUrl, Message message) {
        try {
            log.info("Received message from SQS queue [{}], messageId: {}", queueUrl, message.messageId());
            
            // Deserialize envelope
            EventEnvelope<?> envelope = objectMapper.readValue(
                    message.body(), 
                    new TypeReference<EventEnvelope<Object>>() {}
            );

            // Route to dispatcher
            messageDispatcher.dispatch(envelope);

            // Acknowledge (delete) on successful processing
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
            log.info("Successfully processed and deleted messageId: {}", message.messageId());

        } catch (Exception e) {
            log.error("Failed to process message from SQS queue [{}], messageId: {}: {}", 
                    queueUrl, message.messageId(), e.getMessage(), e);
            // SQS visibility timeout will trigger redelivery according to queue configuration.
        }
    }

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
            return fetchQueueUrlFromSqs(d.trim());
        });
    }

    private String fetchQueueUrlFromSqs(String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqsClient.getQueueUrl(request).queueUrl();
    }
}
