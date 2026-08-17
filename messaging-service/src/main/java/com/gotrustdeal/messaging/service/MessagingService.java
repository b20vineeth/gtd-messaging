package com.gotrustdeal.messaging.service;

import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.MessagePublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service that orchestrates common messaging operations.
 * Delegates actual publishing to the configured transport implementation.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessagingService {

    private final MessagePublisher messagePublisher;

    /**
     * Publishes a message envelope to a logical destination.
     *
     * @param destination the logical destination name (e.g. "message-requests")
     * @param envelope    the event envelope to publish
     */
    public void publish(String destination, EventEnvelope<?> envelope) {
        validate(destination, envelope);

        if (log.isDebugEnabled()) {
            log.debug("MessagingService orchestrating publish to destination [{}], envelope: {}", destination, envelope);
        } else {
            log.info("MessagingService orchestrating publish to destination [{}], eventId: {}, eventType: {}, correlationId: {}",
                    destination, envelope.eventId(), envelope.eventType(), envelope.correlationId());
        }

        messagePublisher.publish(destination, envelope);
    }

    private void validate(String destination, EventEnvelope<?> envelope) {
        if (destination == null || destination.trim().isEmpty()) {
            throw new IllegalArgumentException("Destination cannot be null or empty");
        }
        if (envelope == null) {
            throw new IllegalArgumentException("Event envelope cannot be null");
        }
        if (envelope.eventId() == null || envelope.eventId().trim().isEmpty()) {
            throw new IllegalArgumentException("Event ID cannot be null or empty");
        }
        if (envelope.eventType() == null || envelope.eventType().trim().isEmpty()) {
            throw new IllegalArgumentException("Event type cannot be null or empty");
        }
        if (envelope.payload() == null) {
            throw new IllegalArgumentException("Event payload cannot be null");
        }
    }
}
