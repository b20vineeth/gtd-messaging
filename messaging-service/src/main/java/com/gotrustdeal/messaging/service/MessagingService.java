package com.gotrustdeal.messaging.service;

import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.MessagePublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * Service that orchestrates common messaging operations.
 * Delegates actual publishing to the configured transport implementation.
 */
@Service
@RequiredArgsConstructor
public class MessagingService {

    private final MessagePublisher messagePublisher;

    /**
     * Publishes a message envelope to a logical destination.
     *
     * @param destination the logical destination name
     * @param message     the event envelope to publish
     */
    public void publish(String destination, EventEnvelope<?> message) {
        messagePublisher.publish(destination, message);
    }
}
