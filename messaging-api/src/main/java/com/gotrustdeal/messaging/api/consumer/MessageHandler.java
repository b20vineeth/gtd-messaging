package com.gotrustdeal.messaging.api.consumer;

import com.gotrustdeal.common.events.EventEnvelope;

/**
 * Transport-independent message handler contract.
 *
 * @param <T> the type of the event payload
 */
public interface MessageHandler<T> {

    /**
     * The logical event/message type this handler supports (e.g. "identity.email.verification.triggered").
     *
     * @return the message type string
     */
    String messageType();

    /**
     * Process the message envelope.
     *
     * @param envelope the event envelope containing metadata and payload
     */
    void handle(EventEnvelope<T> envelope);
}
