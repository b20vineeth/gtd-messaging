package com.gotrustdeal.messaging.api;

import com.gotrustdeal.common.events.EventEnvelope;

/**
 * Transport-independent messaging publisher contract.
 */
public interface MessagePublisher {

    /**
     * Publish an event envelope to a logical destination.
     *
     * @param destination the logical destination name (e.g. "message-requests")
     * @param envelope    the transport-independent event envelope containing metadata and payload
     */
    void publish(String destination, EventEnvelope<?> envelope);
}
