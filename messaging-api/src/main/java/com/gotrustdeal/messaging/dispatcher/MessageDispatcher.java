package com.gotrustdeal.messaging.dispatcher;

import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.consumer.MessageHandler;
import com.gotrustdeal.messaging.exception.MessageHandlerNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport-independent dispatcher that routes incoming event envelopes to their registered handlers.
 */
@Slf4j
@Component
public class MessageDispatcher {

    private final Map<String, MessageHandler<?>> handlers = new HashMap<>();

    public MessageDispatcher(List<MessageHandler<?>> handlerList) {
        if (handlerList != null) {
            for (MessageHandler<?> handler : handlerList) {
                String messageType = handler.messageType();
                if (messageType == null || messageType.trim().isEmpty()) {
                    throw new IllegalStateException("MessageHandler messageType cannot be null or empty");
                }
                if (handlers.containsKey(messageType)) {
                    throw new IllegalStateException("Duplicate MessageHandler registered for message type: " + messageType);
                }
                handlers.put(messageType, handler);
            }
        }
    }

    /**
     * Dispatch the envelope to the appropriate handler based on the event/message type.
     *
     * @param envelope the event envelope containing metadata and payload
     */
    public void dispatch(EventEnvelope<?> envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("Event envelope cannot be null");
        }

        String eventType = envelope.eventType();
        if (eventType == null || eventType.trim().isEmpty()) {
            throw new IllegalArgumentException("Event type cannot be null or empty");
        }

        MessageHandler<?> handler = handlers.get(eventType);
        if (handler == null) {
            throw new MessageHandlerNotFoundException("No message handler registered for message type: " + eventType);
        }

        // Correlation and Trace log (Avoid logging sensitive payload)
        log.info("Dispatching message. EventId: {}, EventType: {}, CorrelationId: {}, CausationId/TraceId: {}",
                envelope.eventId(), envelope.eventType(), envelope.correlationId(), envelope.causationId());

        try {
            @SuppressWarnings("unchecked")
            MessageHandler<Object> typedHandler = (MessageHandler<Object>) handler;
            typedHandler.handle((EventEnvelope) envelope);
        } catch (Exception e) {
            log.error("Failed to handle message. EventId: {}, EventType: {}", envelope.eventId(), eventType, e);
            throw e;
        }
    }
}
