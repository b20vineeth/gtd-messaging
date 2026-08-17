package com.gotrustdeal.messaging.dispatcher;

import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.consumer.MessageHandler;
import com.gotrustdeal.messaging.exception.MessageHandlerNotFoundException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageDispatcherTest {

    @Test
    void testDispatch_Success() {
        // Arrange
        TestMessageHandler handler1 = new TestMessageHandler("type.one");
        TestMessageHandler handler2 = new TestMessageHandler("type.two");
        MessageDispatcher dispatcher = new MessageDispatcher(Arrays.asList(handler1, handler2));

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "type.one",
                1,
                Instant.now(),
                "source",
                "corr-id",
                "caus-id",
                "payload-one"
        );

        // Act
        dispatcher.dispatch(envelope);

        // Assert
        assertTrue(handler1.isInvoked());
        assertEquals(envelope, handler1.getReceivedEnvelope());
    }

    @Test
    void testDispatch_UnknownMessageType_ThrowsException() {
        TestMessageHandler handler = new TestMessageHandler("type.one");
        MessageDispatcher dispatcher = new MessageDispatcher(Collections.singletonList(handler));

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "type.unknown",
                1,
                Instant.now(),
                "source",
                "corr-id",
                "caus-id",
                "payload"
        );

        assertThrows(MessageHandlerNotFoundException.class, () -> dispatcher.dispatch(envelope));
    }

    @Test
    void testDuplicateHandlerRegistration_ThrowsException() {
        TestMessageHandler handler1 = new TestMessageHandler("type.duplicate");
        TestMessageHandler handler2 = new TestMessageHandler("type.duplicate");
        List<MessageHandler<?>> handlers = Arrays.asList(handler1, handler2);

        assertThrows(IllegalStateException.class, () -> new MessageDispatcher(handlers));
    }

    @Test
    void testHandlerExceptionPropagated() {
        // Arrange
        MessageHandler<String> failingHandler = new MessageHandler<>() {
            @Override
            public String messageType() {
                return "type.fail";
            }

            @Override
            public void handle(EventEnvelope<String> envelope) {
                throw new RuntimeException("Handler failure");
            }
        };

        MessageDispatcher dispatcher = new MessageDispatcher(Collections.singletonList(failingHandler));

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "type.fail",
                1,
                Instant.now(),
                "source",
                "corr-id",
                "caus-id",
                "payload"
        );

        assertThrows(RuntimeException.class, () -> dispatcher.dispatch(envelope));
    }

    private static class TestMessageHandler implements MessageHandler<String> {
        private final String messageType;
        private boolean invoked = false;
        private EventEnvelope<String> receivedEnvelope;

        public TestMessageHandler(String messageType) {
            this.messageType = messageType;
        }

        @Override
        public String messageType() {
            return messageType;
        }

        @Override
        public void handle(EventEnvelope<String> envelope) {
            this.invoked = true;
            this.receivedEnvelope = envelope;
        }

        public boolean isInvoked() {
            return invoked;
        }

        public EventEnvelope<String> getReceivedEnvelope() {
            return receivedEnvelope;
        }
    }
}
