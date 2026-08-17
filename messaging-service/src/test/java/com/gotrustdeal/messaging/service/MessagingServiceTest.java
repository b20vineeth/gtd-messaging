package com.gotrustdeal.messaging.service;

import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.MessagePublisher;
import com.gotrustdeal.messaging.exception.MessagingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class MessagingServiceTest {

    @Mock
    private MessagePublisher messagePublisher;

    private MessagingService messagingService;

    @BeforeEach
    void setUp() {
        messagingService = new MessagingService(messagePublisher);
    }

    @Test
    void testPublish_Success() {
        String destination = "test-destination";
        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "test.event",
                1,
                Instant.now(),
                "test-source",
                "corr-123",
                "caus-456",
                "Hello"
        );

        messagingService.publish(destination, envelope);

        verify(messagePublisher).publish(destination, envelope);
    }

    @Test
    void testPublish_NullDestination_ThrowsException() {
        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "test.event",
                1,
                Instant.now(),
                "test-source",
                "corr-123",
                "caus-456",
                "Hello"
        );

        assertThrows(IllegalArgumentException.class, () -> messagingService.publish(null, envelope));
    }

    @Test
    void testPublish_BlankDestination_ThrowsException() {
        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "test.event",
                1,
                Instant.now(),
                "test-source",
                "corr-123",
                "caus-456",
                "Hello"
        );

        assertThrows(IllegalArgumentException.class, () -> messagingService.publish("   ", envelope));
    }

    @Test
    void testPublish_NullEnvelope_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> messagingService.publish("destination", null));
    }

    @Test
    void testPublish_MissingEventId_ThrowsException() {
        EventEnvelope<String> envelope = new EventEnvelope<>(
                null,
                "test.event",
                1,
                Instant.now(),
                "test-source",
                "corr-123",
                "caus-456",
                "Hello"
        );

        assertThrows(IllegalArgumentException.class, () -> messagingService.publish("destination", envelope));
    }

    @Test
    void testPublish_MissingEventType_ThrowsException() {
        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "",
                1,
                Instant.now(),
                "test-source",
                "corr-123",
                "caus-456",
                "Hello"
        );

        assertThrows(IllegalArgumentException.class, () -> messagingService.publish("destination", envelope));
    }

    @Test
    void testPublish_MissingPayload_ThrowsException() {
        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "test.event",
                1,
                Instant.now(),
                "test-source",
                "corr-123",
                "caus-456",
                null
        );

        assertThrows(IllegalArgumentException.class, () -> messagingService.publish("destination", envelope));
    }

    @Test
    void testPublish_PropagatesMessagingException() {
        String destination = "test-destination";
        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-1",
                "test.event",
                1,
                Instant.now(),
                "test-source",
                "corr-123",
                "caus-456",
                "Hello"
        );

        doThrow(new MessagingException("Publish failed"))
                .when(messagePublisher).publish(destination, envelope);

        assertThrows(MessagingException.class, () -> messagingService.publish(destination, envelope));
    }
}
