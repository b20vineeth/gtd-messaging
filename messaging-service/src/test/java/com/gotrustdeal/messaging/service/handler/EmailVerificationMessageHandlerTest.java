package com.gotrustdeal.messaging.service.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.consumer.EmailSender;
import com.gotrustdeal.messaging.api.dto.EmailMessagePayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EmailVerificationMessageHandlerTest {

    @Mock
    private EmailSender emailSender;

    private ObjectMapper objectMapper;
    private EmailVerificationMessageHandler handler;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        handler = new EmailVerificationMessageHandler(emailSender, objectMapper);
    }

    @Test
    void testMessageType() {
        assertEquals("identity.email.verification.triggered", handler.messageType());
    }

    @Test
    void testHandle_Success() {
        // Arrange
        Map<String, String> variables = new HashMap<>();
        variables.put("first_name", "John");
        variables.put("verification_code", "123456");
        variables.put("auth_code", "auth-token-123");

        EmailMessagePayload payload = EmailMessagePayload.builder()
                .channel("EMAIL")
                .templateCode("IDENTITY_EMAIL_VERIFICATION")
                .recipient(new EmailMessagePayload.Recipient("john@example.com"))
                .variables(variables)
                .build();

        EventEnvelope<Object> envelope = new EventEnvelope<>(
                "evt-123",
                "identity.email.verification.triggered",
                1,
                Instant.now(),
                "identity-service",
                "corr-123",
                "caus-123",
                payload
        );

        // Act
        handler.handle(envelope);

        // Assert
        verify(emailSender).send(any(EmailMessagePayload.class));
    }

    @Test
    void testHandle_MissingEmail_ThrowsException() {
        // Arrange
        Map<String, String> variables = new HashMap<>();
        variables.put("first_name", "John");
        variables.put("verification_code", "123456");
        variables.put("auth_code", "auth-token-123");

        EmailMessagePayload payload = EmailMessagePayload.builder()
                .channel("EMAIL")
                .templateCode("IDENTITY_EMAIL_VERIFICATION")
                .recipient(new EmailMessagePayload.Recipient("")) // Empty email
                .variables(variables)
                .build();

        EventEnvelope<Object> envelope = new EventEnvelope<>(
                "evt-123",
                "identity.email.verification.triggered",
                1,
                Instant.now(),
                "identity-service",
                "corr-123",
                "caus-123",
                payload
        );

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> handler.handle(envelope));
    }

    @Test
    void testHandle_MissingVerificationCode_ThrowsException() {
        // Arrange
        Map<String, String> variables = new HashMap<>();
        variables.put("first_name", "John");
        variables.put("auth_code", "auth-token-123"); // Missing verification_code

        EmailMessagePayload payload = EmailMessagePayload.builder()
                .channel("EMAIL")
                .templateCode("IDENTITY_EMAIL_VERIFICATION")
                .recipient(new EmailMessagePayload.Recipient("john@example.com"))
                .variables(variables)
                .build();

        EventEnvelope<Object> envelope = new EventEnvelope<>(
                "evt-123",
                "identity.email.verification.triggered",
                1,
                Instant.now(),
                "identity-service",
                "corr-123",
                "caus-123",
                payload
        );

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> handler.handle(envelope));
    }

    @Test
    void testHandle_SenderFailure_PropagatesException() {
        // Arrange
        Map<String, String> variables = new HashMap<>();
        variables.put("first_name", "John");
        variables.put("verification_code", "123456");
        variables.put("auth_code", "auth-token-123");

        EmailMessagePayload payload = EmailMessagePayload.builder()
                .channel("EMAIL")
                .templateCode("IDENTITY_EMAIL_VERIFICATION")
                .recipient(new EmailMessagePayload.Recipient("john@example.com"))
                .variables(variables)
                .build();

        EventEnvelope<Object> envelope = new EventEnvelope<>(
                "evt-123",
                "identity.email.verification.triggered",
                1,
                Instant.now(),
                "identity-service",
                "corr-123",
                "caus-123",
                payload
        );

        doThrow(new RuntimeException("Mail delivery failed")).when(emailSender).send(any(EmailMessagePayload.class));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> handler.handle(envelope));
    }
}
