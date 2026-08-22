package com.gotrustdeal.messaging;

import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.consumer.EmailSender;
import com.gotrustdeal.messaging.api.dto.EmailMessagePayload;
import com.gotrustdeal.messaging.dispatcher.MessageDispatcher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * Integration test verifying that EmailVerificationMessageHandler is discovered
 * by the Spring context, registered within MessageDispatcher, and processes
 * dispatched messages end-to-end.
 */
@SpringBootTest
@ActiveProfiles("local")
class MessagingServiceApplicationIntegrationTest {

    @Autowired
    private MessageDispatcher messageDispatcher;

    @MockBean
    private EmailSender emailSender;

    @Test
    void testHandlerRegistrationAndDispatch() {
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
                "evt-integration-123",
                "identity.email.verification.triggered",
                1,
                Instant.now(),
                "identity-service",
                "corr-integration-123",
                "caus-integration-123",
                payload
        );

        // Act
        messageDispatcher.dispatch(envelope);

        // Assert - verify the handler was registered and successfully invoked the email sender
        verify(emailSender).send(any(EmailMessagePayload.class));
    }
}
