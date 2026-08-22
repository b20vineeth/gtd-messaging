package com.gotrustdeal.messaging.service.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.api.consumer.MessageHandler;
import com.gotrustdeal.messaging.api.consumer.EmailSender;
import com.gotrustdeal.messaging.api.dto.EmailMessagePayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Message handler that processes the "identity.email.verification.triggered" event
 * by extracting and validating the payload, then routing it to the EmailSender.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EmailVerificationMessageHandler implements MessageHandler<Object> {

    private final EmailSender emailSender;
    private final ObjectMapper objectMapper;

    @Override
    public String messageType() {
        return "identity.email.verification.triggered";
    }

    @Override
    public void handle(EventEnvelope<Object> envelope) {
        if (envelope == null || envelope.payload() == null) {
            throw new IllegalArgumentException("Event envelope or payload cannot be null");
        }

        // Convert the generic payload (usually deserialized as a Map) to EmailMessagePayload
        EmailMessagePayload payload = objectMapper.convertValue(envelope.payload(), EmailMessagePayload.class);

        // Validate payload fields
        validatePayload(payload);

        log.info("Processing email verification request. EventId: {}, CorrelationId: {}, CausationId/TraceId: {}",
                envelope.eventId(), envelope.correlationId(), envelope.causationId());

        emailSender.send(payload);
    }

    private void validatePayload(EmailMessagePayload payload) {
        if (payload.getRecipient() == null || payload.getRecipient().getEmail() == null || payload.getRecipient().getEmail().trim().isEmpty()) {
            throw new IllegalArgumentException("Recipient email is required");
        }
        if (payload.getTemplateCode() == null || payload.getTemplateCode().trim().isEmpty()) {
            throw new IllegalArgumentException("Template code is required");
        }
        if (payload.getVariables() == null || payload.getVariables().isEmpty()) {
            throw new IllegalArgumentException("Message variables are required");
        }
        if (!payload.getVariables().containsKey("verification_code")) {
            throw new IllegalArgumentException("Verification code is required in variables");
        }
        if (!payload.getVariables().containsKey("auth_code")) {
            throw new IllegalArgumentException("Auth code is required in variables");
        }
    }
}
