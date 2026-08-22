package com.gotrustdeal.messaging.service.email;

import com.gotrustdeal.messaging.api.consumer.EmailSender;
import com.gotrustdeal.messaging.api.dto.EmailMessagePayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Local development email sender that logs delivery details safely without leaking sensitive information.
 */
@Slf4j
@Component
public class LoggingEmailSender implements EmailSender {

    @Override
    public void send(EmailMessagePayload payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Email payload cannot be null");
        }
        if (payload.getRecipient() == null || payload.getRecipient().getEmail() == null || payload.getRecipient().getEmail().trim().isEmpty()) {
            throw new IllegalArgumentException("Recipient email cannot be null or empty");
        }
        if (payload.getTemplateCode() == null || payload.getTemplateCode().trim().isEmpty()) {
            throw new IllegalArgumentException("Template code cannot be null or empty");
        }

        // Mask sensitive variables for safe logging
        Map<String, String> maskedVariables = new HashMap<>();
        if (payload.getVariables() != null) {
            for (Map.Entry<String, String> entry : payload.getVariables().entrySet()) {
                String key = entry.getKey();
                if ("verification_code".equals(key) || "auth_code".equals(key) || "verification_url".equals(key) || "token".equals(key) || "reset_url".equals(key)) {
                    maskedVariables.put(key, "[MASKED]");
                } else {
                    maskedVariables.put(key, entry.getValue());
                }
            }
        }

        log.info("Email delivery request logged. Recipient: {}, TemplateCode: {}, Channel: {}, Variables: {}",
                payload.getRecipient().getEmail(),
                payload.getTemplateCode(),
                payload.getChannel(),
                maskedVariables);
    }
}
