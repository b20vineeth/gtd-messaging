package com.gotrustdeal.messaging.api.consumer;

import com.gotrustdeal.messaging.api.dto.EmailMessagePayload;

/**
 * Interface contract for email providers (e.g. SMTP, SES, SendGrid).
 */
public interface EmailSender {
    /**
     * Send an email using the provided message payload.
     *
     * @param payload the email message payload
     */
    void send(EmailMessagePayload payload);
}
