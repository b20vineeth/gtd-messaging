package com.gotrustdeal.messaging.exception;

/**
 * Base exception class for all messaging-related errors.
 */
public class MessagingException extends RuntimeException {

    public MessagingException(String message) {
        super(message);
    }

    public MessagingException(String message, Throwable cause) {
        super(message, cause);
    }
}
