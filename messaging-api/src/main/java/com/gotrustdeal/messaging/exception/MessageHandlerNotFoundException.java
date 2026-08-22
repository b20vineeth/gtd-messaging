package com.gotrustdeal.messaging.exception;

/**
 * Exception thrown when a handler cannot be resolved for a specific message/event type.
 */
public class MessageHandlerNotFoundException extends MessagingException {

    public MessageHandlerNotFoundException(String message) {
        super(message);
    }

    public MessageHandlerNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
