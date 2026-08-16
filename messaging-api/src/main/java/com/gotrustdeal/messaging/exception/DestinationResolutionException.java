package com.gotrustdeal.messaging.exception;

/**
 * Exception thrown when a logical destination configuration is missing or invalid.
 */
public class DestinationResolutionException extends MessagingException {

    public DestinationResolutionException(String message) {
        super(message);
    }

    public DestinationResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
