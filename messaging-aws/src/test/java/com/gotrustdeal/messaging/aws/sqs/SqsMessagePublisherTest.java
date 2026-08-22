package com.gotrustdeal.messaging.aws.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.aws.properties.AwsMessagingProperties;
import com.gotrustdeal.messaging.exception.DestinationResolutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqsMessagePublisherTest {

    @Mock
    private SqsClient sqsClient;

    @Mock
    private SnsClient snsClient;

    private AwsMessagingProperties properties;
    private ObjectMapper objectMapper;
    private SqsMessagePublisher publisher;

    @BeforeEach
    void setUp() {
        properties = new AwsMessagingProperties();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule()); // Support Java 8 Time APIs
        publisher = new SqsMessagePublisher(sqsClient, snsClient, properties, objectMapper);
    }

    @Test
    void testPublish_WithConfiguredQueueUrl() throws Exception {
        // Arrange
        String destination = "message-requests";
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/my-test-queue";

        AwsMessagingProperties.DestinationProperties destProps = new AwsMessagingProperties.DestinationProperties();
        destProps.setQueueUrl(queueUrl);
        properties.getDestinations().put(destination, destProps);

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-123",
                "test.event.type",
                1,
                Instant.now(),
                "test-service",
                "correlation-id-abc",
                "causation-id-xyz",
                "Hello World Payload"
        );

        when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(SendMessageResponse.builder().messageId("msg-abc").build());

        // Act
        publisher.publish(destination, envelope);

        // Assert
        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(requestCaptor.capture());

        SendMessageRequest capturedRequest = requestCaptor.getValue();
        assertEquals(queueUrl, capturedRequest.queueUrl());

        // Verify payload JSON serialization retains all properties (correlation_id and trace_id/causationId)
        String body = capturedRequest.messageBody();
        assertTrue(body.contains("evt-123"));
        assertTrue(body.contains("test.event.type"));
        assertTrue(body.contains("correlation-id-abc"));
        assertTrue(body.contains("causation-id-xyz"));
        assertTrue(body.contains("Hello World Payload"));
    }

    @Test
    void testPublish_WithConfiguredQueueNameResolvesAndCaches() {
        // Arrange
        String destination = "message-requests";
        String queueName = "dev-message-queue";
        String resolvedUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/dev-message-queue";

        AwsMessagingProperties.DestinationProperties destProps = new AwsMessagingProperties.DestinationProperties();
        destProps.setQueueName(queueName);
        properties.getDestinations().put(destination, destProps);

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-123",
                "test.event.type",
                1,
                Instant.now(),
                "test-service",
                "correlation-id-abc",
                "causation-id-xyz",
                "Payload"
        );

        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl(resolvedUrl).build());
        when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(SendMessageResponse.builder().messageId("msg-abc").build());

        // Act
        publisher.publish(destination, envelope);
        publisher.publish(destination, envelope); // Second call should hit cache

        // Assert
        verify(sqsClient, times(1)).getQueueUrl(any(GetQueueUrlRequest.class)); // Verifies caching resolves once
        verify(sqsClient, times(2)).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void testPublish_FallbackToDestinationName() {
        // Arrange
        String destination = "unconfigured-queue-name";
        String resolvedUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/unconfigured-queue-name";

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-123",
                "test.event.type",
                1,
                Instant.now(),
                "test-service",
                "correlation-id-abc",
                "causation-id-xyz",
                "Payload"
        );

        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl(resolvedUrl).build());
        when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(SendMessageResponse.builder().messageId("msg-abc").build());

        // Act
        publisher.publish(destination, envelope);

        // Assert
        ArgumentCaptor<GetQueueUrlRequest> requestCaptor = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
        verify(sqsClient).getQueueUrl(requestCaptor.capture());
        assertEquals(destination, requestCaptor.getValue().queueName());
    }

    @Test
    void testPublish_InvalidArguments() {
        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-123",
                "test.event",
                1,
                Instant.now(),
                "test-service",
                "corr-1",
                "caus-1",
                "payload"
        );

        assertThrows(IllegalArgumentException.class, () -> publisher.publish("", envelope));
        assertThrows(IllegalArgumentException.class, () -> publisher.publish("dest", null));
    }

    @Test
    void testPublish_ResolutionFailureThrowsException() {
        // Arrange
        String destination = "invalid-destination";
        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenThrow(new RuntimeException("SQS Queue not found"));

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-123",
                "test.event",
                1,
                Instant.now(),
                "test-service",
                "corr-1",
                "caus-1",
                "payload"
        );

        // Act & Assert
        assertThrows(DestinationResolutionException.class, () -> publisher.publish(destination, envelope));
    }

    @Test
    void testPublish_WithConfiguredTopicArn() throws Exception {
        // Arrange
        String destination = "identity-events";
        String topicArn = "arn:aws:sns:us-east-1:123456789012:identity-events";

        AwsMessagingProperties.DestinationProperties destProps = new AwsMessagingProperties.DestinationProperties();
        destProps.setTopicArn(topicArn);
        properties.getDestinations().put(destination, destProps);

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-123",
                "test.event.type",
                1,
                Instant.now(),
                "test-service",
                "correlation-id-abc",
                "causation-id-xyz",
                "Hello SNS Payload"
        );

        when(snsClient.publish(any(PublishRequest.class)))
                .thenReturn(PublishResponse.builder().messageId("sns-msg-123").build());

        // Act
        publisher.publish(destination, envelope);

        // Assert
        ArgumentCaptor<PublishRequest> requestCaptor = ArgumentCaptor.forClass(PublishRequest.class);
        verify(snsClient).publish(requestCaptor.capture());

        PublishRequest capturedRequest = requestCaptor.getValue();
        assertEquals(topicArn, capturedRequest.topicArn());
        assertTrue(capturedRequest.message().contains("Hello SNS Payload"));
    }
}
