package com.gotrustdeal.messaging.aws.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.messaging.aws.properties.AwsMessagingProperties;
import com.gotrustdeal.messaging.dispatcher.MessageDispatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SqsMessageConsumerIntegrationTest {

    private SqsClient sqsClient;
    private SnsClient snsClient;
    private AwsMessagingProperties properties;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        properties = new AwsMessagingProperties();
        properties.setEndpoint("http://localhost:4566");
        properties.setRegion("us-east-1");
        properties.setAccessKey("test");
        properties.setSecretKey("test");
        properties.getConsumer().setEnabled(true);
        properties.getConsumer().setPollDelayMs(500);
        properties.getConsumer().setWaitTimeSeconds(1);

        AwsMessagingProperties.DestinationProperties dest = new AwsMessagingProperties.DestinationProperties();
        dest.setQueueName("message-requests");
        dest.setConsumeEnabled(true);
        properties.getDestinations().put("message-requests", dest);

        try {
            sqsClient = SqsClient.builder()
                    .region(Region.US_EAST_1)
                    .endpointOverride(URI.create("http://localhost:4566"))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")
                    ))
                    .build();
            
            snsClient = SnsClient.builder()
                    .region(Region.US_EAST_1)
                    .endpointOverride(URI.create("http://localhost:4566"))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")
                    ))
                    .build();

            // Try to create queue in case LocalStack is running but queue doesn't exist
            sqsClient.createQueue(CreateQueueRequest.builder().queueName("message-requests").build());
        } catch (Exception e) {
            // LocalStack might not be running; ignore here so the test can skip or fail gracefully
        }
    }

    @Test
    void testPublishAndConsumeIntegration() throws Exception {
        if (sqsClient == null) {
            System.out.println("LocalStack SQS not running. Skipping integration test.");
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        
        // Mock dispatcher that triggers latch when message type matches
        MessageDispatcher messageDispatcher = new MessageDispatcher(Collections.emptyList()) {
            @Override
            public void dispatch(EventEnvelope<?> envelope) {
                if ("integration.test.event".equals(envelope.eventType())) {
                    latch.countDown();
                }
            }
        };

        SqsMessagePublisher publisher = new SqsMessagePublisher(sqsClient, snsClient, properties, objectMapper);
        SqsMessageConsumer consumer = new SqsMessageConsumer(sqsClient, messageDispatcher, properties, objectMapper);

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-integration-123",
                "integration.test.event",
                1,
                Instant.now(),
                "test-source",
                "corr-id-123",
                "caus-id-123",
                "Hello SQS LocalStack"
        );

        // Act - Publish message
        publisher.publish("message-requests", envelope);

        // Act - Poll once manually
        consumer.poll();

        // Assert - Message should be consumed and trigger dispatcher
        boolean success = latch.await(10, TimeUnit.SECONDS);
        assertTrue(success, "Message should be successfully consumed and dispatched!");
    }

    @Test
    void testSnsToSqsFanOutIntegration() throws Exception {
        if (sqsClient == null || snsClient == null) {
            System.out.println("LocalStack not running. Skipping fan-out integration test.");
            return;
        }

        String topicArn;
        try {
            // 1. Create SNS Topic
            topicArn = snsClient.createTopic(CreateTopicRequest.builder().name("identity-events").build()).topicArn();
        } catch (Exception e) {
            System.out.println("SNS service not enabled/available. Skipping fan-out integration test. Error: " + e.getMessage());
            return;
        }

        // 2. Create SQS Queues
        String emailQueueUrl = sqsClient.createQueue(CreateQueueRequest.builder().queueName("email-events-queue").build()).queueUrl();
        String auditQueueUrl = sqsClient.createQueue(CreateQueueRequest.builder().queueName("audit-events-queue").build()).queueUrl();

        // 3. Get Queue ARNs
        String emailQueueArn = sqsClient.getQueueAttributes(r -> r.queueUrl(emailQueueUrl).attributeNamesWithStrings("QueueArn"))
                .attributesAsStrings().get("QueueArn");
        String auditQueueArn = sqsClient.getQueueAttributes(r -> r.queueUrl(auditQueueUrl).attributeNamesWithStrings("QueueArn"))
                .attributesAsStrings().get("QueueArn");

        // 4. Subscribe SQS Queues to SNS Topic
        snsClient.subscribe(SubscribeRequest.builder().topicArn(topicArn).protocol("sqs").endpoint(emailQueueArn).build());
        snsClient.subscribe(SubscribeRequest.builder().topicArn(topicArn).protocol("sqs").endpoint(auditQueueArn).build());

        // 5. Configure properties destinations
        AwsMessagingProperties.DestinationProperties topicDest = new AwsMessagingProperties.DestinationProperties();
        topicDest.setTopicArn(topicArn);
        properties.getDestinations().put("identity-events", topicDest);

        AwsMessagingProperties.DestinationProperties emailQueueDest = new AwsMessagingProperties.DestinationProperties();
        emailQueueDest.setQueueUrl(emailQueueUrl);
        emailQueueDest.setQueueName("email-events-queue");
        emailQueueDest.setConsumeEnabled(true);
        properties.getDestinations().put("email-events-queue", emailQueueDest);

        AwsMessagingProperties.DestinationProperties auditQueueDest = new AwsMessagingProperties.DestinationProperties();
        auditQueueDest.setQueueUrl(auditQueueUrl);
        auditQueueDest.setQueueName("audit-events-queue");
        auditQueueDest.setConsumeEnabled(true);
        properties.getDestinations().put("audit-events-queue", auditQueueDest);

        // 6. Set up dispatcher to track messages
        CountDownLatch latch = new CountDownLatch(2);
        java.util.List<EventEnvelope<?>> receivedEnvelopes = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        MessageDispatcher messageDispatcher = new MessageDispatcher(Collections.emptyList()) {
            @Override
            public void dispatch(EventEnvelope<?> envelope) {
                if ("identity.email.verification.triggered".equals(envelope.eventType())) {
                    receivedEnvelopes.add(envelope);
                    latch.countDown();
                }
            }
        };

        SqsMessagePublisher publisher = new SqsMessagePublisher(sqsClient, snsClient, properties, objectMapper);
        SqsMessageConsumer consumer = new SqsMessageConsumer(sqsClient, messageDispatcher, properties, objectMapper);

        EventEnvelope<String> envelope = new EventEnvelope<>(
                "evt-fanout-123",
                "identity.email.verification.triggered",
                1,
                Instant.now(),
                "identity-service",
                "corr-id-fanout",
                "caus-id-fanout",
                "UserRegisteredPayload"
        );

        // 7. Publish to SNS
        publisher.publish("identity-events", envelope);

        // Allow some time for SNS to fan out messages to SQS queues
        Thread.sleep(1000);

        // 8. Poll SQS queues (both should contain the message)
        consumer.poll();

        // 9. Verify both consumers processed the message independently
        boolean done = latch.await(10, TimeUnit.SECONDS);
        assertTrue(done, "Both Email and Audit queues should have received and processed the event independently!");
        org.junit.jupiter.api.Assertions.assertEquals(2, receivedEnvelopes.size());
        org.junit.jupiter.api.Assertions.assertEquals("evt-fanout-123", receivedEnvelopes.get(0).eventId());
        org.junit.jupiter.api.Assertions.assertEquals("evt-fanout-123", receivedEnvelopes.get(1).eventId());
    }
}
