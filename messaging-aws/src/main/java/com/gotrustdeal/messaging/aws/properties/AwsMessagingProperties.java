package com.gotrustdeal.messaging.aws.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for AWS Messaging integration.
 */
@Data
@ConfigurationProperties(prefix = "gtd.messaging")
public class AwsMessagingProperties {

    /**
     * The messaging provider ("sqs", "kafka", etc.)
     */
    private String provider = "kafka";

    /**
     * Custom endpoint URL override (useful for LocalStack, e.g. http://localhost:4566)
     */
    private String endpoint;

    /**
     * AWS region to use.
     */
    private String region = "us-east-1";

    /**
     * AWS Access Key (local dev/LocalStack override)
     */
    private String accessKey;

    /**
     * AWS Secret Key (local dev/LocalStack override)
     */
    private String secretKey;

    /**
     * SQS Consumer specific configurations.
     */
    private ConsumerProperties consumer = new ConsumerProperties();

    /**
     * Map of logical destinations to destination configurations.
     * key: logical destination (e.g. "message-requests")
     */
    private Map<String, DestinationProperties> destinations = new HashMap<>();

    @Data
    public static class ConsumerProperties {
        /**
         * Whether the local SQS listener/consumer is enabled.
         */
        private boolean enabled = false;

        /**
         * Delay between polls in milliseconds.
         */
        private int pollDelayMs = 2000;

        /**
         * Maximum number of messages to retrieve in one poll (max 10).
         */
        private int maxNumberOfMessages = 10;

        /**
         * Long polling wait time in seconds (max 20).
         */
        private int waitTimeSeconds = 10;
    }

    @Data
    public static class DestinationProperties {
        /**
         * The name of the SQS queue.
         */
        private String queueName;

        /**
         * The absolute URL of the SQS queue (optional override).
         */
        private String queueUrl;

        /**
         * The ARN of the SNS topic (if publishing via SNS).
         */
        private String topicArn;

        /**
         * Whether to consume messages from this destination.
         */
        private boolean consumeEnabled = false;
    }
}
