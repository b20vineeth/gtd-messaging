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
     * Map of logical destinations to destination configurations.
     * key: logical destination (e.g. "message-requests")
     */
    private Map<String, DestinationProperties> destinations = new HashMap<>();

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
    }
}
