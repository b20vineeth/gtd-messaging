package com.gotrustdeal.messaging.aws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotrustdeal.messaging.api.MessagePublisher;
import com.gotrustdeal.messaging.aws.properties.AwsMessagingProperties;
import com.gotrustdeal.messaging.aws.sqs.SqsMessagePublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Spring Auto-Configuration for AWS SQS messaging components.
 */
@Configuration
@EnableConfigurationProperties(AwsMessagingProperties.class)
public class AwsMessagingConfiguration {

    @Bean
    @ConditionalOnMissingBean(MessagePublisher.class)
    public MessagePublisher messagePublisher(
            SqsClient sqsClient,
            AwsMessagingProperties properties,
            ObjectMapper objectMapper) {
        return new SqsMessagePublisher(sqsClient, properties, objectMapper);
    }
}
