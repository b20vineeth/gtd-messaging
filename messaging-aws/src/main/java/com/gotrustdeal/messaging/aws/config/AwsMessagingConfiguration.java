package com.gotrustdeal.messaging.aws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotrustdeal.messaging.api.MessagePublisher;
import com.gotrustdeal.messaging.aws.properties.AwsMessagingProperties;
import com.gotrustdeal.messaging.aws.sqs.SqsMessageConsumer;
import com.gotrustdeal.messaging.aws.sqs.SqsMessagePublisher;
import com.gotrustdeal.messaging.dispatcher.MessageDispatcher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import org.springframework.beans.factory.InitializingBean;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;

/**
 * Spring Auto-Configuration for AWS SQS messaging components.
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(AwsMessagingProperties.class)
@EnableScheduling
@ConditionalOnProperty(name = "gtd.messaging.provider", havingValue = "sqs")
public class AwsMessagingConfiguration implements InitializingBean {

    private final AwsMessagingProperties properties;

    public AwsMessagingConfiguration(AwsMessagingProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        boolean isLocal = properties.getEndpoint() != null && !properties.getEndpoint().trim().isEmpty();
        log.info("GTD SQS Auto-Configuration Diagnostics - Provider: {}, Region: {}, SQS Endpoint: {}, Local endpoint: {}",
                properties.getProvider(),
                properties.getRegion(),
                isLocal ? properties.getEndpoint().trim() : "AWS Default",
                isLocal);
    }

    @Bean
    @ConditionalOnMissingBean(SqsClient.class)
    public SqsClient sqsClient(AwsMessagingProperties properties) {
        SqsClientBuilder builder = SqsClient.builder()
                .region(Region.of(properties.getRegion()));

        if (properties.getEndpoint() != null && !properties.getEndpoint().trim().isEmpty()) {
            builder.endpointOverride(URI.create(properties.getEndpoint().trim()));
        }

        if (properties.getAccessKey() != null && properties.getSecretKey() != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(properties.getAccessKey(), properties.getSecretKey())
            ));
        }

        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean(SnsClient.class)
    public SnsClient snsClient(AwsMessagingProperties properties) {
        SnsClientBuilder builder = SnsClient.builder()
                .region(Region.of(properties.getRegion()));

        if (properties.getEndpoint() != null && !properties.getEndpoint().trim().isEmpty()) {
            builder.endpointOverride(URI.create(properties.getEndpoint().trim()));
        }

        if (properties.getAccessKey() != null && properties.getSecretKey() != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(properties.getAccessKey(), properties.getSecretKey())
            ));
        }

        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean(MessagePublisher.class)
    public MessagePublisher messagePublisher(
            SqsClient sqsClient,
            SnsClient snsClient,
            AwsMessagingProperties properties,
            ObjectMapper objectMapper) {
        return new SqsMessagePublisher(sqsClient, snsClient, properties, objectMapper);
    }

    @Bean
    @ConditionalOnProperty(name = "gtd.messaging.consumer.enabled", havingValue = "true")
    public SqsMessageConsumer sqsMessageConsumer(
            SqsClient sqsClient,
            MessageDispatcher messageDispatcher,
            AwsMessagingProperties properties,
            ObjectMapper objectMapper) {
        return new SqsMessageConsumer(sqsClient, messageDispatcher, properties, objectMapper);
    }
}
