package com.gotrustdeal.messaging.aws.sqs;

import com.gotrustdeal.common.events.EventEnvelope;
import com.gotrustdeal.common.events.EventPublisher;
import com.gotrustdeal.messaging.api.MessagePublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Bridge class that maps the older EventPublisher interface calls used by the business services
 * (like gtd-identity) to the new MessagePublisher SQS implementation.
 */
@Component("sqsEventPublisher")
@ConditionalOnProperty(name = "gtd.messaging.provider", havingValue = "sqs")
@RequiredArgsConstructor
public class SqsEventPublisherBridge implements EventPublisher {

    private final MessagePublisher messagePublisher;

    @Override
    public <T> void publish(String destination, EventEnvelope<T> event) {
        messagePublisher.publish(destination, event);
    }
}
