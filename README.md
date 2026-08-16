# GoTrustDeal Messaging Infrastructure (gtd-messaging)

`gtd-messaging` is a centralized, multi-module Maven project providing transport-agnostic messaging contracts, orchestration, and AWS SQS integration for the GoTrustDeal platform.

## Architecture

The project consists of three core modules:

```mermaid
graph TD
    subgraph gtd-messaging
        messaging-service[messaging-service] --> messaging-api[messaging-api]
        messaging-aws[messaging-aws] --> messaging-api[messaging-api]
    end
    gtd-common-events[gtd-common-events] <. - Depends on - .-> messaging-api
```

- **`messaging-api`**: Transport-independent API. Defines `MessagePublisher` and references framework-wide message contracts (`EventEnvelope`, `EventMetadata`). It does not contain any AWS-specific dependencies.
- **`messaging-service`**: Business-facing messaging service. Owns standard orchestration logic and uses dependency injection to invoke publishers.
- **`messaging-aws`**: AWS-specific implementation module containing `SqsMessagePublisher` and auto-configuration classes. Only this module depends on the AWS SDK.

### Relationship with `gtd-message`
- **`gtd-messaging`**: Infrastructure framework that provides interfaces, abstractions, AWS configuration, and routing to publishers.
- **`gtd-message`**: A separate business microservice that consumes message requests, resolves templates, and sends email/SMS/push notifications to users.

---

## AWS SQS Flow

1. Business Service publishes an event:
   ```java
   messagingService.publish("message-requests", eventEnvelope);
   ```
2. `SqsMessagePublisher` maps the logical destination `"message-requests"` to the physical SQS queue configuration.
3. Logical destination properties are resolved, fetching the SQS Queue URL from AWS SQS (and cached for future publications) if only a queue name is configured.
4. The envelope is serialized to JSON using `ObjectMapper`.
5. The serialized message body is published to AWS SQS via `SqsClient`.

---

## Configuration

Properties are mapped using the `gtd.messaging` prefix. You can specify direct queue URLs or queue names to be resolved.

```yaml
gtd:
  messaging:
    destinations:
      message-requests:
        queue-name: dev-message-requests-queue
      user-notifications:
        queue-url: https://sqs.us-east-1.amazonaws.com/123456789012/user-notifications-queue
```

*Note: If no destination mapping is configured for a logical name, the publisher falls back to using the logical name directly as the SQS queue name for URL resolution.*

---

## Example Usage

```java
@Service
@RequiredArgsConstructor
public class UserNotificationService {

    private final MessagingService messagingService;

    public void notifyUser(UserDto user) {
        EventEnvelope<UserDto> envelope = EventEnvelope.wrap(
            user, 
            "user.created", 
            "identity-service", 
            user.getCompanyId()
        );
        
        messagingService.publish("user-notifications", envelope);
    }
}
```

---

## Local Development & Testing

Unit tests run without requiring a live AWS connection. All AWS client operations are mocked via Mockito.
To compile and run tests:
```bash
mvn clean test
```
