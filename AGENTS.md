# GoTrustDeal Messaging Developer Rules

This document defines mandatory guidelines and constraints for developers and AI agents working on the `gtd-messaging` codebase.
for start application start-gtd-messaging.bat 
## 1. Architectural & Dependency Rules
- **Infrastructure Scope**: `gtd-messaging` is shared infrastructure. Do not put microservice-specific business logic, email templates, or email delivery logic here. (Email/SMS microservice-specific delivery logic belongs in `gtd-message`).
- **Strict Separation of Concerns**:
  - `messaging-api`: Must remain transport-independent. It MUST NOT contain AWS SQS, Kafka, or other provider-specific dependencies.
  - `messaging-service`: Acts as the service orchestration layer. It depends on `messaging-api` but remains transport-agnostic (no AWS SDK code).
  - `messaging-aws`: The only module allowed to use the AWS SDK (`sqs`).
- **No Direct SqsClient Exposure**: Never expose `SqsClient`, `SendMessageRequest`, or SQS queue URLs directly to business services or outer layers. Keep all AWS objects encapsulated within `messaging-aws`.

## 2. Queue Resolution & Routing
- **Use Logical Destinations**: Publishers must refer to logical destinations (e.g. `"message-requests"`).
- **Never Hardcode Queue URLs**: Retrieve and map queue URLs dynamically via logical destination mappings defined under the `gtd.messaging.destinations` property prefix.
- **Cache Resolved URLs**: Always cache physical SQS queue URLs resolved via the SqsClient to avoid latency and API rate-limiting.

## 3. Data Integrity & Logging
- **Preserve Correlation & Causation IDs**: Ensure that `correlationId` and `causationId`/`requestId` are preserved across boundaries inside the `EventEnvelope`.
- **Never Log Sensitive Payloads**: Avoid logging full message payloads at INFO or higher levels. Use DEBUG level for full serialization logs, and log metadata (eventId, eventType) at INFO level.
- **Reuse Framework Infrastructure**: Integrate with and reuse `gtd-common-events` (e.g., `EventEnvelope`, `EventPublisher`) instead of creating duplicate representations or custom outbox implementations.
