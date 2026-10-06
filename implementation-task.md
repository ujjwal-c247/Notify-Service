# Local Booking Notification System
## Implementation Specification for Antigravity

**Status:** Ready for implementation  
**Environment:** Local development / POC  
**Primary use case:** Booking confirmation + cancellation with email notification  
**Architecture target:** Production-oriented design, locally executable  
**Current notification channel:** Email only  
**Current booking events:** `BOOKING_CONFIRMED`, `BOOKING_CANCELLED`

---

# 1. Goal

Build a reliable, event-driven notification system for the booking platform that can later scale to millions of notification events.

The local implementation must demonstrate:

- Reliable booking event generation
- Transactional Outbox pattern
- Kafka event delivery
- Notification creation
- BullMQ job processing
- Redis-backed queue
- Email worker
- Final notification validity validation
- Idempotency/deduplication
- Retry handling
- Notification expiration
- Booking cancellation race-condition handling
- Worker failure/retry handling
- Clear separation between booking and notification domains

The implementation must be production-oriented in architecture but optimized for local development.

---

# 2. Scope

## In scope

### Booking events

Only:

```text
BOOKING_CONFIRMED
BOOKING_CANCELLED
```

### Notification channel

Only:

```text
EMAIL
```

### Notification types

```text
BOOKING_CONFIRMATION_EMAIL
```

The cancellation event is currently used primarily to invalidate/suppress previously created confirmation notifications.

Future notification types such as reminders, rescheduling, SMS, Push and WhatsApp must be architecturally possible but must NOT be implemented in this phase.

---

# 3. Non-goals

Do NOT implement:

- SMS
- Push notifications
- WhatsApp
- Reminder scheduling
- Marketing notifications
- Multi-provider failover
- Kubernetes
- Cloud deployment
- Production Kafka cluster
- Production Redis cluster
- Distributed tracing infrastructure
- Complex notification template management
- Multiple notification services/microservices

The local POC should establish the correct architecture without unnecessary complexity.

---

# 4. Technology Stack

## Application

```text
Node.js
TypeScript
NestJS
Fastify
```

NestJS is the application framework.

Fastify is the HTTP adapter.

---

## Database

```text
PostgreSQL
Prisma ORM
```

PostgreSQL is the **source of truth**.

The user already has PostgreSQL running locally.

Expected connection:

```text
localhost:5432
```

---

## Event streaming

```text
Apache Kafka
KafkaJS
```

Kafka is the event transport layer.

Use Kafka in **KRaft mode** for local development.

Do not introduce ZooKeeper.

Expected local broker:

```text
localhost:9092
```

---

## Job queue

```text
Redis
BullMQ
```

Redis is used by BullMQ for:

- email jobs
- retries
- delayed jobs
- worker coordination
- queue state

Expected local Redis:

```text
localhost:6379
```

---

## Email

Create an abstraction:

```text
EmailProvider
```

The first implementation may use a local/mock provider.

Do not hard-code a production email provider into the domain logic.

---

# 5. High-Level Architecture

```text
                         Client
                           |
                           | HTTP
                           v
                 +---------------------+
                 | NestJS + Fastify    |
                 | Booking API         |
                 +----------+----------+
                            |
                            v
                 +---------------------+
                 | Booking Service     |
                 | Business Logic      |
                 +----------+----------+
                            |
                     DB Transaction
                            |
             +--------------+--------------+
             |                             |
             v                             v
       bookings table               outbox_events
             |                             |
             +--------------+--------------+
                            |
                            v
                    Outbox Publisher
                            |
                            v
                    +---------------+
                    | Kafka         |
                    | booking.events|
                    +-------+-------+
                            |
                            v
                Notification Consumer
                            |
                            v
                Notification Service
                            |
                    PostgreSQL
                            |
                            v
                       BullMQ
                            |
                          Redis
                            |
                            v
                    Email Worker
                            |
                            v
                  Final Validation
                            |
                  +---------+---------+
                  |                   |
                VALID              INVALID
                  |                   |
                  v                   v
            Email Provider         EXPIRED
                  |
                  v
                SENT
```

---

# 6. Core Design Principle

Creating a notification does NOT mean sending it.

The system must have two separate decisions:

```text
1. Should a notification be created?
2. Is the notification still valid immediately before sending?
```

The second validation is mandatory.

This protects against:

- booking cancellation
- stale events
- duplicate events
- worker delays
- worker retries
- concurrent workers
- booking version changes

---

# 7. Project Structure

Use a modular monorepo-style backend structure.

```text
src/
├── app.module.ts
├── main.ts
│
├── modules/
│   │
│   ├── booking/
│   │   ├── booking.controller.ts
│   │   ├── booking.service.ts
│   │   ├── booking.repository.ts
│   │   ├── booking.module.ts
│   │   ├── dto/
│   │   └── domain/
│   │
│   ├── notification/
│   │   ├── notification.service.ts
│   │   ├── notification.repository.ts
│   │   ├── notification.module.ts
│   │   ├── notification-policy.ts
│   │   ├── notification-state-machine.ts
│   │   └── domain/
│   │
│   └── outbox/
│       ├── outbox.service.ts
│       ├── outbox.publisher.ts
│       └── outbox.module.ts
│
├── infrastructure/
│   ├── database/
│   │   └── prisma/
│   │
│   ├── kafka/
│   │   ├── kafka.client.ts
│   │   ├── kafka.producer.ts
│   │   └── kafka.consumer.ts
│   │
│   ├── redis/
│   │   └── redis.config.ts
│   │
│   ├── queue/
│   │   ├── email.queue.ts
│   │   └── queue.config.ts
│   │
│   └── email/
│       ├── email.provider.ts
│       └── mock-email.provider.ts
│
├── workers/
│   ├── notification.worker.ts
│   ├── email.worker.ts
│   └── outbox.worker.ts
│
└── common/
    ├── constants/
    ├── enums/
    ├── errors/
    └── utils/
```

Keep domain/business logic independent from Kafka, Redis and email provider implementations.

---

# 8. Booking Data Model

Create a booking model containing at least:

```text
Booking
-------------------------
id
tenantId
customerId
status
version
startAt
createdAt
updatedAt
```

Required statuses:

```text
PENDING
CONFIRMED
CANCELLED
```

`version` must increase whenever a booking lifecycle change invalidates previous notification work.

Example:

```text
version 1
CONFIRMED

version 2
CANCELLED
```

---

# 9. Outbox Data Model

Create:

```text
OutboxEvent
-------------------------
id
eventType
aggregateType
aggregateId
aggregateVersion
payload
createdAt
publishedAt
attemptCount
```

Important:

```text
publishedAt = null
```

means the event still needs to be published.

The booking update and outbox insertion MUST happen in the same PostgreSQL transaction.

---

# 10. Notification Data Model

Create:

```text
Notification
-------------------------
id
tenantId
bookingId
bookingVersion

type
channel

status

dedupeKey

payload

scheduledAt
expiresAt

attemptCount
lastError

createdAt
updatedAt
sentAt
```

Initial values:

```text
type:
BOOKING_CONFIRMATION

channel:
EMAIL
```

---

# 11. Notification Status

Use:

```text
CREATED
READY
PROCESSING
SENT
FAILED
RETRYING
EXPIRED
SUPPRESSED
DEAD_LETTER
```

Only allow valid state transitions.

Example:

```text
CREATED
   |
   v
READY
   |
   v
PROCESSING
   |
   +------> SENT
   |
   +------> RETRYING
   |
   +------> EXPIRED
```

A notification must never move from:

```text
EXPIRED -> READY
```

or:

```text
SENT -> READY
```

---

# 12. Idempotency / Deduplication

Every notification must have a deterministic `dedupeKey`.

For the current system:

```text
bookingId
+
bookingVersion
+
notificationType
+
channel
```

Example:

```text
B123:2:BOOKING_CONFIRMATION:EMAIL
```

Create a unique database constraint:

```text
UNIQUE(dedupeKey)
```

If Kafka delivers the same event multiple times, only one notification should be created.

---

# 13. Kafka Topics

Initial topic:

```text
booking.events
```

Events:

```text
BOOKING_CONFIRMED
BOOKING_CANCELLED
```

Do not create separate Kafka topics for every event at this stage.

Use:

```text
booking.events
```

with event type inside the message.

---

# 14. Kafka Event Contract

Example:

```json
{
  "eventId": "uuid",
  "eventType": "BOOKING_CONFIRMED",
  "aggregateType": "BOOKING",
  "aggregateId": "booking-id",
  "aggregateVersion": 1,
  "tenantId": "tenant-id",
  "occurredAt": "ISO-8601 timestamp",
  "payload": {
    "bookingId": "booking-id"
  }
}
```

For cancellation:

```json
{
  "eventId": "uuid",
  "eventType": "BOOKING_CANCELLED",
  "aggregateType": "BOOKING",
  "aggregateId": "booking-id",
  "aggregateVersion": 2,
  "tenantId": "tenant-id",
  "occurredAt": "ISO-8601 timestamp",
  "payload": {
    "bookingId": "booking-id"
  }
}
```

Use `bookingId` as Kafka message key so events for the same booking remain ordered within a partition.

---

# 15. Kafka Consumer Group

Notification consumer group:

```text
notification-service
```

The consumer must be horizontally scalable.

Local development may run one consumer.

The code must support multiple instances later.

---

# 16. Notification Event Processing

When:

```text
BOOKING_CONFIRMED
```

is consumed:

```text
1. Validate event
2. Load current booking
3. Check booking status
4. Check booking version
5. Generate dedupe key
6. Check/create notification
7. Add email job to BullMQ
```

Do not send the email directly from the Kafka consumer.

Kafka consumer responsibility:

```text
EVENT → NOTIFICATION/JOB
```

Email worker responsibility:

```text
JOB → EMAIL
```

---

# 17. Cancellation Event Processing

When:

```text
BOOKING_CANCELLED
```

is consumed:

```text
1. Load booking
2. Update/invalidate pending notifications
3. Mark relevant confirmation notifications as EXPIRED
4. Do not create a confirmation email
```

A cancellation event must never create a booking confirmation email.

---

# 18. BullMQ Queue

Create:

```text
email-notifications
```

Queue job name:

```text
send-booking-confirmation
```

Job payload:

```json
{
  "notificationId": "notification-id",
  "bookingId": "booking-id",
  "bookingVersion": 1
}
```

The job should contain identifiers, not large duplicated objects.

---

# 19. Redis Responsibility

Redis is not the source of truth.

Redis/BullMQ handles:

```text
Queue
Job state
Retry
Delayed execution
Worker coordination
Rate limiting
```

PostgreSQL remains authoritative for:

```text
Booking state
Notification state
Outbox state
Delivery state
```

If Redis is lost, notification state must still exist in PostgreSQL.

---

# 20. Email Worker Flow

Email worker continuously consumes BullMQ jobs.

```text
BullMQ
   |
   v
Email Worker
   |
   v
Load Notification
   |
   v
Load Current Booking
   |
   v
Final Validation
   |
   +---- invalid ----> EXPIRED/SUPPRESSED
   |
   +---- valid ------> Email Provider
```

---

# 21. Mandatory Final Validation

Immediately before sending an email:

```text
1. Notification exists?
2. Notification status is sendable?
3. Notification already sent?
4. Notification expired?
5. Booking exists?
6. Booking status is still CONFIRMED?
7. Booking version matches notification.bookingVersion?
8. Dedupe/idempotency check passes?
```

Only after all checks pass:

```text
SEND EMAIL
```

---

# 22. Critical Race Condition

Scenario:

```text
10:00
BOOKING_CONFIRMED
        |
        v
Notification created
        |
        v
BullMQ job created
        |
        |
10:01
BOOKING_CANCELLED
        |
        v
Booking status = CANCELLED
version = 2
        |
        |
Email Worker picks old job
        |
        v
Final validation
        |
        v
booking.status = CANCELLED
        |
        v
EXPIRED
        |
        v
DO NOT SEND
```

This behavior is mandatory.

---

# 23. Stale Event Handling

Scenario:

```text
CONFIRMED event
version = 1

CANCELLED event
version = 2

Old CONFIRMED event arrives late
```

The notification consumer must compare:

```text
event.aggregateVersion
```

against:

```text
booking.version
```

If:

```text
event.version < booking.version
```

the event is stale and must not create a new notification.

---

# 24. Duplicate Event Handling

If Kafka delivers:

```text
BOOKING_CONFIRMED
BOOKING_CONFIRMED
BOOKING_CONFIRMED
```

expected:

```text
ONE notification
ONE email
```

Use:

```text
eventId
+
dedupeKey
+
database unique constraints
```

Do not depend on Kafka alone to prevent duplicates.

---

# 25. Worker Failure

If email worker crashes:

```text
Worker
   |
   v
BullMQ job
   |
   X
worker crashes
```

BullMQ should make the job available for retry/reprocessing.

The application must remain idempotent.

---

# 26. Email Failure

Classify failures.

### Retryable

```text
429
500
502
503
504
timeout
network failure
```

Use:

```text
exponential backoff
+
jitter
+
maximum attempts
```

### Permanent

Examples:

```text
invalid email
invalid recipient
permanent provider rejection
```

Mark:

```text
FAILED
```

Do not endlessly retry permanent failures.

---

# 27. Dead Letter Handling

After maximum retry attempts:

```text
RETRYING
   |
   v
DEAD_LETTER
```

Store enough information for debugging:

```text
notificationId
bookingId
eventId
attemptCount
lastError
lastAttemptAt
```

For local POC, a database status is sufficient. A dedicated Kafka DLQ topic can be introduced later.

---

# 28. Email Provider Abstraction

Define:

```typescript
interface EmailProvider {
  send(input: EmailMessage): Promise<EmailSendResult>;
}
```

Implement:

```text
MockEmailProvider
```

for local development.

The notification system must not directly depend on:

```text
SES
SendGrid
Resend
```

The provider is injected through the abstraction.

---

# 29. Local Email Behavior

For the POC, do not require a real email provider.

The mock provider should:

```text
1. Log email
2. Generate fake providerMessageId
3. Return success/failure based on configurable test mode
```

Example:

```text
[EMAIL MOCK]
To: customer@test.com
Subject: Booking Confirmed
Notification: N1001
```

This allows complete testing without external services.

---

# 30. API Requirements

Implement only the APIs necessary to demonstrate the flow.

### Create/confirm booking

```http
POST /api/bookings
```

Behavior:

```text
Create booking
status = CONFIRMED
version = 1
create BOOKING_CONFIRMED outbox event
```

### Cancel booking

```http
POST /api/bookings/:id/cancel
```

Behavior:

```text
status = CANCELLED
increment version
create BOOKING_CANCELLED outbox event
```

### Get booking

```http
GET /api/bookings/:id
```

### Get notification

```http
GET /api/notifications/:id
```

This is primarily for demonstrating/debugging the POC.

---

# 31. Environment Configuration

Create:

```text
.env.example
```

Required configuration:

```env
NODE_ENV=development

PORT=3000

DATABASE_URL=postgresql://USER:PASSWORD@localhost:5432/booking_notification

KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=booking-notification-local
KAFKA_GROUP_ID=notification-service

REDIS_HOST=localhost
REDIS_PORT=6379

EMAIL_PROVIDER=mock
EMAIL_FROM=no-reply@local.test

NOTIFICATION_MAX_RETRIES=5
NOTIFICATION_RETRY_DELAY_MS=1000
```

Do not commit real secrets.

---

# 32. Kafka Configuration

For local development:

```text
broker:
localhost:9092

topic:
booking.events

consumer group:
notification-service
```

Producer configuration should enable idempotent production where supported.

Consumer processing must still be idempotent.

---

# 33. BullMQ Configuration

Queue:

```text
email-notifications
```

Default behavior:

```text
attempts = configurable
backoff = exponential
removeOnComplete = configurable
removeOnFail = false
```

Do not aggressively delete failed jobs during local development because they are useful for debugging.

---

# 34. Database Transactions

Booking confirmation/cancellation MUST use a single PostgreSQL transaction.

Example conceptual flow:

```text
BEGIN

UPDATE booking

INSERT outbox_event

COMMIT
```

Never:

```text
UPDATE booking
COMMIT

publish Kafka
```

as the only mechanism.

---

# 35. Concurrency Requirements

The system must safely handle:

```text
Two confirmation requests
Confirmation + cancellation simultaneously
Cancellation + old Kafka event
Duplicate Kafka event
Multiple email workers
Worker retry
Provider timeout
```

Use PostgreSQL transactions/locking and unique constraints where necessary.

---

# 36. Observability

Every operation should carry:

```text
requestId
eventId
bookingId
notificationId
jobId
```

Logs should make it possible to trace:

```text
HTTP request
   ↓
Booking
   ↓
Outbox
   ↓
Kafka event
   ↓
Notification
   ↓
BullMQ job
   ↓
Email worker
   ↓
Provider
```

For local POC, structured JSON logging is sufficient.

---

# 37. Logging Examples

Use structured logs:

```json
{
  "event": "notification.created",
  "notificationId": "N1001",
  "bookingId": "B123",
  "bookingVersion": 1,
  "type": "BOOKING_CONFIRMATION",
  "channel": "EMAIL"
}
```

Cancellation:

```json
{
  "event": "notification.expired",
  "notificationId": "N1001",
  "bookingId": "B123",
  "reason": "BOOKING_CANCELLED"
}
```

Email:

```json
{
  "event": "email.sent",
  "notificationId": "N1001",
  "providerMessageId": "mock-123"
}
```

---

# 38. Testing Requirements

The implementation is not complete until these tests pass.

## Happy path

```text
Confirm booking
→ outbox event
→ Kafka
→ notification
→ BullMQ
→ email worker
→ email SENT
```

## Cancellation before notification creation

```text
Confirm
Cancel
Old event arrives
→ no email
```

## Cancellation after notification creation

```text
Confirm
Notification created
Cancel
Worker picks job
→ EXPIRED
→ no email
```

## Cancellation while worker is processing

```text
Worker starts
Booking cancelled
Final validation
→ no email
```

## Duplicate event

```text
Same Kafka event twice
→ one notification
→ one email
```

## Stale event

```text
version 1 event
current booking version 2
→ event ignored
```

## Worker failure

```text
Worker crashes
→ job retry
```

## Email provider timeout

```text
timeout
→ retry
→ eventual success
```

## Permanent email failure

```text
invalid recipient
→ FAILED
→ no infinite retry
```

## Redis restart

```text
Redis unavailable
→ application does not corrupt PostgreSQL state
→ jobs can be recovered/reconciled according to implementation
```

---

# 39. Acceptance Criteria

The implementation is considered successful when:

1. Booking confirmation creates a PostgreSQL outbox event.
2. Outbox worker publishes the event to Kafka.
3. Notification consumer receives the Kafka event.
4. Exactly one notification is created for duplicate events.
5. Notification creates an email job in BullMQ.
6. Email worker consumes the job.
7. Worker performs final booking validation.
8. Confirmed booking results in a mock email.
9. Cancelled booking results in no email.
10. Stale booking versions cannot generate emails.
11. Failed email jobs retry.
12. Permanent failures stop retrying.
13. Worker crashes do not permanently lose jobs.
14. All important operations are traceable using IDs.
15. All services can run locally using the documented local infrastructure.
16. No production/cloud dependency is required.

---

# 40. Implementation Order

Antigravity should implement in this order:

```text
Phase 1
Project + NestJS + Fastify
        ↓
Phase 2
Prisma + PostgreSQL
        ↓
Phase 3
Booking APIs
        ↓
Phase 4
Transactional Outbox
        ↓
Phase 5
Kafka producer/consumer
        ↓
Phase 6
Notification domain
        ↓
Phase 7
Redis + BullMQ
        ↓
Phase 8
Email worker + Mock Email Provider
        ↓
Phase 9
Final validation + race-condition handling
        ↓
Phase 10
Retries + failure handling
        ↓
Phase 11
Integration tests
        ↓
Phase 12
End-to-end test scenarios
```

Do not implement future channels/features until this complete flow is working and tested.

---

# 41. Architectural Rules for Antigravity

The implementation MUST follow these rules:

1. Booking module must not directly send emails.
2. Kafka consumer must not directly call the email provider.
3. PostgreSQL is the source of truth.
4. Redis is not the source of truth.
5. Kafka events must be idempotently consumed.
6. Notification creation must be idempotent.
7. Email sending must be separated from event consumption.
8. Final notification validation must happen immediately before sending.
9. Booking version must be used to invalidate stale notifications.
10. Cancellation must invalidate pending confirmation notifications.
11. Database state changes and outbox creation must be one transaction.
12. Email provider must be accessed through an interface.
13. No hard-coded infrastructure credentials.
14. No unnecessary microservices for the local POC.
15. Keep business logic independent of Kafka/Redis/BullMQ implementations.
16. Code should be structured so workers can later be horizontally scaled.
17. Do not introduce reminders, SMS, Push or WhatsApp in this phase.
18. Do not implement production cloud infrastructure in this phase.

---

# 42. Final Expected Local Flow

```text
CLIENT
  |
  | POST /api/bookings
  v
NESTJS + FASTIFY
  |
  v
BOOKING SERVICE
  |
  | PostgreSQL transaction
  +-----------------------------+
  |                             |
  v                             v
BOOKING TABLE              OUTBOX EVENT
                                |
                                v
                         OUTBOX WORKER
                                |
                                v
                              KAFKA
                         booking.events
                                |
                                v
                    NOTIFICATION CONSUMER
                                |
                                v
                       NOTIFICATION TABLE
                                |
                                v
                            BULLMQ
                                |
                                v
                             REDIS
                                |
                                v
                         EMAIL WORKER
                                |
                                v
                       FINAL VALIDATION
                         /            \
                     VALID          INVALID
                       |               |
                       v               v
                 MOCK EMAIL         EXPIRED
                       |
                       v
                      SENT
```

This is the **only architecture Antigravity should implement for Phase 1**. The design must remain extensible for future notification types and channels, but future functionality should not be built now.