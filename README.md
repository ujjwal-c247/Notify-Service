# Local Booking Notification System

A production-oriented event-driven Local Proof-of-Concept (POC) for a **Booking Notification System** built with **NestJS**, **Fastify**, **Prisma (PostgreSQL)**, **Kafka (KafkaJS)**, and **BullMQ (Redis)**.

---

## 1. Project Overview & Purpose

The goal of this system is to reliably manage booking creation, confirmation, and cancellation notifications in an asynchronous, event-driven architecture.

### Key Capabilities:
- **Transactional Outbox Pattern**: Ensures dual-write safety by transactionally persisting domain model changes and outbox events in a single PostgreSQL transaction.
- **Asynchronous Event Delivery**: Decouples HTTP request handling from background notification processing via Apache Kafka.
- **Race Condition Prevention (Mandatory Final Validation)**: Re-validates the latest booking state in PostgreSQL directly inside the worker before delivering emails (e.g. preventing confirmation emails for cancelled bookings).
- **Idempotency & Deduplication**: Database-enforced `dedupeKey` (`UNIQUE` constraint) prevents duplicate notifications across retries or repeated Kafka events.
- **State Machine Rules**: Enforces strict lifecycle transitions (`CREATED` -> `READY` -> `PROCESSING` -> `SENT` / `EXPIRED` / `FAILED`).

---

## 2. Architecture Overview

### Event & Data Processing Flow
```
[Client Request]
       │
       ▼
[Booking Controller / Service]
       │
       ▼ (Single Database Transaction)
┌───────────────────────────────────────┐
│ PostgreSQL Database                  │
│  ├─ Booking Table (CONFIRMED / v1)    │
│  └─ OutboxEvent Table (BOOKING_CONF.) │
└───────────────────────────────────────┘
       │
       ▼ (Polling via OutboxPublisher)
[Apache Kafka Topic: booking.events]
       │
       ▼ (KafkaConsumer)
[Notification Service] ───► Database (Create Notification: CREATED -> READY)
       │
       ▼
[BullMQ Queue: email-notifications (Redis)]
       │
       ▼ (EmailWorker)
┌────────────────────────────────────────────────────────┐
│ MANDATORY FINAL VALIDATION                              │
│ Check latest Booking status in PostgreSQL               │
│ - If CANCELLED or version mismatch ──► Mark EXPIRED     │
│ - If CONFIRMED & version match     ──► Send Email & SENT│
└────────────────────────────────────────────────────────┘
       │
       ▼
[Mock Email Provider]
```

---

## 3. Technology Stack

- **Framework**: NestJS v10 + `@nestjs/platform-fastify`
- **Database & ORM**: PostgreSQL + Prisma ORM v5
- **Message Broker**: Apache Kafka + `kafkajs`
- **Background Queue & Cache**: Redis + `bullmq` / `ioredis`
- **Language**: TypeScript (Node.js ES2021)
- **Testing**: Jest + `@nestjs/testing`

---

## 4. Repository & Project Structure

```
├── prisma/
│   └── schema.prisma                 # Database schema (Booking, OutboxEvent, Notification)
├── src/
│   ├── common/                       # Enums, interfaces, domain errors, and HTTP filters
│   │   ├── enums/
│   │   ├── errors/
│   │   ├── filters/
│   │   └── interfaces/
│   ├── infrastructure/               # External adapters (Prisma, Kafka, Redis, Email)
│   │   ├── database/
│   │   ├── email/
│   │   ├── kafka/
│   │   └── redis/
│   ├── modules/                      # Feature modules
│   │   ├── booking/                  # Booking domain API & transaction handling
│   │   ├── notification/             # Notification state machine, policy, service & repo
│   │   └── outbox/                   # Outbox polling publisher
│   ├── workers/                      # Async background job workers (BullMQ EmailWorker)
│   ├── health/                       # Application health check endpoint
│   ├── app.module.ts                 # Main NestJS root module
│   └── main.ts                       # Fastify application entrypoint
├── test/                             # End-to-end integration test suite
│   ├── app.e2e-spec.ts
│   └── jest-e2e.json
├── package.json
└── tsconfig.json
```

---

## 5. Prerequisites

- **Node.js**: `v18.x` or `v20.x`
- **npm**: `v9.x` or higher
- **PostgreSQL**: `v14+` running locally on port `5432`
- **Apache Kafka**: `v3.x` running locally on port `9092`
- **Redis**: `v6+` running locally on port `6379`

---

## 6. Required Local Services

Make sure the following local services are running on standard ports before launching the application:

| Service | Host | Port | Default DB / Configuration |
|---|---|---|---|
| PostgreSQL | `localhost` | `5432` | Database: `booking_notification` |
| Kafka Broker | `localhost` | `9092` | Auto-creates topic: `booking.events` |
| Redis | `localhost` | `6379` | Default DB index 0 |

---

## 7. Required Environment Variables

Create a `.env` file in the project root containing:

```env
NODE_ENV=development
PORT=3000

DATABASE_URL="postgresql://postgres:postgres@localhost:5432/booking_notification?schema=public"

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

---

## 8. Database Setup & Prisma Commands

Generate Prisma Client artifacts:
```bash
npm run prisma:generate
```

Push Prisma schema to PostgreSQL:
```bash
npx prisma db push
```

Run dev database migrations:
```bash
npm run prisma:migrate
```

---

## 9. How to Start the Application & Processes

### Starting the API Server
```bash
npm run start:dev
```
The NestJS Fastify HTTP server starts on `http://localhost:3000`.

### Background Services Execution
All components (`OutboxPublisher`, `KafkaConsumer`, and `EmailWorker`) are registered as providers inside `AppModule` and start automatically when running `npm run start:dev`.

---

## 10. Outbox Publisher

- **Location**: `src/modules/outbox/outbox.publisher.ts`
- **Behavior**: Periodically queries unpublished outbox records (`publishedAt IS NULL`) from PostgreSQL every 1 second (configurable interval) and sends them to Kafka topic `booking.events`. Upon successful Kafka dispatch, `publishedAt` is populated.

---

## 11. Kafka Notification Consumer

- **Location**: `src/infrastructure/kafka/kafka.consumer.ts`
- **Topic**: `booking.events`
- **Consumer Group**: `notification-service`
- **Behavior**: Consumes events emitted by `OutboxPublisher` and routes them to `NotificationService.processBookingConfirmedEvent` or `processBookingCancelledEvent`.

---

## 12. Email Worker

- **Location**: `src/workers/email.worker.ts`
- **Queue**: `email-notifications`
- **Behavior**: Processes jobs enqueued by `NotificationService`. Re-verifies booking state in PostgreSQL before calling `MockEmailProvider`.

---

## 13. How to Run All Processes Locally

```bash
# 1. Install dependencies
npm install

# 2. Sync database schema
npx prisma db push

# 3. Start development server (runs API, Outbox Publisher, Kafka Consumer, & BullMQ Worker)
npm run start:dev
```

---

## 14. Available API Endpoints

### 1. Health Check
- **`GET /api/health`**
- **Response `200 OK`**:
```json
{
  "status": "ok",
  "timestamp": "2026-10-06T10:00:00.000Z"
}
```

### 2. Create Booking
- **`POST /api/bookings`**
- **Request Payload**:
```json
{
  "tenantId": "tenant-1",
  "customerId": "cust-100",
  "startAt": "2026-10-10T10:00:00.000Z"
}
```
- **Response `201 Created`**:
```json
{
  "id": "b73a9032-15f1-48ba-a5db-87612f001234",
  "tenantId": "tenant-1",
  "customerId": "cust-100",
  "status": "CONFIRMED",
  "version": 1,
  "startAt": "2026-10-10T10:00:00.000Z",
  "createdAt": "2026-10-06T10:00:00.000Z",
  "updatedAt": "2026-10-06T10:00:00.000Z"
}
```

### 3. Cancel Booking
- **`POST /api/bookings/:id/cancel`**
- **Response `201 Created`**:
```json
{
  "id": "b73a9032-15f1-48ba-a5db-87612f001234",
  "tenantId": "tenant-1",
  "customerId": "cust-100",
  "status": "CANCELLED",
  "version": 2,
  "startAt": "2026-10-10T10:00:00.000Z",
  "createdAt": "2026-10-06T10:00:00.000Z",
  "updatedAt": "2026-10-06T10:00:01.000Z"
}
```

### 4. Get Booking Details
- **`GET /api/bookings/:id`**

### 5. Get Notification Details
- **`GET /api/notifications/:id`**

---

## 15. Example Booking Confirmation Flow

1. Client sends `POST /api/bookings`.
2. `BookingService` executes a PostgreSQL transaction:
   - Inserts `Booking` (`status: CONFIRMED`, `version: 1`).
   - Inserts `OutboxEvent` (`eventType: BOOKING_CONFIRMED`).
3. `OutboxPublisher` picks up the outbox event and publishes it to Kafka topic `booking.events`.
4. `KafkaConsumer` receives `BOOKING_CONFIRMED` event and calls `NotificationService.processBookingConfirmedEvent`.
5. `NotificationService` creates a `Notification` record in state `READY` with a unique `dedupeKey` (`<bookingId>:1:BOOKING_CONFIRMATION:EMAIL`) and enqueues a BullMQ job `email-notifications`.
6. `EmailWorker` executes job:
   - Performs **Final Validation** against PostgreSQL.
   - Verifies booking is still `CONFIRMED` and `version == 1`.
   - Sends email via `MockEmailProvider`.
   - Updates `Notification` status to `SENT`.

---

## 16. Example Cancellation / Race-Condition Flow

1. Booking is created (`version: 1`).
2. `OutboxPublisher` publishes `BOOKING_CONFIRMED` to Kafka.
3. Notification service enqueues BullMQ job for v1 confirmation email.
4. **Before EmailWorker processes job**, client calls `POST /api/bookings/:id/cancel`.
5. Booking is updated to `CANCELLED` (`version: 2`) in PostgreSQL.
6. `EmailWorker` picks up the queued v1 job:
   - Executes **Final Validation**: Queries PostgreSQL for booking status.
   - Sees current status is `CANCELLED` (or version is 2 != 1).
   - Marks Notification as **`EXPIRED`**.
   - **Does NOT send email**.

---

## 17. Kafka Topics & Consumer Groups

- **Topic Name**: `booking.events`
- **Consumer Group ID**: `notification-service`
- **Event Types**:
  - `BOOKING_CONFIRMED`
  - `BOOKING_CANCELLED`

---

## 18. BullMQ / Redis Queue Information

- **Queue Name**: `email-notifications`
- **Job Data Payload**:
```json
{
  "notificationId": "uuid",
  "bookingId": "uuid",
  "bookingVersion": 1
}
```
- **Job ID Format**: `job_<sanitized_dedupeKey>`

---

## 19. Notification State Machine

Valid lifecycle state transitions:

```
[CREATED] ──► [READY] ──► [PROCESSING] ──► [SENT]
                 │              │
                 ├─► [EXPIRED]  ├─► [EXPIRED]
                 │              ├─► [FAILED]
                 └─► [SUPPRESSED]└─► [RETRYING]
```

Forbidden transitions:
- `EXPIRED` -> `READY`
- `SENT` -> `READY`
- `DEAD_LETTER` -> `READY`

---

## 20. Testing Commands

### Unit Tests
```bash
npm test
```

### End-to-End (E2E) Integration Tests
```bash
npm run test:e2e
```

---

## 21. Running Unit & E2E Tests

The E2E test suite (`test/app.e2e-spec.ts`) covers:
1. **Happy Path**: Complete end-to-end confirmation and email dispatch.
2. **Race Condition Check**: Instant cancellation invalidating pending confirmation jobs.
3. **Deduplication**: Duplicate Kafka events resulting in exactly 1 email.
4. **Stale Event Prevention**: Ignoring obsolete event versions.
5. **Failure Classification**: Provider error categorization (`400` permanent failure vs `503` retryable).
6. **Already SENT Safety**: Preventing re-sending sent notifications.

---

## 22. Troubleshooting Common Local-Development Issues

1. **PostgreSQL Authentication Error**:
   - Ensure PostgreSQL is running on port 5432 and user credentials match `.env`.
2. **Kafka Group Coordinator Warning / Error**:
   - If running single-node local Kafka, ensure broker auto-creates topics or create topic `booking.events` manually.
3. **Redis Connection Refused**:
   - Verify Redis service is running locally (`redis-cli ping` returns `PONG`).

---

## 23. Important Architectural Rules

- **PostgreSQL is Single Source of Truth**: Email delivery decision depends strictly on fresh PostgreSQL database state, not event payload snapshots.
- **Transactional Outbox Dual-Write Protection**: Outbox events must be written within the same DB transaction as domain entity mutations.
- **Strict State Machine**: Notifications cannot regress to prior states or mutate terminal states (`SENT`, `EXPIRED`, `FAILED`).
- **Sanitized Dedupe Keys**: Unique keys format `<bookingId>:<version>:<type>:<channel>` ensure exact-once processing per event version.

---

## 24. Limitations & Out-of-Scope Features

- SMS / Push Channels (POC focuses on Email channel abstraction).
- Real SMTP / External Email Integration (POC uses `MockEmailProvider`).
- Multi-region Kafka replication.

---

## Quick Start

```bash
# 1. Ensure local services (Postgres, Kafka, Redis) are running

# 2. Sync database schema
npx prisma db push

# 3. Run all tests
npm test && npm run test:e2e

# 4. Launch development server
npm run start:dev
```
