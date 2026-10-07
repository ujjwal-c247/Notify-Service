# Component Reference Guide

This document is a dictionary of every major class, function, and variable in the system. Use this to quickly find out *where* something is and *what* it does.

---

## 1. Booking Module
**Location:** `src/modules/booking/`

### `BookingController` (`booking.controller.ts`)
*   **Purpose:** The REST API entry point.
*   **Functions:**
    *   `create()`: Receives `POST /api/bookings`. Calls `BookingService.createBooking()`.
    *   `cancel()`: Receives `POST /api/bookings/:id/cancel`. Calls `BookingService.cancelBooking()`.

### `BookingService` (`booking.service.ts`)
*   **Purpose:** Business logic for bookings.
*   **Functions:**
    *   `createBooking(dto)`: Validates input, builds the `OutboxEvent` payload, and calls the repository.
    *   `cancelBooking(id)`: Fetches the booking, checks if it's already cancelled, increments the version, and calls the repository.

### `BookingRepository` (`booking.repository.ts`)
*   **Purpose:** Direct database access.
*   **Functions:**
    *   `createWithOutbox()`: Uses Prisma `$transaction` to safely insert a Booking and Outbox row simultaneously.
    *   `updateStatusWithOutbox()`: Updates a booking and inserts an Outbox row simultaneously.

---

## 2. Outbox Module
**Location:** `src/modules/outbox/`

### `OutboxService` (`outbox.service.ts`)
*   **Purpose:** Database helper for Outbox events.
*   **Functions:**
    *   `fetchUnpublishedEvents(limit)`: Gets events where `publishedAt == null`.
    *   `markAsPublished(id)`: Stamps `publishedAt = new Date()`.
    *   `pruneOldOutboxEvents()`: Deletes rows older than 7 days.

### `OutboxPublisher` (`outbox.publisher.ts`)
*   **Purpose:** Background cron jobs for the Outbox.
*   **Variables:**
    *   `timer`: A `setInterval` that triggers `processOutbox` every 1 second.
    *   `isProcessing`: A boolean lock to prevent overlapping runs.
*   **Functions:**
    *   `processOutbox()`: Loops through unpublished events and sends them to `KafkaProducer`.
    *   `@Cron pruneOldOutboxEvents()`: Triggers the DB sweeper at 2 AM every day.

---

## 3. Kafka Infrastructure
**Location:** `src/infrastructure/kafka/`

### `KafkaClient` (`kafka.client.ts`)
*   **Purpose:** Core connection management to the Kafka broker.
*   **Variables:** `kafka` (the raw Kafkajs instance), `isConnected` (boolean).
*   **Functions:**
    *   `connect()`: Connects the Producer, Consumer, and Admin to `localhost:9092`.
    *   `ensureTopicsExist()`: Auto-creates the `booking.events` topic if it's missing.

### `KafkaProducer` (`kafka.producer.ts`)
*   **Functions:**
    *   `sendEvent(event)`: Packages the JSON payload and uses `producer.send()` to push it to the `booking.events` topic.

### `KafkaConsumer` (`kafka.consumer.ts`)
*   **Variables:** `KAFKA_GROUPS.NOTIFICATION_SERVICE` (The consumer group ID).
*   **Functions:**
    *   `eachMessage()`: Fires every time a new message arrives from Kafka. Parses the JSON and passes it to `NotificationService`.

---

## 4. Notification Module
**Location:** `src/modules/notification/`

### `NotificationService` (`notification.service.ts`)
*   **Purpose:** The core brain for idempotency and state.
*   **Functions:**
    *   `processBookingConfirmedEvent()`: Generates a `dedupeKey`, saves the `Notification` to Prisma (status `READY`), and pushes a job to `EmailQueue`.
    *   `processEmailJob()`: The worker calls this. It does the **Stale Check** against the Booking table. If valid, changes status to `PROCESSING` and calls the `EmailProvider`.
    *   `markAsSent()` / `markAsFailed()`: Updates the final terminal state in Postgres.

### `NotificationPolicy` (`notification-policy.ts`)
*   **Functions:**
    *   `generateDedupeKey(bookingId, version, type, channel)`: Creates the exact unique string used to block duplicate events.

---

## 5. Queue & Worker
**Location:** `src/infrastructure/queue/` & `src/workers/`

### `EmailQueue` (`email.queue.ts`)
*   **Purpose:** Connects to Redis and manages the BullMQ instance.
*   **Variables:**
    *   `queue`: The BullMQ instance.
    *   `removeOnComplete` / `removeOnFail`: Configuration telling Redis how long to keep finished jobs.
*   **Functions:**
    *   `addEmailJob(data)`: Takes a notification ID and pushes it into Redis.

### `EmailWorker` (`email.worker.ts`)
*   **Purpose:** Pulls jobs out of Redis.
*   **Variables:** `worker`: The BullMQ Worker instance listening to the `email-notifications` queue.
*   **Functions:**
    *   `process(job)`: BullMQ calls this automatically when a job is ready. It extracts the ID and passes it back to `NotificationService.processEmailJob()`.

---

## 6. Email Provider
**Location:** `src/infrastructure/email/`

### `MockEmailProvider` (`mock-email.provider.ts`)
*   **Purpose:** A fake SMTP server for local testing.
*   **Variables:**
    *   `failureRate`: (0-100) The percentage of times it will intentionally throw an error.
    *   `failureMode`: (`none`, `timeout`, `retryable`, `permanent`) Dictates *what kind* of error to throw.
*   **Functions:**
    *   `send(to, subject)`: Simulates sending an email. Logs `email.sent_mock` to the console. If `failureRate` triggers, it throws an error to test the BullMQ retry logic.
