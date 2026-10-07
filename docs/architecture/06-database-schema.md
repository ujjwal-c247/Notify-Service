# Database Schema Design

**File:** `prisma/schema.prisma`

Our Postgres database is designed around event-driven durability and idempotency. Below is a breakdown of the three core tables, their critical keys, and how they are queried.

---

## 1. `Booking` (The Domain Entity)

This table stores the core business state of an appointment.

### Critical Keys & Design:
*   `id` (UUID): Primary key.
*   `status` (Enum): Tracks the lifecycle (`PENDING`, `CONFIRMED`, `CANCELLED`).
*   `version` (Int): **Crucial for concurrency.** Defaults to 1. Every time a booking is updated (e.g. cancelled), the version increments (2, 3, etc.). This ensures that downstream workers know exactly what version of the booking they are looking at, preventing stale notifications from being sent out of order.
*   **Indexes (`@@index`)**: `tenantId` (for multi-tenant data isolation), `customerId` (to quickly fetch a user's history), and `status` (for dashboard aggregations).

### Common Queries:
*   **Create**: Always executed in a `$transaction` alongside an `OutboxEvent`.
*   **Update**: Increments the `version` field while updating the `status`.

---

## 2. `OutboxEvent` (The Reliable Message Queue)

This table implements the Transactional Outbox Pattern, serving as a staging area before messages are sent to Kafka.

### Critical Keys & Design:
*   `id` (UUID): Primary key.
*   `aggregateId` / `aggregateType`: Identifies what the event belongs to (e.g., `aggregateId = <bookingId>`, `aggregateType = 'BOOKING'`).
*   `aggregateVersion` (Int): Captures the exact version of the booking *at the exact moment the event occurred*.
*   `payload` (JSON): The full message data that Kafka will need.
*   `publishedAt` (DateTime?): **Crucial for the cron job.** Starts as `null`. When the background worker successfully sends the event to Kafka, it stamps this column with the current timestamp.

### Common Queries:
*   **Write**: Written by `booking.repository.ts` in the same transaction as the booking.
*   **Polling (The Publisher)**: `SELECT * FROM outbox_events WHERE publishedAt IS NULL ORDER BY createdAt ASC LIMIT 20`. This ensures chronological processing.
*   **Pruning (The Sweeper)**: `DELETE FROM outbox_events WHERE publishedAt <= '7 days ago'`. Keeps the database lightweight.

---

## 3. `Notification` (The Idempotent Ledger)

This table tracks the exact lifecycle of a notification (like an email) moving through the BullMQ worker system.

### Critical Keys & Design:
*   `id` (UUID): Primary key.
*   `bookingId` / `bookingVersion`: Links back to the exact version of the booking that triggered this notification. If the current DB booking version is higher than this column, we know the notification is stale!
*   `status` (Enum): Tracks the email (`READY`, `PROCESSING`, `SENT`, `FAILED`, `EXPIRED`, `RETRYING`).
*   `dedupeKey` (String, `@unique`): **Crucial for idempotency.** Formatted as `bookingId:version:type:channel`. Because Kafka provides *at-least-once* delivery, it might send the same event twice. By making this column unique, the database outright rejects duplicate events, ensuring we never spam a customer.
*   `attemptCount` (Int) / `lastError` (String): Used to track retries if the external MockEmailProvider fails.

### Common Queries:
*   **Upsert / Create**: We use a `create` operation and catch unique constraint violations to silently drop duplicates.
*   **Status Updates**: When BullMQ finishes a job, it updates the `status` to `SENT` or `FAILED`.
