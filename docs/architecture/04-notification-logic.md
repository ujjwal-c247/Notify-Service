# Notification Core Logic

**Folder:** `src/modules/notification/`

This is the "brain" of the system. It receives events from Kafka and decides whether an email should actually be sent.

## Important Logic & Conditions

### 1. Deterministic Idempotency
**File:** `notification-policy.ts`

Because Kafka might deliver the same event twice, we must prevent spamming the user. 
When an event arrives, we generate a highly predictable `dedupeKey`:
```typescript
`${bookingId}:${bookingVersion}:${NotificationType}:${NotificationChannel}`
```
Example: `b123:1:BOOKING_CONFIRMATION:EMAIL`

**Condition:** We use Prisma's `create` with this `dedupeKey` marked as `@unique` in the database schema. If Kafka delivers the event twice, the second attempt will violate the database unique constraint and crash harmlessly. We catch the error and safely ignore the duplicate.

### 2. State Machine Enforcement
**File:** `notification-state-machine.ts`

A notification has a strict lifecycle:
`READY` -> `SENT` (Terminal)
`READY` -> `FAILED` (Terminal)
`READY` -> `EXPIRED` (Terminal)
`READY` -> `RETRYING` -> `SENT`

**Condition:** The state machine code prevents illegal transitions. If an email is already marked `SENT`, trying to mark it `RETRYING` will throw a Domain Error.

### 3. Stale Event / Race Condition Protection
**File:** `notification.service.ts` -> `processEmailJob()`

Imagine this scenario:
1. Customer creates a booking.
2. The "Create" email sits in a queue for 30 seconds.
3. Customer instantly cancels the booking 5 seconds later.

If the current version of the booking is higher than the version the notification was generated for (e.g., DB version is 2, but this email is for version 1), it aborts the send and marks the notification as `EXPIRED`. This prevents sending "Booking Confirmed" emails for cancelled bookings.

### 4. Scheduled Reminders (Database Polling Pattern)
**File:** `notification.service.ts` -> `dispatchScheduledReminders()`

Instead of dumping future jobs into BullMQ and consuming expensive Redis RAM, long-term reminders are stored in the PostgreSQL database.

**Pattern Flow:**
1. Upon booking confirmation, a `BOOKING_REMINDER` row is created with a `scheduledAt` timestamp (2 hours before the booking starts). Status is `CREATED`.
2. A Cron Job runs every minute using `@nestjs/schedule`.
3. It fetches all `CREATED` notifications where `scheduledAt <= NOW()`.
4. It updates their status to `READY` and pushes them into the BullMQ queue for immediate processing.
5. If the booking was cancelled or updated before the reminder was due, the standard Stale Event check (Section 3 above) will catch it when the worker processes the queue, preventing incorrect emails from firing.
