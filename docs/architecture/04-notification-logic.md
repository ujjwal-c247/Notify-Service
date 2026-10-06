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

**Condition:** Right before the worker actually sends the email, it queries the database for the current `Booking.version`. 
If the current version of the booking is higher than the version the notification was generated for (e.g., DB version is 2, but this email is for version 1), it aborts the send and marks the notification as `EXPIRED`. This prevents sending "Booking Confirmed" emails for cancelled bookings.
