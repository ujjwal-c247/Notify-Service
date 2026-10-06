# Booking & Outbox Module

**Folder:** `src/modules/booking/` & `src/modules/outbox/`

This module is the entry point of the system. It exposes the REST API to create and cancel bookings.

## Important Logic & Conditions

### 1. The Transactional Outbox Pattern
**File:** `booking.repository.ts`

When a booking is created, we cannot just save it to Postgres and then send a message to Kafka directly. If the app crashes between those two steps, the booking exists but the notification is lost forever.

**The Solution:**
We use Prisma's `$transaction` to write BOTH the `Booking` and the `OutboxEvent` to the Postgres database at the exact same time. 
Because Postgres guarantees ACID transactions, it's physically impossible to save the booking without saving the event.

### 2. Event Payload Structure
When saving the Outbox event, we attach the `aggregateVersion`. Every time a booking is updated (e.g. cancelled), its `version` integer goes up by 1. 

**Condition:**
The version is critical. If we receive a cancellation event (version 2) before a creation event (version 1), the version numbers tell the rest of the system how to handle the out-of-order data.

### 3. Outbox Publisher (Cron Job)
**File:** `outbox.publisher.ts`

A background timer runs continuously. It looks for rows in the `OutboxEvent` table where `publishedAt == null`. 
It grabs those rows, sends them to Kafka using `KafkaProducer`, and then updates `publishedAt = current_time`.
