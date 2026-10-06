# Architecture Overview

This project implements an **Event-Driven Notification System** using the **Transactional Outbox Pattern**.

The goal of the system is to guarantee that when a Booking is created or updated, the customer *always* gets an email, even if services crash, the database restarts, or the email provider is temporarily down.

## The 5-Step Flow

1. **Booking API**: A customer creates a booking via `POST /api/bookings`.
2. **Database (Outbox)**: The system saves the booking to the database AND saves an "Event" in the Outbox table in the exact same transaction.
3. **Message Broker (Kafka)**: A background process reads the Outbox table and publishes the event to a Kafka topic (`booking.events`).
4. **Notification Brain**: A Kafka consumer picks up the event, decides if it's a duplicate (idempotency), creates a `Notification` record in the database, and sends a job to a Redis Queue.
5. **Email Worker (BullMQ)**: A worker picks up the job from Redis, verifies the booking hasn't been cancelled in the meantime, and sends the actual email. If it fails, it retries.

## Directory Structure Overview

The code is strictly separated into domain modules:
*   `src/modules/booking/` - Handles the API and Outbox writing.
*   `src/infrastructure/kafka/` - Handles Kafka connectivity.
*   `src/modules/notification/` - The core logic for deciding when and how to notify.
*   `src/workers/` - Background processes pulling from Redis.
