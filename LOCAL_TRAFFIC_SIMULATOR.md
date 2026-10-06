# Local Traffic Simulator

The Local Traffic Simulator is a development tool built to stress-test and verify the behavior of the `notify-service` modular monolith under various load conditions and simulated failure scenarios. It evaluates the complete lifecycle of notifications, starting from booking creation through the outbox pattern, Kafka event consumption, and BullMQ email job execution.

This is a **local-only** tool and relies on `NODE_ENV !== 'production'`.

## Architecture Overview

The simulator orchestrates traffic by directly calling the local development HTTP endpoints (`/api/bookings`, `/api/bookings/:id/cancel`). 
Because it interacts with the system strictly at the HTTP boundary, it authentically exercises the full application stack:
1. **Booking Creation:** HTTP API -> Transaction -> Booking + Outbox Event.
2. **Event Publishing:** Outbox cron -> Kafka Producer (`booking.events`).
3. **Notification Processing:** Kafka Consumer -> Notification Service -> BullMQ.
4. **Email Delivery:** BullMQ Email Worker -> MockEmailProvider.

Metrics are subsequently queried from the Database, Redis BullMQ, and the MockEmailProvider to present a cohesive report on success rates, queue behavior, and data consistency.

## Usage

You can invoke the traffic simulator using the following npm script:

```bash
npm run traffic:test -- [options]
```

### Options

| Flag | Description | Default | Example |
|------|-------------|---------|---------|
| `--mode` | Traffic generation mode: `burst` (all at once) or `continuous` (time-sliced intervals). | `burst` | `--mode=continuous` |
| `--rate` | Number of bookings to create per second. | `10` | `--rate=50` |
| `--duration` | Duration of the test in seconds. | `5` | `--duration=10` |
| `--cancellationRate` | Percentage (0-100) of bookings that should be immediately cancelled. | `0` | `--cancellationRate=20` |
| `--delayMs` | Millisecond delay before triggering a cancellation. | `0` | `--delayMs=500` |
| `--failureRate` | Percentage (0-100) of mock email failures to trigger retry logic testing. | `0` | `--failureRate=15` |
| `--verify` | Run specialized verification scenarios (`stale`, `dedupe`, or `all`) instead of traffic generation. | `undefined` | `--verify=all` |

### Examples

**1. Normal Burst Traffic:**
Simulate a spike in bookings without cancellations or failures.
```bash
npm run traffic:test -- --mode=burst --rate=20 --duration=5
```

**2. Sustained Load with Cancellations (Testing Stale Events):**
Creates a sustained flow of bookings and immediately cancels 30% of them. Validates that notifications are correctly marked `EXPIRED` if the booking is cancelled before the email is sent.
```bash
npm run traffic:test -- --mode=continuous --rate=10 --duration=15 --cancellationRate=30
```

**3. Retry Logic and DLQ Testing:**
Simulate email provider outages (30% failure rate) to verify BullMQ retry mechanisms.
```bash
npm run traffic:test -- --mode=continuous --rate=5 --duration=10 --failureRate=30
```

**4. Run Automated Idempotency/Race-Condition Scenarios:**
Executes isolated test cases specifically designed to verify duplicate event handling and stale version protection.
```bash
npm run traffic:test -- --verify=all
```

## How It Works Under the Hood

The simulator comprises several components placed under `src/dev/traffic/`:
*   **`traffic-simulator.ts`**: The CLI entry point. It attempts to trigger the test via the running NestJS HTTP server (`/api/dev/traffic/start`). If the server isn't reachable or the endpoints 404, it falls back to bootstrapping a standalone Nest Application Context.
*   **`TrafficSimulatorService`**: The core execution engine. Handles controlled concurrency, HTTP orchestration, queue monitoring, database metric extraction, and idempotency checks.
*   **`TrafficSimulatorController`**: Exposes the simulator via `POST /api/dev/traffic/start` only when not in production.
*   **`MockEmailProvider` Enhancement**: The provider was enhanced to capture every dispatch attempt and accurately simulate transient failures via a configured failure rate.

## Summary Report Metrics

At the end of a successful simulation run, you receive a detailed report:
- **Kafka Metrics:** Ensures published events equal consumed events.
- **Notification Metrics:** Tracks states (`SENT`, `EXPIRED`, `FAILED`, `RETRYING`).
- **Email Metrics:** Compares actual provider attempts vs successful deliveries (revealing retry activity).
- **Queue Breakdown:** Displays final state of BullMQ (`waiting`, `active`, `completed`, `delayed`, `failed`). 

## Monitoring Real-Time Steps

Because the simulator interacts with your running NestJS application, **you can watch every step happen in real-time by looking at the terminal where `npm run start:dev` is running.**

The codebase is instrumented with detailed logging. As the simulator pushes traffic, you will see logs streaming for the entire lifecycle:
1. **`booking.created` / `booking.cancelled`** (from `BookingService`)
2. **`kafka.published`** (from `KafkaProducer`)
3. **`kafka.consumed`** (from `KafkaConsumer`)
4. **`notification.created`** (from `NotificationService`)
5. **`queue.job_added`** (from `EmailQueue`)
6. **`worker.job_started` / `worker.job_completed`** (from `EmailWorker`)
7. **`email.sent_mock`** (from `MockEmailProvider`)

**External GUI Tools:**
- **Database:** Keep `npx prisma studio` open (http://localhost:5555) to watch rows being inserted and `version` columns incrementing.
- **Kafka:** Use a tool like [Offset Explorer](https://www.kafkatool.com/) or Conduktor, connect to `localhost:9092`, and tail the `booking.events` topic.
- **Redis (BullMQ):** Use a tool like [RedisInsight](https://redis.com/redis-enterprise/redis-insight/) to connect to `localhost:6379` and view the raw job payloads waiting in the BullMQ hashes.

Use this report to identify bottlenecks, resource leaks, or missing dead-letter configurations during development.
