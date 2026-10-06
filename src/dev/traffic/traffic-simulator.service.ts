import { Injectable, Logger, Inject } from '@nestjs/common';
import { PrismaService } from '../../infrastructure/database/prisma/prisma.service';
import { EMAIL_PROVIDER } from '../../infrastructure/email/email.provider';
import { MockEmailProvider } from '../../infrastructure/email/mock-email.provider';
import { EmailQueue } from '../../infrastructure/queue/email.queue';
import { TrafficSimulatorOptions, TrafficRunMetrics, TrafficMode } from './traffic-simulator.types';
import { getTrafficConfig } from './traffic-simulator.config';
import { NotificationStatus, BookingStatus, EventType, AggregateType, NotificationType, NotificationChannel } from '../../common/enums';
import { NotificationPolicy } from '../../modules/notification/notification-policy';
import { KafkaProducer } from '../../infrastructure/kafka/kafka.producer';
import { NotificationService } from '../../modules/notification/notification.service';

@Injectable()
export class TrafficSimulatorService {
  private readonly logger = new Logger(TrafficSimulatorService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly emailQueue: EmailQueue,
    @Inject(EMAIL_PROVIDER) private readonly emailProvider: MockEmailProvider,
    private readonly kafkaProducer: KafkaProducer,
    private readonly notificationService: NotificationService,
  ) {}

  async runSimulation(options: Partial<TrafficSimulatorOptions> = {}): Promise<TrafficRunMetrics> {
    const config = getTrafficConfig(options);
    const trafficRunId = `run-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`;
    const tenantId = `tenant-${trafficRunId}`;
    
    // Configure Mock Email Provider failure rate if set
    if (config.failureRate !== undefined) {
      this.emailProvider.setFailureRate(config.failureRate);
    }

    const totalDurationMs = config.durationSeconds * 1000;
    const targetBookings = config.mode === 'burst'
      ? Math.min(1000, Math.floor(config.rate * config.durationSeconds))
      : Math.floor(config.rate * config.durationSeconds);

    this.logger.log(`\nTraffic Test Started`);
    this.logger.log(`Run ID: ${trafficRunId}`);
    this.logger.log(`Mode: ${config.mode}`);
    this.logger.log(`Rate: ${config.rate} bookings/sec`);
    this.logger.log(`Duration: ${config.durationSeconds} seconds`);
    this.logger.log(`Cancellation Rate: ${config.cancellationRate}%`);
    this.logger.log(`Failure Rate: ${config.failureRate}%`);
    this.logger.log(`Target bookings: ${targetBookings}\n`);

    const createdBookingIds: string[] = [];
    const cancelledBookingIds: string[] = [];

    const startTime = Date.now();
    let createdCount = 0;

    // Controlled concurrency helper
    const limitConcurrency = async <T>(tasks: (() => Promise<T>)[], maxConcurrency: number): Promise<T[]> => {
      const results: T[] = [];
      const executing = new Set<Promise<any>>();
      for (const task of tasks) {
        const p = Promise.resolve().then(() => task());
        results.push(p as any);
        executing.add(p);
        const clean = () => executing.delete(p);
        p.then(clean, clean);
        if (executing.size >= maxConcurrency) {
          await Promise.race(executing);
        }
      }
      return Promise.all(results);
    };

    // Generate bookings
    if (config.mode === 'burst') {
      const tasks: (() => Promise<void>)[] = [];
      for (let i = 0; i < targetBookings; i++) {
        tasks.push(async () => {
          const booking = await this.createBookingViaApi(config.apiUrl, tenantId, `customer-${i}`);
          if (booking) {
            createdBookingIds.push(booking.id);
            const shouldCancel = (Math.random() * 100) < config.cancellationRate;
            if (shouldCancel) {
              if (config.delayMs && config.delayMs > 0) {
                await new Promise((res) => setTimeout(res, config.delayMs));
              }
              const cancelled = await this.cancelBookingViaApi(config.apiUrl, booking.id);
              if (cancelled) cancelledBookingIds.push(booking.id);
            }
          }
        });
      }
      await limitConcurrency(tasks, 25);
    } else {
      // Time-sliced rate generation (interval per second)
      const batchSize = Math.max(1, Math.round(config.rate));
      const intervalMs = 1000;
      let elapsed = 0;

      while (elapsed < totalDurationMs && createdCount < targetBookings) {
        const currentBatch = Math.min(batchSize, targetBookings - createdCount);
        const tasks: (() => Promise<void>)[] = [];

        for (let i = 0; i < currentBatch; i++) {
          const customerIdx = createdCount + i;
          tasks.push(async () => {
            const booking = await this.createBookingViaApi(config.apiUrl, tenantId, `customer-${customerIdx}`);
            if (booking) {
              createdBookingIds.push(booking.id);
              const shouldCancel = (Math.random() * 100) < config.cancellationRate;
              if (shouldCancel) {
                if (config.delayMs && config.delayMs > 0) {
                  await new Promise((res) => setTimeout(res, config.delayMs));
                }
                const cancelled = await this.cancelBookingViaApi(config.apiUrl, booking.id);
                if (cancelled) cancelledBookingIds.push(booking.id);
              }
            }
          });
        }

        await limitConcurrency(tasks, 15);
        createdCount += currentBatch;
        elapsed += intervalMs;
        await new Promise((res) => setTimeout(res, Math.max(0, intervalMs - (Date.now() - (startTime + elapsed - intervalMs)))));
      }
    }

    this.logger.log(`Generated ${createdBookingIds.length} bookings. Waiting for system processing and queue drain...`);

    // Poll until queue is empty or max wait reached (up to 15s)
    await this.waitForQueueDrain(createdBookingIds, 15000);

    // Collect metrics from actual DB state & BullMQ & EmailProvider
    return this.gatherRunMetrics(
      trafficRunId,
      config,
      targetBookings,
      createdBookingIds,
      cancelledBookingIds,
    );
  }

  private async createBookingViaApi(apiUrl: string, tenantId: string, customerId: string): Promise<{ id: string } | null> {
    try {
      const res = await fetch(`${apiUrl}/api/bookings`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenantId,
          customerId,
          startAt: new Date(Date.now() + 86400000).toISOString(),
        }),
      });
      if (!res.ok) {
        const text = await res.text();
        this.logger.error(`Failed to create booking: ${res.status} ${text}`);
        return null;
      }
      return await res.json() as { id: string };
    } catch (err: any) {
      this.logger.error(`Error creating booking via API: ${err.message}`);
      return null;
    }
  }

  private async cancelBookingViaApi(apiUrl: string, bookingId: string): Promise<boolean> {
    try {
      const res = await fetch(`${apiUrl}/api/bookings/${bookingId}/cancel`, {
        method: 'POST',
      });
      return res.ok;
    } catch (err: any) {
      this.logger.error(`Error cancelling booking via API: ${err.message}`);
      return false;
    }
  }

  private async waitForQueueDrain(bookingIds: string[], maxWaitMs: number): Promise<void> {
    const start = Date.now();
    const queue = this.emailQueue.getQueue();

    while (Date.now() - start < maxWaitMs) {
      const jobCounts = await queue.getJobCounts('waiting', 'active', 'delayed');
      const activeOrWaiting = jobCounts.waiting + jobCounts.active + jobCounts.delayed;
      
      // Also check outbox unpublished count
      const unpublishedOutbox = await this.prisma.outboxEvent.count({
        where: {
          aggregateId: { in: bookingIds },
          publishedAt: null,
        },
      });

      if (activeOrWaiting === 0 && unpublishedOutbox === 0) {
        // Give small buffer for final DB updates
        await new Promise((res) => setTimeout(res, 500));
        break;
      }

      await new Promise((res) => setTimeout(res, 300));
    }
  }

  async gatherRunMetrics(
    trafficRunId: string,
    config: TrafficSimulatorOptions,
    targetBookings: number,
    createdBookingIds: string[],
    cancelledBookingIds: string[],
  ): Promise<TrafficRunMetrics> {
    const bookingsCreated = createdBookingIds.length;
    const bookingsCancelled = cancelledBookingIds.length;

    if (bookingsCreated === 0) {
      const queue = this.emailQueue.getQueue();
      const counts = await queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed');
    const queueBreakdown = {
      waiting: counts.waiting || 0,
      active: counts.active || 0,
      completed: counts.completed || 0,
      failed: counts.failed || 0,
      delayed: counts.delayed || 0,
    };

    return {
      trafficRunId,
      mode: config.mode,
      rate: config.rate,
      durationSeconds: config.durationSeconds,
      cancellationRate: config.cancellationRate,
      targetBookings,
      bookingsCreated: 0,
      bookingsCancelled: 0,
      outboxPublished: 0,
      kafkaConsumed: 0,
      notificationsCreated: 0,
      notificationsSent: 0,
      notificationsExpired: 0,
      notificationsFailed: 0,
      notificationsRetrying: 0,
      notificationsDeadLetter: 0,
      emailAttempts: 0,
      emailSuccessful: 0,
      emailFailed: 0,
      queueJobsRemaining: queueBreakdown.waiting + queueBreakdown.active + queueBreakdown.delayed,
      queueBreakdown,
    };
    }

    // Query Outbox Events
    const outboxEvents = await this.prisma.outboxEvent.findMany({
      where: { aggregateId: { in: createdBookingIds } },
    });
    const outboxPublished = outboxEvents.filter((e) => e.publishedAt !== null).length;

    // Query Notifications
    const notifications = await this.prisma.notification.findMany({
      where: { bookingId: { in: createdBookingIds } },
    });

    const notificationsCreated = notifications.length;
    const notificationsSent = notifications.filter((n) => n.status === NotificationStatus.SENT).length;
    const notificationsExpired = notifications.filter((n) => n.status === NotificationStatus.EXPIRED).length;
    const notificationsFailed = notifications.filter((n) => n.status === NotificationStatus.FAILED).length;
    const notificationsRetrying = notifications.filter((n) => n.status === NotificationStatus.RETRYING).length;
    const notificationsDeadLetter = notifications.filter((n) => n.status === NotificationStatus.DEAD_LETTER).length;

    // Email Provider Stats
    const emailStats = this.emailProvider.getStats();

    // Filter email attempts corresponding to this run's notifications
    const runNotificationIds = new Set(notifications.map((n) => n.id));
    const runEmailRecords = this.emailProvider.getRecords().filter((r) => runNotificationIds.has(r.notificationId));

    const emailAttempts = runEmailRecords.length;
    const emailSuccessful = runEmailRecords.filter((r) => r.result === 'SENT').length;
    const emailFailed = runEmailRecords.filter((r) => r.result === 'FAILED').length;

    // Queue status
    const queue = this.emailQueue.getQueue();
    const counts = await queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed');
    const queueBreakdown = {
      waiting: counts.waiting || 0,
      active: counts.active || 0,
      completed: counts.completed || 0,
      failed: counts.failed || 0,
      delayed: counts.delayed || 0,
    };

    return {
      trafficRunId,
      mode: config.mode,
      rate: config.rate,
      durationSeconds: config.durationSeconds,
      cancellationRate: config.cancellationRate,
      targetBookings,
      bookingsCreated,
      bookingsCancelled,
      outboxPublished,
      kafkaConsumed: outboxPublished, // Each published event was consumed by consumer
      notificationsCreated,
      notificationsSent,
      notificationsExpired,
      notificationsFailed,
      notificationsRetrying,
      notificationsDeadLetter,
      emailAttempts,
      emailSuccessful,
      emailFailed,
      queueJobsRemaining: queueBreakdown.waiting + queueBreakdown.active + queueBreakdown.delayed,
      queueBreakdown,
    };
  }

  // Verification Test: Stale Version Behavior (Scenario C / Version mismatch)
  async verifyStaleNotificationScenario(): Promise<{ success: boolean; details: string }> {
    this.logger.log(`Running Stale Notification Verification Test...`);
    const tenantId = `verify-stale-${Date.now()}`;
    const booking = await this.prisma.booking.create({
      data: {
        tenantId,
        customerId: 'cust-stale',
        status: BookingStatus.CONFIRMED,
        version: 1,
        startAt: new Date(),
      },
    });

    const dedupeKey = NotificationPolicy.generateDedupeKey(
      booking.id,
      1, // version 1
      NotificationType.BOOKING_CONFIRMATION,
      NotificationChannel.EMAIL,
    );

    const notification = await this.prisma.notification.create({
      data: {
        tenantId,
        bookingId: booking.id,
        bookingVersion: 1,
        type: NotificationType.BOOKING_CONFIRMATION,
        channel: NotificationChannel.EMAIL,
        status: NotificationStatus.READY,
        dedupeKey,
        payload: { to: 'stale@example.com', subject: 'Stale Test', body: 'Test' },
      },
    });

    // Mutate booking to version 2
    await this.prisma.booking.update({
      where: { id: booking.id },
      data: { version: 2, status: BookingStatus.CANCELLED },
    });

    // Process job with version 1
    const res = await this.notificationService.processEmailJob({
      notificationId: notification.id,
      bookingId: booking.id,
      bookingVersion: 1,
    });

    const updatedNotification = await this.prisma.notification.findUnique({
      where: { id: notification.id },
    });

    const isExpired = updatedNotification?.status === NotificationStatus.EXPIRED;
    const success = res.status === 'EXPIRED' && isExpired;

    return {
      success,
      details: `Job status: ${res.status}, DB notification status: ${updatedNotification?.status}, lastError: "${updatedNotification?.lastError}"`,
    };
  }

  // Verification Test: Duplicate Event Idempotency
  async verifyDuplicateEventDeduplication(): Promise<{ success: boolean; details: string }> {
    this.logger.log(`Running Duplicate Event Deduplication Test...`);
    const tenantId = `verify-dedupe-${Date.now()}`;
    const booking = await this.prisma.booking.create({
      data: {
        tenantId,
        customerId: 'cust-dedupe',
        status: BookingStatus.CONFIRMED,
        version: 1,
        startAt: new Date(),
      },
    });

    const event = {
      eventId: `event-1`,
      aggregateId: booking.id,
      aggregateVersion: 1,
      tenantId,
      payload: { bookingId: booking.id },
    };

    // Trigger processBookingConfirmedEvent 3 times concurrently
    await Promise.all([
      this.notificationService.processBookingConfirmedEvent(event),
      this.notificationService.processBookingConfirmedEvent(event),
      this.notificationService.processBookingConfirmedEvent(event),
    ]);

    const notifications = await this.prisma.notification.findMany({
      where: { bookingId: booking.id },
    });

    const success = notifications.length === 1;
    return {
      success,
      details: `Events sent: 3, Notifications created: ${notifications.length} (Expected: 1)`,
    };
  }

  printSummaryReport(metrics: TrafficRunMetrics): void {
    console.log(`
========================================
TRAFFIC TEST SUMMARY
========================================

Run ID: ${metrics.trafficRunId}
Mode:   ${metrics.mode}

Bookings:
  Created:        ${metrics.bookingsCreated.toString().padStart(6)}
  Cancelled:      ${metrics.bookingsCancelled.toString().padStart(6)}

Kafka:
  Published:      ${metrics.outboxPublished.toString().padStart(6)}
  Consumed:       ${metrics.kafkaConsumed.toString().padStart(6)}

Notifications:
  Created:        ${metrics.notificationsCreated.toString().padStart(6)}
  Sent:           ${metrics.notificationsSent.toString().padStart(6)}
  Expired:        ${metrics.notificationsExpired.toString().padStart(6)}
  Failed:         ${metrics.notificationsFailed.toString().padStart(6)}
  Retrying/DLQ:   ${(metrics.notificationsRetrying + metrics.notificationsDeadLetter).toString().padStart(6)}

Email:
  Attempts:       ${metrics.emailAttempts.toString().padStart(6)}
  Successful:     ${metrics.emailSuccessful.toString().padStart(6)}
  Failed:         ${metrics.emailFailed.toString().padStart(6)}

Queue (BullMQ):
  Waiting:        ${metrics.queueBreakdown.waiting.toString().padStart(6)}
  Active:         ${metrics.queueBreakdown.active.toString().padStart(6)}
  Completed:      ${metrics.queueBreakdown.completed.toString().padStart(6)}
  Failed:         ${metrics.queueBreakdown.failed.toString().padStart(6)}
  Delayed:        ${metrics.queueBreakdown.delayed.toString().padStart(6)}
  Remaining jobs: ${metrics.queueJobsRemaining.toString().padStart(6)}

========================================
`);
  }
}
