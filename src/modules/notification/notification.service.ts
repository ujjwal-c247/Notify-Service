import { Injectable, Logger, Inject } from '@nestjs/common';
import { NotificationRepository } from './notification.repository';
import { BookingRepository } from '../booking/booking.repository';
import { EMAIL_PROVIDER, EmailProvider } from '../../infrastructure/email/email.provider';
import { EmailQueue } from '../../infrastructure/queue/email.queue';
import { NotificationPolicy } from './notification-policy';
import {
  NotificationStatus,
  NotificationType,
  NotificationChannel,
  BookingStatus,
} from '../../common/enums';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private readonly maxRetries: number;

  constructor(
    private readonly notificationRepo: NotificationRepository,
    private readonly bookingRepo: BookingRepository,
    private readonly emailQueue: EmailQueue,
    @Inject(EMAIL_PROVIDER) private readonly emailProvider: EmailProvider,
    private readonly configService: ConfigService,
  ) {
    this.maxRetries = Number(this.configService.get<number>('NOTIFICATION_MAX_RETRIES', 5));
  }

  async processBookingConfirmedEvent(event: {
    eventId: string;
    aggregateId: string;
    aggregateVersion: number;
    tenantId: string;
    payload: { bookingId: string };
  }) {
    const bookingId = event.aggregateId || event.payload.bookingId;
    const booking = await this.bookingRepo.findById(bookingId);

    if (!booking) {
      this.logger.warn(`Booking ${bookingId} not found when processing BOOKING_CONFIRMED event`);
      return;
    }

    // Stale event check
    if (event.aggregateVersion < booking.version) {
      this.logger.warn(
        `Ignoring stale BOOKING_CONFIRMED event. Event version: ${event.aggregateVersion}, current booking version: ${booking.version}`,
      );
      return;
    }

    if (booking.status !== BookingStatus.CONFIRMED) {
      this.logger.warn(
        `Ignoring BOOKING_CONFIRMED event because current booking status is ${booking.status}`,
      );
      return;
    }

    const dedupeKey = NotificationPolicy.generateDedupeKey(
      booking.id,
      event.aggregateVersion,
      NotificationType.BOOKING_CONFIRMATION,
      NotificationChannel.EMAIL,
    );

    const notification = await this.notificationRepo.create({
      tenantId: booking.tenantId,
      bookingId: booking.id,
      bookingVersion: event.aggregateVersion,
      type: NotificationType.BOOKING_CONFIRMATION,
      channel: NotificationChannel.EMAIL,
      dedupeKey,
      payload: {
        to: `customer-${booking.customerId}@example.com`,
        subject: `Booking Confirmed #${booking.id}`,
        body: `Dear customer, your booking #${booking.id} is confirmed.`,
      },
    });

    if (!notification) return;

    if (
      notification.status === NotificationStatus.CREATED ||
      notification.status === NotificationStatus.READY
    ) {
      await this.notificationRepo.updateStatus(notification.id, NotificationStatus.READY);

      // Add email job to BullMQ
      await this.emailQueue.addEmailJob(
        {
          notificationId: notification.id,
          bookingId: booking.id,
          bookingVersion: event.aggregateVersion,
        },
        `job_${dedupeKey.replace(/:/g, '_')}`,
      );
    }

    // Schedule 2-hour reminder
    if (booking.startAt) {
      const scheduledAt = new Date(booking.startAt);
      // scheduledAt.setHours(scheduledAt.getHours() - 2);
      scheduledAt.setMinutes(scheduledAt.getMinutes() - 5);

      // Only schedule if it's in the future
      if (scheduledAt > new Date()) {
        const reminderDedupeKey = NotificationPolicy.generateDedupeKey(
          booking.id,
          event.aggregateVersion,
          NotificationType.BOOKING_REMINDER,
          NotificationChannel.EMAIL,
        );

        await this.notificationRepo.create({
          tenantId: booking.tenantId,
          bookingId: booking.id,
          bookingVersion: event.aggregateVersion,
          type: NotificationType.BOOKING_REMINDER,
          channel: NotificationChannel.EMAIL,
          dedupeKey: reminderDedupeKey,
          scheduledAt,
          payload: {
            to: `customer-${booking.customerId}@example.com`,
            subject: `Reminder: Your Booking #${booking.id} is in 2 hours!`,
            body: `Dear customer, your booking #${booking.id} is starting soon.`,
          },
        });
      }
    }
  }

  async processBookingCancelledEvent(event: {
    aggregateId: string;
    aggregateVersion: number;
    payload: { bookingId: string };
  }) {
    const bookingId = event.aggregateId || event.payload.bookingId;
    this.logger.log(
      `Processing BOOKING_CANCELLED event for booking ${bookingId} (v${event.aggregateVersion})`,
    );
    await this.notificationRepo.markPendingAsExpiredForBooking(bookingId, event.aggregateVersion);
  }

  async processEmailJob(jobData: {
    notificationId: string;
    bookingId: string;
    bookingVersion: number;
  }): Promise<{ status: string }> {
    const { notificationId, bookingId, bookingVersion } = jobData;

    // 1. Reload notification from PostgreSQL
    const notification = await this.notificationRepo.findById(notificationId);
    if (!notification) {
      this.logger.warn(`Email worker: Notification ${notificationId} not found`);
      return { status: 'NOT_FOUND' };
    }

    // 2. Status sendability check
    if (
      notification.status === NotificationStatus.SENT ||
      notification.status === NotificationStatus.EXPIRED ||
      notification.status === NotificationStatus.SUPPRESSED ||
      notification.status === NotificationStatus.FAILED ||
      notification.status === NotificationStatus.DEAD_LETTER
    ) {
      this.logger.log(
        `Email worker: Skipping notification ${notificationId} in non-sendable status ${notification.status}`,
      );
      return { status: notification.status };
    }

    // 3. Reload current booking from PostgreSQL (MANDATORY FINAL VALIDATION)
    const booking = await this.bookingRepo.findById(bookingId);

    // 4. Validate booking existence and state
    if (!booking) {
      await this.notificationRepo.updateStatus(notificationId, NotificationStatus.EXPIRED, {
        lastError: 'Booking no longer exists',
      });
      return { status: 'EXPIRED' };
    }

    if (booking.status !== BookingStatus.CONFIRMED) {
      this.logger.warn(
        `Final Validation Failed: Booking ${bookingId} status is ${booking.status} (expected CONFIRMED). Marking notification ${notificationId} as EXPIRED.`,
      );
      await this.notificationRepo.updateStatus(notificationId, NotificationStatus.EXPIRED, {
        lastError: `Booking status is ${booking.status}`,
      });
      return { status: 'EXPIRED' };
    }

    if (booking.version !== bookingVersion) {
      this.logger.warn(
        `Final Validation Failed: Booking version mismatch (booking v${booking.version} vs job v${bookingVersion}). Marking notification ${notificationId} as EXPIRED.`,
      );
      await this.notificationRepo.updateStatus(notificationId, NotificationStatus.EXPIRED, {
        lastError: `Booking version mismatch: ${booking.version} vs ${bookingVersion}`,
      });
      return { status: 'EXPIRED' };
    }

    // All validation passed -> Mark PROCESSING
    await this.notificationRepo.updateStatus(notificationId, NotificationStatus.PROCESSING, {
      attemptCountIncrement: true,
    });

    const payload = (typeof notification.payload === 'object'
      ? notification.payload
      : JSON.parse(notification.payload as string)) as Record<string, any>;

    const result = await this.emailProvider.send({
      to: payload.to || 'customer@example.com',
      subject: payload.subject || 'Booking Update',
      body: payload.body || '',
      notificationId: notification.id,
      bookingId: booking.id,
    });

    if (result.success) {
      await this.notificationRepo.updateStatus(notificationId, NotificationStatus.SENT, {
        sentAt: new Date(),
      });
      this.logger.log({
        event: 'email.sent',
        notificationId,
        bookingId,
        providerMessageId: result.providerMessageId,
      });
      return { status: 'SENT' };
    }

    // Handle Failure
    const currentAttempt = notification.attemptCount + 1;
    if (result.retryable) {
      if (currentAttempt >= this.maxRetries) {
        await this.notificationRepo.updateStatus(notificationId, NotificationStatus.DEAD_LETTER, {
          lastError: result.error,
        });
        this.logger.error(
          `Notification ${notificationId} reached max retries (${this.maxRetries}). Marked as DEAD_LETTER.`,
        );
        return { status: 'DEAD_LETTER' };
      } else {
        await this.notificationRepo.updateStatus(notificationId, NotificationStatus.RETRYING, {
          lastError: result.error,
        });
        this.logger.warn(
          `Notification ${notificationId} email failed (retryable): ${result.error}. Attempt ${currentAttempt}/${this.maxRetries}.`,
        );
        throw new Error(result.error || 'Retryable email error');
      }
    } else {
      // Permanent failure
      await this.notificationRepo.updateStatus(notificationId, NotificationStatus.FAILED, {
        lastError: result.error,
      });
      this.logger.error(
        `Notification ${notificationId} email failed permanently: ${result.error}. Marked as FAILED.`,
      );
      return { status: 'FAILED' };
    }
  }

  @Cron('* * * * *') // Run every minute
  async dispatchScheduledReminders() {
    this.logger.debug('Checking for due reminders...');
    const dueNotifications = await this.notificationRepo.findDueReminders();

    for (const notif of dueNotifications) {
      this.logger.log(`Dispatching reminder notification ${notif.id} for booking ${notif.bookingId}`);
      await this.notificationRepo.updateStatus(notif.id, NotificationStatus.READY);

      await this.emailQueue.addEmailJob(
        {
          notificationId: notif.id,
          bookingId: notif.bookingId,
          bookingVersion: notif.bookingVersion,
        },
        `job_${notif.dedupeKey.replace(/:/g, '_')}`,
      );
    }
  }

  async getNotification(id: string) {
    return this.notificationRepo.findById(id);
  }
}
