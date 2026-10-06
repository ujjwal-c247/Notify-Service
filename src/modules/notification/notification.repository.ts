import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../infrastructure/database/prisma/prisma.service';
import { NotificationStatus, NotificationType, NotificationChannel } from '../../common/enums';
import { NotificationStateMachine } from './notification-state-machine';
import { Prisma } from '@prisma/client';

export interface CreateNotificationParams {
  tenantId: string;
  bookingId: string;
  bookingVersion: number;
  type: NotificationType;
  channel: NotificationChannel;
  dedupeKey: string;
  payload: Record<string, any>;
  expiresAt?: Date;
}

@Injectable()
export class NotificationRepository {
  private readonly logger = new Logger(NotificationRepository.name);

  constructor(private readonly prisma: PrismaService) {}

  async findById(id: string) {
    return this.prisma.notification.findUnique({
      where: { id },
      include: { booking: true },
    });
  }

  async findByDedupeKey(dedupeKey: string) {
    return this.prisma.notification.findUnique({
      where: { dedupeKey },
    });
  }

  async create(params: CreateNotificationParams) {
    try {
      return await this.prisma.notification.create({
        data: {
          tenantId: params.tenantId,
          bookingId: params.bookingId,
          bookingVersion: params.bookingVersion,
          type: params.type,
          channel: params.channel,
          status: NotificationStatus.CREATED,
          dedupeKey: params.dedupeKey,
          payload: params.payload,
          expiresAt: params.expiresAt,
        },
      });
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2002' // Unique constraint failed
      ) {
        this.logger.warn(`Duplicate notification blocked by dedupeKey: ${params.dedupeKey}`);
        return this.findByDedupeKey(params.dedupeKey);
      }
      throw error;
    }
  }

  async updateStatus(
    id: string,
    newStatus: NotificationStatus,
    extraData: { lastError?: string; sentAt?: Date; attemptCountIncrement?: boolean } = {},
  ) {
    const existing = await this.prisma.notification.findUnique({ where: { id } });
    if (!existing) return null;

    if (existing.status === newStatus) {
      return existing;
    }

    NotificationStateMachine.validateTransition(
      existing.status as NotificationStatus,
      newStatus,
    );

    return this.prisma.notification.update({
      where: { id },
      data: {
        status: newStatus,
        lastError: extraData.lastError !== undefined ? extraData.lastError : existing.lastError,
        sentAt: extraData.sentAt !== undefined ? extraData.sentAt : existing.sentAt,
        attemptCount: extraData.attemptCountIncrement
          ? { increment: 1 }
          : existing.attemptCount,
      },
    });
  }

  async markPendingAsExpiredForBooking(bookingId: string, maxVersion?: number) {
    const activeStatuses: NotificationStatus[] = [
      NotificationStatus.CREATED,
      NotificationStatus.READY,
      NotificationStatus.PROCESSING,
      NotificationStatus.RETRYING,
    ];

    const whereCondition: Prisma.NotificationWhereInput = {
      bookingId,
      status: { in: activeStatuses },
    };

    if (maxVersion !== undefined) {
      whereCondition.bookingVersion = { lte: maxVersion };
    }

    const count = await this.prisma.notification.updateMany({
      where: whereCondition,
      data: {
        status: NotificationStatus.EXPIRED,
        lastError: 'Expired due to booking cancellation or version change',
      },
    });

    this.logger.log(`Marked ${count.count} pending notifications as EXPIRED for booking ${bookingId}`);
    return count.count;
  }
}
