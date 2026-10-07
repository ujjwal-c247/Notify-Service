import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../infrastructure/database/prisma/prisma.service';
import { Prisma } from '@prisma/client';
import { EventType, AggregateType } from '../../common/enums';

export interface CreateOutboxEventDto {
  eventType: EventType;
  aggregateType: AggregateType;
  aggregateId: string;
  aggregateVersion: number;
  payload: Record<string, any>;
}

@Injectable()
export class OutboxService {
  private readonly logger = new Logger(OutboxService.name);

  constructor(private readonly prisma: PrismaService) { }

  async createOutboxEvent(
    tx: Prisma.TransactionClient,
    dto: CreateOutboxEventDto,
  ) {
    return tx.outboxEvent.create({
      data: {
        eventType: dto.eventType,
        aggregateType: dto.aggregateType,
        aggregateId: dto.aggregateId,
        aggregateVersion: dto.aggregateVersion,
        payload: dto.payload,
      },
    });
  }

  async fetchUnpublishedEvents(limit = 50) {
    return this.prisma.outboxEvent.findMany({
      where: { publishedAt: null },
      orderBy: { createdAt: 'asc' },
      take: limit,
    });
  }

  async markAsPublished(id: string) {
    return this.prisma.outboxEvent.update({
      where: { id },
      data: { publishedAt: new Date() },
    });
  }

  async incrementAttemptCount(id: string) {
    return this.prisma.outboxEvent.update({
      where: { id },
      data: { attemptCount: { increment: 1 } },
    });
  }

  async pruneOldOutboxEvents() {
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

    return this.prisma.outboxEvent.deleteMany({
      where: {
        publishedAt: { lte: sevenDaysAgo },
      },
    });
  }
}
