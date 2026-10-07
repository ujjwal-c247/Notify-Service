import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { OutboxService } from './outbox.service';
import { KafkaProducer } from '../../infrastructure/kafka/kafka.producer';
import { Cron } from '@nestjs/schedule';

@Injectable()
export class OutboxPublisher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisher.name);
  private timer: NodeJS.Timeout | null = null;
  private isProcessing = false;

  constructor(
    private readonly outboxService: OutboxService,
    private readonly kafkaProducer: KafkaProducer,
  ) { }

  onModuleInit() {
    this.timer = setInterval(() => {
      this.processOutbox().catch((err) => {
        this.logger.error('Error processing outbox events', err);
      });
    }, 1000);
  }

  onModuleDestroy() {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  @Cron('0 2 * * *') // Run daily at 2 AM
  async pruneOldOutboxEvents() {
    this.logger.log('Running nightly outbox event pruning...');
    try {
      await this.outboxService.pruneOldOutboxEvents();
      this.logger.log('Successfully pruned old outbox events');
    } catch (error) {
      this.logger.error('Failed to prune old outbox events', error);
    }
  }

  async processOutbox() {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      const events = await this.outboxService.fetchUnpublishedEvents(20);
      for (const event of events) {
        try {
          const payload = typeof event.payload === 'object' ? event.payload : JSON.parse(event.payload as string);
          await this.kafkaProducer.sendEvent({
            eventId: event.id,
            eventType: event.eventType,
            aggregateType: event.aggregateType,
            aggregateId: event.aggregateId,
            aggregateVersion: event.aggregateVersion,
            tenantId: payload.tenantId || 'default',
            occurredAt: event.createdAt.toISOString(),
            payload: payload as Record<string, any>,
          });

          await this.outboxService.markAsPublished(event.id);
        } catch (err) {
          this.logger.error(`Failed to publish outbox event ${event.id}`, err);
          await this.outboxService.incrementAttemptCount(event.id);
        }
      }
    } finally {
      this.isProcessing = false;
    }
  }
}
