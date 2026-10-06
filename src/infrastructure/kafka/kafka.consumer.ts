import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { KafkaClient } from './kafka.client';
import { KAFKA_TOPICS, KAFKA_GROUPS } from '../../common/constants';
import { NotificationService } from '../../modules/notification/notification.service';
import { Consumer } from 'kafkajs';
import { EventType } from '../../common/enums';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumer.name);
  private consumer: Consumer;

  constructor(
    private readonly kafkaClient: KafkaClient,
    private readonly notificationService: NotificationService,
    private readonly configService: ConfigService,
  ) {}

  async onModuleInit() {
    try {
      const groupId = this.configService.get<string>('KAFKA_GROUP_ID', KAFKA_GROUPS.NOTIFICATION_SERVICE);
      this.consumer = this.kafkaClient.createConsumer(groupId);
      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: KAFKA_TOPICS.BOOKING_EVENTS,
        fromBeginning: true,
      });

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) return;

          try {
            const raw = message.value.toString();
            const event = JSON.parse(raw);

            this.logger.log({
              event: 'kafka.message_received',
              topic,
              partition,
              eventType: event.eventType,
              aggregateId: event.aggregateId,
              aggregateVersion: event.aggregateVersion,
            });

            if (event.eventType === EventType.BOOKING_CONFIRMED) {
              await this.notificationService.processBookingConfirmedEvent(event);
            } else if (event.eventType === EventType.BOOKING_CANCELLED) {
              await this.notificationService.processBookingCancelledEvent(event);
            } else {
              this.logger.warn(`Unknown Kafka eventType: ${event.eventType}`);
            }
          } catch (err) {
            this.logger.error('Error processing Kafka message', err);
          }
        },
      });

      this.logger.log('Kafka Consumer subscribed & running');
    } catch (err) {
      this.logger.error('Failed to initialize Kafka Consumer', err);
    }
  }

  async onModuleDestroy() {
    if (this.consumer) {
      await this.consumer.disconnect();
    }
  }
}
