import { Injectable, Logger } from '@nestjs/common';
import { KafkaClient } from './kafka.client';
import { KAFKA_TOPICS } from '../../common/constants';

export interface KafkaEventPayload {
  eventId: string;
  eventType: string;
  aggregateType: string;
  aggregateId: string;
  aggregateVersion: number;
  tenantId: string;
  occurredAt: string;
  payload: Record<string, any>;
}

@Injectable()
export class KafkaProducer {
  private readonly logger = new Logger(KafkaProducer.name);

  constructor(private readonly kafkaClient: KafkaClient) {}

  async sendEvent(event: KafkaEventPayload) {
    const producer = this.kafkaClient.getProducer();
    const key = event.aggregateId || event.payload?.bookingId;

    await producer.send({
      topic: KAFKA_TOPICS.BOOKING_EVENTS,
      messages: [
        {
          key,
          value: JSON.stringify(event),
          headers: {
            eventType: event.eventType,
            aggregateVersion: String(event.aggregateVersion),
          },
        },
      ],
    });

    this.logger.log({
      event: 'kafka.event_published',
      eventId: event.eventId,
      eventType: event.eventType,
      aggregateId: event.aggregateId,
      aggregateVersion: event.aggregateVersion,
    });
  }
}
