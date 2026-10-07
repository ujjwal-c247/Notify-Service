import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Producer, Consumer, Admin } from 'kafkajs';
import { KAFKA_TOPICS, KAFKA_GROUPS } from '../../common/constants';

@Injectable()
export class KafkaClient implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaClient.name);
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private admin: Admin;
  private isConnected = false;

  constructor(private readonly configService: ConfigService) {
    const brokers = this.configService.get<string>('KAFKA_BROKERS', 'localhost:9092').split(',');
    const clientId = this.configService.get<string>('KAFKA_CLIENT_ID', 'booking-notification-local');

    this.kafka = new Kafka({
      clientId,
      brokers,
      retry: {
        initialRetryTime: 300,
        retries: 10,
      },
    });

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
      transactionTimeout: 30000,
    });

    this.admin = this.kafka.admin();
  }

  async onModuleInit() {
    try {
      await this.admin.connect();
      const existingTopics = await this.admin.listTopics();
      if (!existingTopics.includes(KAFKA_TOPICS.BOOKING_EVENTS)) {
        await this.admin.createTopics({
          topics: [
            {
              topic: KAFKA_TOPICS.BOOKING_EVENTS,
              numPartitions: 3,
              replicationFactor: 1,
              configEntries: [
                {
                  name: 'retention.ms',
                  value: '259200000' // 3 days in milliseconds
                }
              ],
            },
          ],
        });
        this.logger.log(`Created Kafka topic: ${KAFKA_TOPICS.BOOKING_EVENTS}`);
      }
      await this.admin.disconnect();

      await this.producer.connect();
      this.isConnected = true;
      this.logger.log('Kafka Producer connected');
    } catch (err) {
      this.logger.error('Failed to connect to Kafka', err);
    }
  }

  async onModuleDestroy() {
    if (this.producer) {
      await this.producer.disconnect();
    }
    if (this.consumer) {
      await this.consumer.disconnect();
    }
  }

  getProducer(): Producer {
    return this.producer;
  }

  createConsumer(groupId: string = KAFKA_GROUPS.NOTIFICATION_SERVICE): Consumer {
    return this.kafka.consumer({ groupId });
  }
}
