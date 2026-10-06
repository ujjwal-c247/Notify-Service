import { Module } from '@nestjs/common';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';
import { NotificationRepository } from './notification.repository';
import { BookingModule } from '../booking/booking.module';
import { EMAIL_PROVIDER } from '../../infrastructure/email/email.provider';
import { MockEmailProvider } from '../../infrastructure/email/mock-email.provider';
import { EmailQueue } from '../../infrastructure/queue/email.queue';
import { KafkaClient } from '../../infrastructure/kafka/kafka.client';
import { KafkaProducer } from '../../infrastructure/kafka/kafka.producer';
import { KafkaConsumer } from '../../infrastructure/kafka/kafka.consumer';
import { EmailWorker } from '../../workers/email.worker';

@Module({
  imports: [BookingModule],
  controllers: [NotificationController],
  providers: [
    NotificationService,
    NotificationRepository,
    EmailQueue,
    KafkaClient,
    KafkaProducer,
    KafkaConsumer,
    EmailWorker,
    {
      provide: EMAIL_PROVIDER,
      useClass: MockEmailProvider,
    },
  ],
  exports: [NotificationService, NotificationRepository, EMAIL_PROVIDER],
})
export class NotificationModule {}
