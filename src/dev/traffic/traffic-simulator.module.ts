import { Module } from '@nestjs/common';
import { TrafficSimulatorService } from './traffic-simulator.service';
import { TrafficSimulatorController } from './traffic-simulator.controller';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../infrastructure/database/prisma/prisma.module';
import { NotificationModule } from '../../modules/notification/notification.module';
import { BookingModule } from '../../modules/booking/booking.module';
import { KafkaModule } from '../../infrastructure/kafka/kafka.module';
import { EmailQueue } from '../../infrastructure/queue/email.queue';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
    }),
    PrismaModule,
    BookingModule,
    NotificationModule,
    KafkaModule,
  ],
  controllers: [TrafficSimulatorController],
  providers: [TrafficSimulatorService, EmailQueue],
  exports: [TrafficSimulatorService],
})
export class TrafficSimulatorModule {}
