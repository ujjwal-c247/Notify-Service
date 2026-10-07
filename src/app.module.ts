import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from './infrastructure/database/prisma/prisma.module';
import { KafkaModule } from './infrastructure/kafka/kafka.module';
import { HealthController } from './health/health.controller';
import { BookingModule } from './modules/booking/booking.module';
import { OutboxModule } from './modules/outbox/outbox.module';
import { NotificationModule } from './modules/notification/notification.module';
import { TrafficSimulatorModule } from './dev/traffic/traffic-simulator.module';
import { ScheduleModule } from '@nestjs/schedule';

const devModules = process.env.NODE_ENV !== 'production' ? [TrafficSimulatorModule] : [];

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
    }),
    ScheduleModule.forRoot(),
    PrismaModule,
    KafkaModule,
    BookingModule,
    OutboxModule,
    NotificationModule,
    ...devModules,
  ],
  controllers: [HealthController],
})
export class AppModule {}

