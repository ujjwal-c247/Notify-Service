import { Global, Module } from '@nestjs/common';
import { KafkaClient } from './kafka.client';
import { KafkaProducer } from './kafka.producer';

@Global()
@Module({
  providers: [KafkaClient, KafkaProducer],
  exports: [KafkaClient, KafkaProducer],
})
export class KafkaModule {}
