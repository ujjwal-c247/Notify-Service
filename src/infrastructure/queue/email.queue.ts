import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Queue } from 'bullmq';
import { QUEUES, JOBS } from '../../common/constants';
import { getRedisConfig } from '../redis/redis.config';

export interface EmailJobData {
  notificationId: string;
  bookingId: string;
  bookingVersion: number;
}

@Injectable()
export class EmailQueue implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EmailQueue.name);
  private queue: Queue<EmailJobData>;

  constructor(private readonly configService: ConfigService) {}

  onModuleInit() {
    const connection = getRedisConfig(this.configService);
    const maxAttempts = Number(this.configService.get<number>('NOTIFICATION_MAX_RETRIES', 5));
    const backoffDelay = Number(this.configService.get<number>('NOTIFICATION_RETRY_DELAY_MS', 1000));

    this.queue = new Queue<EmailJobData>(QUEUES.EMAIL_NOTIFICATIONS, {
      connection,
      defaultJobOptions: {
        attempts: maxAttempts,
        backoff: {
          type: 'exponential',
          delay: backoffDelay,
        },
        removeOnComplete: false,
        removeOnFail: false,
      },
    });

    this.logger.log(`BullMQ Queue initialized: ${QUEUES.EMAIL_NOTIFICATIONS}`);
  }

  async onModuleDestroy() {
    if (this.queue) {
      await this.queue.close();
    }
  }

  async addEmailJob(data: EmailJobData, jobId?: string) {
    const options = jobId ? { jobId } : {};
    const job = await this.queue.add(JOBS.SEND_BOOKING_CONFIRMATION, data, options);
    this.logger.log({
      event: 'queue.job_added',
      jobId: job.id,
      notificationId: data.notificationId,
      bookingId: data.bookingId,
      bookingVersion: data.bookingVersion,
    });
    return job;
  }

  getQueue(): Queue<EmailJobData> {
    return this.queue;
  }
}
