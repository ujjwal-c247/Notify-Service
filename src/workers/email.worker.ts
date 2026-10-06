import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Worker, Job } from 'bullmq';
import { QUEUES } from '../common/constants';
import { getRedisConfig } from '../infrastructure/redis/redis.config';
import { EmailJobData } from '../infrastructure/queue/email.queue';
import { NotificationService } from '../modules/notification/notification.service';

@Injectable()
export class EmailWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EmailWorker.name);
  private worker: Worker<EmailJobData>;

  constructor(
    private readonly configService: ConfigService,
    private readonly notificationService: NotificationService,
  ) {}

  onModuleInit() {
    const connection = getRedisConfig(this.configService);

    this.worker = new Worker<EmailJobData>(
      QUEUES.EMAIL_NOTIFICATIONS,
      async (job: Job<EmailJobData>) => {
        this.logger.log({
          event: 'worker.job_started',
          jobId: job.id,
          notificationId: job.data.notificationId,
          bookingId: job.data.bookingId,
          attempt: job.attemptsMade + 1,
        });

        return await this.notificationService.processEmailJob(job.data);
      },
      {
        connection,
        concurrency: 5,
      },
    );

    this.worker.on('completed', (job) => {
      this.logger.log({
        event: 'worker.job_completed',
        jobId: job.id,
        notificationId: job.data.notificationId,
      });
    });

    this.worker.on('failed', (job, err) => {
      this.logger.error({
        event: 'worker.job_failed',
        jobId: job?.id,
        notificationId: job?.data?.notificationId,
        error: err.message,
        attemptsMade: job?.attemptsMade,
      });
    });

    this.logger.log(`BullMQ EmailWorker initialized on queue: ${QUEUES.EMAIL_NOTIFICATIONS}`);
  }

  async onModuleDestroy() {
    if (this.worker) {
      await this.worker.close();
    }
  }
}
