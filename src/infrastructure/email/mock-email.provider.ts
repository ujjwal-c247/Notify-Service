import { Injectable, Logger } from '@nestjs/common';
import { EmailProvider, EmailMessage, EmailSendResult } from './email.provider';

export type MockFailureMode = 'none' | 'retryable' | 'permanent' | 'timeout';

export interface MockEmailRecord {
  notificationId: string;
  bookingId: string;
  recipient: string;
  subject: string;
  timestamp: Date;
  result: 'SENT' | 'FAILED';
  error?: string;
  retryable?: boolean;
}

export interface MockEmailStats {
  totalAttempts: number;
  successfullySent: number;
  failed: number;
  retryableFailures: number;
  permanentFailures: number;
}

@Injectable()
export class MockEmailProvider implements EmailProvider {
  private readonly logger = new Logger(MockEmailProvider.name);
  private failureMode: MockFailureMode = 'none';
  private failureRate: number = 0; // 0 to 100 percentage
  public sentEmails: EmailMessage[] = [];
  public attemptRecords: MockEmailRecord[] = [];

  setFailureMode(mode: MockFailureMode) {
    this.failureMode = mode;
  }

  setFailureRate(rate: number) {
    this.failureRate = Math.max(0, Math.min(100, rate));
  }

  getFailureRate(): number {
    return this.failureRate;
  }

  clear() {
    this.sentEmails = [];
    this.attemptRecords = [];
    this.failureRate = 0;
    this.failureMode = 'none';
  }

  getStats(): MockEmailStats {
    const totalAttempts = this.attemptRecords.length;
    const successfullySent = this.attemptRecords.filter((r) => r.result === 'SENT').length;
    const failed = this.attemptRecords.filter((r) => r.result === 'FAILED').length;
    const retryableFailures = this.attemptRecords.filter((r) => r.result === 'FAILED' && r.retryable).length;
    const permanentFailures = this.attemptRecords.filter((r) => r.result === 'FAILED' && !r.retryable).length;

    return {
      totalAttempts,
      successfullySent,
      failed,
      retryableFailures,
      permanentFailures,
    };
  }

  getRecords(): MockEmailRecord[] {
    return [...this.attemptRecords];
  }

  async send(message: EmailMessage): Promise<EmailSendResult> {
    const timestamp = new Date();

    // Check random failure rate first
    if (this.failureRate > 0) {
      const roll = Math.random() * 100;
      if (roll < this.failureRate) {
        // 80% of simulated failures are retryable 503, 20% permanent 400
        const isRetryable = Math.random() < 0.8;
        const errorMsg = isRetryable
          ? 'Simulated provider error (503 Service Unavailable)'
          : 'Simulated permanent error (400 Bad Request)';

        this.attemptRecords.push({
          notificationId: message.notificationId,
          bookingId: message.bookingId,
          recipient: message.to,
          subject: message.subject,
          timestamp,
          result: 'FAILED',
          error: errorMsg,
          retryable: isRetryable,
        });

        this.logger.warn({
          event: 'email.simulated_failure',
          notificationId: message.notificationId,
          bookingId: message.bookingId,
          error: errorMsg,
          retryable: isRetryable,
        });

        return {
          success: false,
          error: errorMsg,
          retryable: isRetryable,
        };
      }
    }

    if (this.failureMode === 'timeout') {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const errorMsg = 'Network timeout (504 Gateway Timeout)';
      this.attemptRecords.push({
        notificationId: message.notificationId,
        bookingId: message.bookingId,
        recipient: message.to,
        subject: message.subject,
        timestamp,
        result: 'FAILED',
        error: errorMsg,
        retryable: true,
      });

      return {
        success: false,
        error: errorMsg,
        retryable: true,
      };
    }

    if (this.failureMode === 'retryable') {
      const errorMsg = 'Email service temporarily unavailable (503 Service Unavailable)';
      this.attemptRecords.push({
        notificationId: message.notificationId,
        bookingId: message.bookingId,
        recipient: message.to,
        subject: message.subject,
        timestamp,
        result: 'FAILED',
        error: errorMsg,
        retryable: true,
      });

      return {
        success: false,
        error: errorMsg,
        retryable: true,
      };
    }

    if (this.failureMode === 'permanent') {
      const errorMsg = 'Invalid recipient address (400 Bad Request)';
      this.attemptRecords.push({
        notificationId: message.notificationId,
        bookingId: message.bookingId,
        recipient: message.to,
        subject: message.subject,
        timestamp,
        result: 'FAILED',
        error: errorMsg,
        retryable: false,
      });

      return {
        success: false,
        error: errorMsg,
        retryable: false,
      };
    }

    const providerMessageId = `mock-email-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    this.sentEmails.push(message);
    this.attemptRecords.push({
      notificationId: message.notificationId,
      bookingId: message.bookingId,
      recipient: message.to,
      subject: message.subject,
      timestamp,
      result: 'SENT',
    });

    this.logger.log({
      event: 'email.sent_mock',
      to: message.to,
      subject: message.subject,
      notificationId: message.notificationId,
      bookingId: message.bookingId,
      providerMessageId,
    });

    return {
      success: true,
      providerMessageId,
    };
  }
}

