export interface EmailMessage {
  to: string;
  subject: string;
  body: string;
  notificationId: string;
  bookingId: string;
}

export interface EmailSendResult {
  success: boolean;
  providerMessageId?: string;
  error?: string;
  retryable?: boolean;
}

export const EMAIL_PROVIDER = 'EMAIL_PROVIDER';

export interface EmailProvider {
  send(message: EmailMessage): Promise<EmailSendResult>;
}
