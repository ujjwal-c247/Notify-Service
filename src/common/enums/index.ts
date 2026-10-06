export enum BookingStatus {
  PENDING = 'PENDING',
  CONFIRMED = 'CONFIRMED',
  CANCELLED = 'CANCELLED',
}

export enum NotificationStatus {
  CREATED = 'CREATED',
  READY = 'READY',
  PROCESSING = 'PROCESSING',
  SENT = 'SENT',
  FAILED = 'FAILED',
  RETRYING = 'RETRYING',
  EXPIRED = 'EXPIRED',
  SUPPRESSED = 'SUPPRESSED',
  DEAD_LETTER = 'DEAD_LETTER',
}

export enum NotificationType {
  BOOKING_CONFIRMATION = 'BOOKING_CONFIRMATION',
}

export enum NotificationChannel {
  EMAIL = 'EMAIL',
}

export enum EventType {
  BOOKING_CONFIRMED = 'BOOKING_CONFIRMED',
  BOOKING_CANCELLED = 'BOOKING_CANCELLED',
}

export enum AggregateType {
  BOOKING = 'BOOKING',
}
