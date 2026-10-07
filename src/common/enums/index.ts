export {
  BookingStatus,
  NotificationStatus,
  NotificationType,
  NotificationChannel,
} from '@prisma/client';

export enum EventType {
  BOOKING_CONFIRMED = 'BOOKING_CONFIRMED',
  BOOKING_CANCELLED = 'BOOKING_CANCELLED',
}

export enum AggregateType {
  BOOKING = 'BOOKING',
}
