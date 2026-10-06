export const KAFKA_TOPICS = {
  BOOKING_EVENTS: 'booking.events',
} as const;

export const KAFKA_GROUPS = {
  NOTIFICATION_SERVICE: 'notification-service',
} as const;

export const QUEUES = {
  EMAIL_NOTIFICATIONS: 'email-notifications',
} as const;

export const JOBS = {
  SEND_BOOKING_CONFIRMATION: 'send-booking-confirmation',
} as const;
