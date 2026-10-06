export type TrafficMode = 'normal' | 'cancel' | 'mixed' | 'burst';

export interface TrafficSimulatorOptions {
  mode: TrafficMode;
  rate: number; // bookings per second
  durationSeconds: number; // duration in seconds
  cancellationRate: number; // percentage (0-100)
  delayMs?: number; // delay between create and cancel
  failureRate?: number; // email failure rate percentage (0-100)
}

export interface TrafficRunMetrics {
  trafficRunId: string;
  mode: TrafficMode;
  rate: number;
  durationSeconds: number;
  cancellationRate: number;
  targetBookings: number;
  
  bookingsCreated: number;
  bookingsCancelled: number;
  
  outboxPublished: number;
  kafkaConsumed: number;
  
  notificationsCreated: number;
  notificationsSent: number;
  notificationsExpired: number;
  notificationsFailed: number;
  notificationsRetrying: number;
  notificationsDeadLetter: number;
  
  emailAttempts: number;
  emailSuccessful: number;
  emailFailed: number;
  
  queueJobsRemaining: number;
  queueBreakdown: {
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
  };
}
