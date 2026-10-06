import { TrafficSimulatorOptions, TrafficMode } from './traffic-simulator.types';

export const getTrafficConfig = (overrides: Partial<TrafficSimulatorOptions> = {}): TrafficSimulatorOptions & { apiUrl: string } => {
  const mode = (overrides.mode || process.env.TRAFFIC_MODE || 'mixed') as TrafficMode;
  const rate = overrides.rate !== undefined ? overrides.rate : Number(process.env.TRAFFIC_RATE || 10);
  const durationSeconds = overrides.durationSeconds !== undefined ? overrides.durationSeconds : Number(process.env.TRAFFIC_DURATION_SECONDS || 30);
  const cancellationRate = overrides.cancellationRate !== undefined ? overrides.cancellationRate : Number(process.env.TRAFFIC_CANCELLATION_RATE || 20);
  const delayMs = overrides.delayMs !== undefined ? overrides.delayMs : Number(process.env.TRAFFIC_DELAY_MS || 0);
  const failureRate = overrides.failureRate !== undefined ? overrides.failureRate : Number(process.env.MOCK_EMAIL_FAILURE_RATE || 0);
  const apiUrl = process.env.API_URL || 'http://localhost:3000';

  return {
    mode,
    rate,
    durationSeconds,
    cancellationRate,
    delayMs,
    failureRate,
    apiUrl,
  };
};
