import { NestFactory } from '@nestjs/core';
import { TrafficSimulatorModule } from './traffic-simulator.module';
import { TrafficSimulatorService } from './traffic-simulator.service';
import { TrafficSimulatorOptions, TrafficMode } from './traffic-simulator.types';
import { getTrafficConfig } from './traffic-simulator.config';

function parseArgs(): { options: Partial<TrafficSimulatorOptions>; verifyMode?: string } {
  const args = process.argv.slice(2);
  const options: Partial<TrafficSimulatorOptions> = {};
  let verifyMode: string | undefined;

  for (const arg of args) {
    if (arg.startsWith('--mode=')) {
      options.mode = arg.split('=')[1] as TrafficMode;
    } else if (arg.startsWith('--rate=')) {
      options.rate = Number(arg.split('=')[1]);
    } else if (arg.startsWith('--duration=')) {
      options.durationSeconds = Number(arg.split('=')[1]);
    } else if (arg.startsWith('--cancellationRate=')) {
      options.cancellationRate = Number(arg.split('=')[1]);
    } else if (arg.startsWith('--delayMs=')) {
      options.delayMs = Number(arg.split('=')[1]);
    } else if (arg.startsWith('--failureRate=')) {
      options.failureRate = Number(arg.split('=')[1]);
    } else if (arg.startsWith('--verify=')) {
      verifyMode = arg.split('=')[1];
    }
  }

  return { options, verifyMode };
}

async function main() {
  if (process.env.NODE_ENV === 'production') {
    console.error('Traffic simulator is disabled in production.');
    process.exit(1);
  }

  const { options, verifyMode } = parseArgs();
  const config = getTrafficConfig(options);

  // First try calling the running Nest server via HTTP API
  try {
    const healthCheck = await fetch(`${config.apiUrl}/health`).catch(() => null);

    if (healthCheck && healthCheck.ok) {
      if (verifyMode) {
        console.log(`Running verification mode: ${verifyMode} via HTTP API (${config.apiUrl})...\n`);
        let handled = false;
        if (verifyMode === 'stale' || verifyMode === 'all') {
          const res = await fetch(`${config.apiUrl}/api/dev/traffic/verify/stale`);
          if (res.ok) {
            const data = await res.json();
            console.log(`[Stale Version Test]: ${data.success ? 'PASSED ✅' : 'FAILED ❌'}`);
            console.log(`Details: ${data.details}\n`);
            handled = true;
          }
        }
        if (verifyMode === 'dedupe' || verifyMode === 'all') {
          const res = await fetch(`${config.apiUrl}/api/dev/traffic/verify/dedupe`);
          if (res.ok) {
            const data = await res.json();
            console.log(`[Duplicate Event Test]: ${data.success ? 'PASSED ✅' : 'FAILED ❌'}`);
            console.log(`Details: ${data.details}\n`);
            handled = true;
          }
        }
        if (handled) {
          process.exit(0);
        }
      }

      console.log(`Connecting to running NestJS app at ${config.apiUrl}...`);
      const res = await fetch(`${config.apiUrl}/api/dev/traffic/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config),
      });

      if (!res.ok) {
        const errText = await res.text();
        console.error(`Traffic test request failed (${res.status}): ${errText}`);
        process.exit(1);
      }

      const metrics = await res.json();
      
      // Print Summary Report
      console.log(`
========================================
TRAFFIC TEST SUMMARY
========================================

Run ID: ${metrics.trafficRunId}
Mode:   ${metrics.mode}

Bookings:
  Created:        ${metrics.bookingsCreated.toString().padStart(6)}
  Cancelled:      ${metrics.bookingsCancelled.toString().padStart(6)}

Kafka:
  Published:      ${metrics.outboxPublished.toString().padStart(6)}
  Consumed:       ${metrics.kafkaConsumed.toString().padStart(6)}

Notifications:
  Created:        ${metrics.notificationsCreated.toString().padStart(6)}
  Sent:           ${metrics.notificationsSent.toString().padStart(6)}
  Expired:        ${metrics.notificationsExpired.toString().padStart(6)}
  Failed:         ${metrics.notificationsFailed.toString().padStart(6)}
  Retrying/DLQ:   ${(metrics.notificationsRetrying + metrics.notificationsDeadLetter).toString().padStart(6)}

Email:
  Attempts:       ${metrics.emailAttempts.toString().padStart(6)}
  Successful:     ${metrics.emailSuccessful.toString().padStart(6)}
  Failed:         ${metrics.emailFailed.toString().padStart(6)}

Queue (BullMQ):
  Waiting:        ${metrics.queueBreakdown.waiting.toString().padStart(6)}
  Active:         ${metrics.queueBreakdown.active.toString().padStart(6)}
  Completed:      ${metrics.queueBreakdown.completed.toString().padStart(6)}
  Failed:         ${metrics.queueBreakdown.failed.toString().padStart(6)}
  Delayed:        ${metrics.queueBreakdown.delayed.toString().padStart(6)}
  Remaining jobs: ${metrics.queueJobsRemaining.toString().padStart(6)}

========================================
`);
      process.exit(0);
    }
  } catch (e) {
    // Fall through to standalone application context if server not reachable
  }

  // Fallback: Bootstrap standalone Nest Application Context
  console.log(`NestJS server not detected at ${config.apiUrl}. Bootstrapping standalone simulator context...`);
  const app = await NestFactory.createApplicationContext(TrafficSimulatorModule, { logger: ['error', 'warn', 'log'] });
  const simulatorService = app.get(TrafficSimulatorService);

  try {
    if (verifyMode) {
      if (verifyMode === 'stale' || verifyMode === 'all') {
        const res = await simulatorService.verifyStaleNotificationScenario();
        console.log(`[Stale Version Test]: ${res.success ? 'PASSED ✅' : 'FAILED ❌'}`);
        console.log(`Details: ${res.details}\n`);
      }
      if (verifyMode === 'dedupe' || verifyMode === 'all') {
        const res = await simulatorService.verifyDuplicateEventDeduplication();
        console.log(`[Duplicate Event Test]: ${res.success ? 'PASSED ✅' : 'FAILED ❌'}`);
        console.log(`Details: ${res.details}\n`);
      }
    } else {
      const metrics = await simulatorService.runSimulation(config);
      simulatorService.printSummaryReport(metrics);
    }
  } finally {
    await app.close();
  }
}

main().catch((err) => {
  console.error('Fatal error running traffic simulator:', err);
  process.exit(1);
});
