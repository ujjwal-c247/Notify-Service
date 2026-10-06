import { Controller, Post, Get, Body, HttpException, HttpStatus, Inject } from '@nestjs/common';
import { TrafficSimulatorService } from './traffic-simulator.service';
import { TrafficSimulatorOptions, TrafficRunMetrics } from './traffic-simulator.types';
import { EMAIL_PROVIDER } from '../../infrastructure/email/email.provider';
import { MockEmailProvider } from '../../infrastructure/email/mock-email.provider';

@Controller('api/dev/traffic')
export class TrafficSimulatorController {
  constructor(
    private readonly simulatorService: TrafficSimulatorService,
    @Inject(EMAIL_PROVIDER) private readonly emailProvider: MockEmailProvider,
  ) {}

  @Post('start')
  async startTraffic(@Body() body: Partial<TrafficSimulatorOptions>): Promise<TrafficRunMetrics> {
    if (process.env.NODE_ENV === 'production') {
      throw new HttpException('Traffic simulator is disabled in production', HttpStatus.FORBIDDEN);
    }
    return this.simulatorService.runSimulation(body);
  }

  @Get('email-stats')
  getEmailStats() {
    return this.emailProvider.getStats();
  }

  @Post('reset-stats')
  resetEmailStats() {
    this.emailProvider.clear();
    return { message: 'Mock email stats cleared' };
  }

  @Post('config')
  setFailureRate(@Body() body: { failureRate?: number; failureMode?: any }) {
    if (body.failureRate !== undefined) {
      this.emailProvider.setFailureRate(body.failureRate);
    }
    if (body.failureMode !== undefined) {
      this.emailProvider.setFailureMode(body.failureMode);
    }
    return { failureRate: this.emailProvider.getFailureRate() };
  }

  @Get('verify/stale')
  async verifyStale() {
    return this.simulatorService.verifyStaleNotificationScenario();
  }

  @Get('verify/dedupe')
  async verifyDedupe() {
    return this.simulatorService.verifyDuplicateEventDeduplication();
  }
}
